# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
"""Implementation of rbd specific details of the kubernetes manifests."""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from lightkube.codecs import AnyResource
from lightkube.resources.core_v1 import Secret
from lightkube.resources.storage_v1 import StorageClass
from ops.manifests import Addition, ConfigRegistry, ManifestLabel, Patch
from ops.manifests.manipulations import Subtraction

from manifests_base import (
    AdjustNamespace,
    CephToleration,
    ConfigureLivenessPrometheus,
    SafeManifest,
    StorageClassAddition,
)

if TYPE_CHECKING:
    from charm import CephCsiCharm

log = logging.getLogger(__name__)


class StorageSecret(Addition):
    """Create secret for the deployment."""

    SECRET_NAME = "csi-cephfs-secret"

    REQUIRED_CONFIG = {
        "user": ["userID", "adminID"],
        "kubernetes_key": ["userKey", "adminKey"],
    }

    def __call__(self) -> Optional[AnyResource]:
        """Craft the secrets object for the deployment."""
        if not self.manifests.config["enabled"]:
            log.info("Ignore Cephfs Storage Secrets")
            return None

        secret_config = {}
        for k, keys in self.REQUIRED_CONFIG.items():
            for secret_key in keys:
                if value := self.manifests.config.get(k):
                    secret_config[secret_key] = value
                else:
                    log.error(f"Cephfs is missing required secret item: '{k}'")
                    return None

        log.info("Modelling secret data for cephfs storage.")
        return Secret.from_dict(
            dict(metadata=dict(name=self.SECRET_NAME), stringData=secret_config)
        )


class CephStorageClass(StorageClassAddition):
    """Create ceph storage classes."""

    STORAGE_NAME = "cephfs"
    REQUIRED_CONFIG = {"fsid", "fsname"}
    PROVISIONER = "cephfs.csi.ceph.com"
    POOL = "ceph-fs_data"

    @property
    def name(self) -> str:
        return self.STORAGE_NAME

    def __call__(self) -> Optional[AnyResource]:
        """Craft the storage class object."""
        if not self.manifests.config["enabled"]:
            log.info("Ignore CephFS Storage Class")
            return None

        clusterID = self.manifests.config.get("fsid")
        if not clusterID:
            log.error("CephFS is missing required storage item: 'clusterID'")
            return None

        fsname = self.manifests.config.get("fsname")
        if not fsname:
            log.error("CephFS is missing required storage item: 'fsname'")
            return None

        ns = self.manifests.config["namespace"]
        metadata: Dict[str, Any] = dict(name=self.name)
        if self.manifests.config.get("default-storage") == metadata["name"]:
            metadata["annotations"] = {"storageclass.kubernetes.io/is-default-class": "true"}

        log.info(f"Modelling storage class {metadata['name']}")
        parameters = {
            "clusterID": clusterID,
            "fsName": fsname,
            "csi.storage.k8s.io/controller-expand-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/controller-expand-secret-namespace": ns,
            "csi.storage.k8s.io/provisioner-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/provisioner-secret-namespace": ns,
            "csi.storage.k8s.io/node-stage-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/node-stage-secret-namespace": ns,
            "pool": self.POOL,
        }
        mounter = self.manifests.config.get("cephfs-mounter")
        if mounter in ["kernel", "fuse"]:
            parameters["mounter"] = mounter

        self.update_parameters(parameters)

        return StorageClass.from_dict(
            dict(
                metadata=metadata,
                provisioner=self.PROVISIONER,
                allowVolumeExpansion=True,
                reclaimPolicy="Delete",
                parameters=parameters,
            )
        )


class ProvisionerAdjustments(Patch):
    """Update Cephfs provisioner."""

    def tolerations(self) -> Tuple[List[CephToleration], bool]:
        cfg = self.manifests.config.get("cephfs-tolerations") or ""
        if cfg == "$csi-cephfsplugin-legacy$":
            return [], True
        return CephToleration.from_space_separated(cfg), False

    def __call__(self, obj: AnyResource) -> None:
        """Use the provisioner-replicas and enable-host-networking to update obj."""
        tolerations, legacy = self.tolerations()
        if (
            obj.kind == "Deployment"
            and obj.metadata
            and obj.metadata.name == "csi-cephfsplugin-provisioner"
        ):
            obj.spec.replicas = replica = self.manifests.config.get("provisioner-replicas")
            log.info(f"Updating deployment replicas to {replica}")

            obj.spec.template.spec.tolerations = tolerations
            log.info("Updating deployment tolerations")

            obj.spec.template.spec.hostNetwork = host_network = self.manifests.config.get(
                "enable-host-networking"
            )
            log.info(f"Updating deployment hostNetwork to {host_network}")
        if obj.kind == "DaemonSet" and obj.metadata and obj.metadata.name == "csi-cephfsplugin":
            obj.spec.template.spec.tolerations = (
                tolerations if not legacy else [CephToleration(operator="Exists")]
            )
            log.info("Updating daemonset tolerations")

            kubelet_dir = self.manifests.config.get("kubelet_dir", "/var/lib/kubelet")

            for c in obj.spec.template.spec.containers:
                c.args = [arg.replace("/var/lib/kubelet", kubelet_dir) for arg in c.args]
                for m in c.volumeMounts:
                    m.mountPath = m.mountPath.replace("/var/lib/kubelet", kubelet_dir)
            for v in obj.spec.template.spec.volumes:
                if v.hostPath:
                    v.hostPath.path = v.hostPath.path.replace("/var/lib/kubelet", kubelet_dir)
            log.info(f"Updating CephFS daemonset kubeletDir to {kubelet_dir}")


class RbacAdjustments(Patch):
    """Update RBD RBAC Attributes."""

    def __call__(self, obj: AnyResource) -> None:
        ns = self.manifests.config["namespace"]
        if obj.kind in ["ClusterRoleBinding", "RoleBinding"]:
            for each in obj.subjects:
                if each.kind == "ServiceAccount":
                    each.namespace = ns


class RemoveCephFS(Subtraction):
    """Remove all Cephfs resources when disabled."""

    def __call__(self, _obj: AnyResource) -> bool:
        """Remove this obj if cephfs is not enabled."""
        return not self.manifests.config["enabled"]


class CephFSManifests(SafeManifest):
    """Deployment Specific details for the aws-ebs-csi-driver."""

    def __init__(self, charm: "CephCsiCharm"):
        super().__init__(
            "cephfs",
            charm.model,
            "upstream/cephfs",
            [
                StorageSecret(self),
                ManifestLabel(self),
                ConfigRegistry(self),
                ProvisionerAdjustments(self),
                CephStorageClass(self),
                RbacAdjustments(self),
                RemoveCephFS(self),
                AdjustNamespace(self),
                ConfigureLivenessPrometheus(
                    self, "Deployment", "csi-cephfsplugin-provisioner", "cephfsplugin-provisioner"
                ),
                ConfigureLivenessPrometheus(
                    self, "Service", "csi-cephfsplugin-provisioner", "cephfsplugin-provisioner"
                ),
                ConfigureLivenessPrometheus(self, "DaemonSet", "csi-cephfsplugin", "cephfsplugin"),
                ConfigureLivenessPrometheus(
                    self, "Service", "csi-metrics-cephfsplugin", "cephfsplugin"
                ),
            ],
        )
        self.charm = charm

    @property
    def config(self) -> Dict:
        """Returns current config available from charm config and joined relations."""
        config: Dict = {}

        config.update(**self.charm.ceph_context)
        config.update(**self.charm.kubernetes_context)
        config.update(**self.charm.config)

        for key, value in dict(**config).items():
            if value == "" or value is None:
                del config[key]

        config["namespace"] = self.charm.stored.namespace
        config["release"] = config.pop("release", None)
        config["enabled"] = self.purgeable or config.get("cephfs-enable", None)
        return config

    def evaluate(self) -> Optional[str]:
        """Determine if manifest_config can be applied to manifests."""
        if not self.config.get("enabled"):
            # not enabled, not a problem
            log.info("Skipping CephFS evaluation since it's disabled")
            return None

        props = StorageSecret.REQUIRED_CONFIG.keys() | CephStorageClass.REQUIRED_CONFIG
        for prop in sorted(props):
            value = self.config.get(prop)
            if not value:
                return f"CephFS manifests require the definition of '{prop}'"

        pa_manipulator = next(
            m for m in self.manipulations if isinstance(m, ProvisionerAdjustments)
        )
        try:
            pa_manipulator.tolerations()
        except ValueError as err:
            return f"Cannot adjust CephFS Pods: {err}"

        return None
