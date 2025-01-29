# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
"""Implementation of rbd specific details of the kubernetes manifests."""

import logging
from dataclasses import dataclass
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
    update_storage_params,
)

if TYPE_CHECKING:
    from charm import CephCsiCharm

log = logging.getLogger(__name__)


@dataclass
class CephFilesystem:
    """Ceph Filesystem details."""

    name: str
    metadata_pool: str
    metadata_pool_id: int
    data_pool_ids: List[int]
    data_pools: List[str]


@dataclass
class CephStorageClassParameters:
    """Ceph Storage class parameters."""

    cluster_id: str
    storage_class_name: str
    filesystem_name: str
    data_pool: str


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


class CephStorageClass(Addition):
    """Create ceph storage classes."""

    STORAGE_NAME = "cephfs"
    STORAGE_NAME_FORMATTER = "cephfs-storage-class-name-formatter"
    FILESYSTEM_LISTING = "fs_list"
    REQUIRED_CONFIG = {STORAGE_NAME_FORMATTER, "fsid", FILESYSTEM_LISTING}
    PROVISIONER = "cephfs.csi.ceph.com"

    def create(self, param: CephStorageClassParameters) -> AnyResource:
        """Create a storage class object."""

        ns = self.manifests.config["namespace"]
        metadata: Dict[str, Any] = dict(name=param.storage_class_name)
        if self.manifests.config.get("default-storage") == param.storage_class_name:
            metadata["annotations"] = {"storageclass.kubernetes.io/is-default-class": "true"}

        log.info(f"Modelling storage class sc='{param.storage_class_name}'")
        parameters = {
            "clusterID": param.cluster_id,
            "fsName": param.filesystem_name,
            "csi.storage.k8s.io/controller-expand-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/controller-expand-secret-namespace": ns,
            "csi.storage.k8s.io/provisioner-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/provisioner-secret-namespace": ns,
            "csi.storage.k8s.io/node-stage-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/node-stage-secret-namespace": ns,
            "pool": param.data_pool,
        }
        mounter = self.manifests.config.get("cephfs-mounter")
        if mounter in ["kernel", "fuse"]:
            parameters["mounter"] = mounter

        update_storage_params(self.STORAGE_NAME, self.manifests.config, parameters)

        return StorageClass.from_dict(
            dict(
                metadata=metadata,
                provisioner=self.PROVISIONER,
                allowVolumeExpansion=True,
                reclaimPolicy="Delete",
                parameters=parameters,
            )
        )

    def parameter_list(self) -> List[CephStorageClassParameters]:
        """Accumulate names and settings of the storage classes."""
        enabled = self.manifests.config.get("enabled")
        fsid = self.manifests.config.get("fsid")
        fs_data: List[CephFilesystem] = self.manifests.config.get(self.FILESYSTEM_LISTING) or []
        formatter = str(self.manifests.config.get(self.STORAGE_NAME_FORMATTER) or "")

        if not enabled:
            log.info("Ignore CephFS Storage Class")
            return []

        if not fsid:
            log.error("CephFS is missing a filesystem: 'fsid'")
            raise ValueError("missing fsid")

        if not fs_data:
            log.error("CephFS is missing a filesystem listing: '%s'", self.FILESYSTEM_LISTING)
            raise ValueError("missing filesystem listing")

        if not formatter:
            log.error("CephFS is missing '%s'", self.STORAGE_NAME_FORMATTER)
            raise ValueError(f"empty {self.STORAGE_NAME_FORMATTER}")

        sc_names: List[CephStorageClassParameters] = []
        for fs in fs_data:
            for idx, data_pool in enumerate(fs.data_pools):
                context = {
                    "app": self.manifests.model.app.name,
                    "namespace": self.manifests.config["namespace"],
                    "name": fs.name,
                    "pool": data_pool,
                    "pool-id": fs.data_pool_ids[idx],
                }
                sc_name = formatter.format(**context)
                sc_names += [CephStorageClassParameters(fsid, sc_name, fs.name, data_pool)]

        if len(set(n.storage_class_name for n in sc_names)) != len(sc_names):
            log.error(
                "The formatter cannot generate unique storage class names for all the available pools. "
                "Consider improving the config '%s' to expand to meet the number of pools."
                "\n\tfile systems    = %s"
                "\n\tstorage_classes = %s",
                self.STORAGE_NAME_FORMATTER,
                fs_data,
                sc_names,
            )
            raise ValueError(f"{self.STORAGE_NAME_FORMATTER} does not generate unique names")
        return sc_names

    def __call__(self) -> List[AnyResource]:
        """Craft the storage class object."""
        return [self.create(class_param) for class_param in self.parameter_list()]


class ProvisionerAdjustments(Patch):
    """Update Cephfs provisioner."""
    PROVISIONER_NAME = "csi-cephfsplugin-provisioner"
    PLUGIN_NAME = "csi-cephfsplugin"

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
            and obj.metadata.name == self.PROVISIONER_NAME
        ):
            obj.spec.replicas = replica = self.manifests.config.get("provisioner-replicas")
            log.info(f"Updating deployment replicas to {replica}")

            obj.spec.template.spec.tolerations = tolerations
            log.info("Updating deployment tolerations")

            obj.spec.template.spec.hostNetwork = host_network = self.manifests.config.get(
                "enable-host-networking"
            )
            log.info(f"Updating deployment hostNetwork to {host_network}")
        if obj.kind == "DaemonSet" and obj.metadata and obj.metadata.name == self.PLUGIN_NAME:
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
            log.info(f"Updating daemonset kubeletDir to {kubelet_dir}")


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
            log.info("Skipping CephFS evaluation since it's disabled")
            return None

        props = StorageSecret.REQUIRED_CONFIG.keys()
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

        sc_manipulator = next(m for m in self.manipulations if isinstance(m, CephStorageClass))
        try:
            sc_manipulator.parameter_list()
        except ValueError as err:
            return f"CephFS manifests failed to create storage classes: {err}"

        return None
