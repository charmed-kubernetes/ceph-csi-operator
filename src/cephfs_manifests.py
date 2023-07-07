# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
"""Implementation of rbd specific details of the kubernetes manifests."""

import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

from lightkube.codecs import AnyResource
from lightkube.models.core_v1 import Toleration
from lightkube.resources.core_v1 import Secret
from lightkube.resources.storage_v1 import StorageClass
from ops.manifests import Addition, ConfigRegistry, ManifestLabel, Patch
from ops.manifests.manipulations import Subtraction

from safe_manifests import SafeManifest

if TYPE_CHECKING:
    from charm import CephCsiCharm

log = logging.getLogger(__name__)
DEFAULT_NAMESPACE = "default"


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
                secret_config[secret_key] = self.manifests.config.get(k)

        if any(s is None for s in secret_config.values()):
            log.error("secret data item is None")
            return None

        log.info("Craft secret data for cephfs storage.")
        ns = self.manifests.config.get("namespace")
        return Secret.from_dict(
            dict(metadata=dict(name=self.SECRET_NAME, namespace=ns), stringData=secret_config)
        )


class CreateStorageClass(Addition):
    """Create ceph storage classes."""

    REQUIRED_CONFIG = {"fsid", "fsname"}

    def __call__(self) -> Optional[AnyResource]:
        """Craft the storage class object."""
        if not self.manifests.config["enabled"]:
            log.info("Ignore Cephfs Storage Class")
            return None

        clusterID = self.manifests.config.get("fsid")
        if not clusterID:
            log.error("Storage clusterID unknown.")
            return None

        fsname = self.manifests.config.get("fsname")

        ns = self.manifests.config.get("namespace")
        metadata: Dict[str, Any] = dict(name="cephfs")
        if self.manifests.config.get("default-storage") == metadata["name"]:
            metadata["annotations"] = {"storageclass.kubernetes.io/is-default-class": "true"}

        log.info(f"Craft storage class {metadata['name']}")
        parameters = {
            "clusterID": clusterID,
            "fsName": fsname,
            "csi.storage.k8s.io/controller-expand-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/controller-expand-secret-namespace": ns,
            "csi.storage.k8s.io/provisioner-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/provisioner-secret-namespace": ns,
            "csi.storage.k8s.io/node-stage-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/node-stage-secret-namespace": ns,
            "pool": "ceph-fs-pool",
        }
        mounter = self.manifests.config.get("cephfs-mounter")
        if mounter in ["kernel", "fuse"]:
            parameters["mounter"] = mounter

        return StorageClass.from_dict(
            dict(
                metadata=metadata,
                provisioner="cephfs.csi.ceph.com",
                allowVolumeExpansion=True,
                mountOptions=["debug"],
                reclaimPolicy="Delete",
                parameters=parameters,
            )
        )


class ProvisionerAdjustments(Patch):
    """Update Cephfs provisioner."""

    def __call__(self, obj: AnyResource) -> None:
        """Use the provisioner-replicas and enable-host-networking to update obj."""
        if (
            obj.kind == "Deployment"
            and obj.metadata
            and obj.metadata.name == "csi-cephfsplugin-provisioner"
        ):
            obj.spec.replicas = replica = self.manifests.config.get("provisioner-replicas")
            log.info(f"Updating deployment replicas to {replica}")

            obj.spec.template.spec.hostNetwork = host_network = self.manifests.config.get(
                "enable-host-networking"
            )
            log.info(f"Updating deployment hostNetwork to {host_network}")
        if obj.kind == "DaemonSet" and obj.metadata and obj.metadata.name == "csi-cephfsplugin":
            obj.spec.template.spec.tolerations = [Toleration(operator="Exists")]
            log.info("Updating daemonset tolerations to operator=Exists")


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
                CreateStorageClass(self),  # creates cephfs storage class
                RemoveCephFS(self),
            ],
        )
        self.charm = charm

    @property
    def config(self) -> Dict:
        """Returns current config available from charm config and joined relations."""
        config: Dict = {}
        config["image-registry"] = "rocks.canonical.com:443/cdk"
        config["namespace"] = DEFAULT_NAMESPACE

        config.update(**self.charm.ceph_context)
        config.update(**self.charm.config)

        for key, value in dict(**config).items():
            if value == "" or value is None:
                del config[key]

        config["release"] = config.pop("release", None)
        config["enabled"] = self.purgeable or config.get("cephfs-enable", None)
        return config

    def evaluate(self) -> Optional[str]:
        """Determine if manifest_config can be applied to manifests."""
        if not self.config.get("cephfs-enable"):
            # not enabled, not a problem
            log.info("Skipping ceph-fs evaluation since its disabled")
            return None

        props = StorageSecret.REQUIRED_CONFIG.keys() | CreateStorageClass.REQUIRED_CONFIG
        for prop in props:
            value = self.config.get(prop)
            if not value:
                return f"Cephfs manifests waiting for definition of {prop}"
        return None
