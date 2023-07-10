# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
"""Implementation of rbd specific details of the kubernetes manifests."""

import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

from lightkube.codecs import AnyResource
from lightkube.resources.core_v1 import Secret
from lightkube.resources.storage_v1 import StorageClass
from ops.manifests import Addition, ConfigRegistry, ManifestLabel, Manifests, Patch

from manifests_base import SafeManifest

if TYPE_CHECKING:
    from charm import CephCsiCharm

log = logging.getLogger(__name__)
DEFAULT_NAMESPACE = "default"


class StorageSecret(Addition):
    """Create secret for the deployment."""

    SECRET_NAME = "csi-rbd-secret"

    REQUIRED_CONFIG = {
        "user": "userID",
        "kubernetes_key": "userKey",
    }

    def __call__(self) -> Optional[AnyResource]:
        """Craft the secrets object for the deployment."""
        secret_config = {}
        for k, secret_key in self.REQUIRED_CONFIG.items():
            if value := self.manifests.config.get(k):
                secret_config[secret_key] = value
            else:
                log.error(f"RBD is missing required secret item: '{k}'")
                return None

        log.info("Modelling secret data for rbd storage.")
        ns = self.manifests.config.get("namespace")
        return Secret.from_dict(
            dict(metadata=dict(name=self.SECRET_NAME, namespace=ns), stringData=secret_config)
        )


class CephStorageClass(Addition):
    """Create ceph storage classes."""

    REQUIRED_CONFIG = {"fsid"}
    PROVISIONER = "rbd.csi.ceph.com"

    def __init__(self, manifests: Manifests, fs_type: str):
        super().__init__(manifests)
        self.fs_type = fs_type

    @property
    def fs_name(self) -> str:
        return f"ceph-{self.fs_type}"

    def __call__(self) -> Optional[AnyResource]:
        """Craft the storage class object."""
        clusterID = self.manifests.config.get("fsid")
        if not clusterID:
            log.error(f"Ceph {self.fs_type.capitalize()} is missing required storage item: 'fsid'")
            return None

        ns = self.manifests.config.get("namespace")
        metadata: Dict[str, Any] = dict(name=self.fs_name)
        if self.manifests.config.get("default-storage") == self.fs_name:
            metadata["annotations"] = {"storageclass.kubernetes.io/is-default-class": "true"}

        log.info(f"Modelling storage class {metadata['name']}")
        parameters = {
            "clusterID": clusterID,
            "csi.storage.k8s.io/controller-expand-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/controller-expand-secret-namespace": ns,
            "csi.storage.k8s.io/fstype": self.fs_type,
            "csi.storage.k8s.io/node-stage-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/node-stage-secret-namespace": ns,
            "csi.storage.k8s.io/provisioner-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/provisioner-secret-namespace": ns,
            "imageFeatures": "layering",
            "pool": f"{self.fs_type}-pool",
            "thickProvision": "false",
        }
        return StorageClass.from_dict(
            dict(
                metadata=metadata,
                provisioner=self.PROVISIONER,
                allowVolumeExpansion=True,
                mountOptions=["discard"],
                reclaimPolicy="Delete",
                parameters=parameters,
            )
        )


class ProvisionerAdjustments(Patch):
    """Update RBD provisioner."""

    def __call__(self, obj: AnyResource) -> None:
        """Use the image-registry config and updates container images in obj."""
        if (
            obj.kind == "Deployment"
            and obj.metadata
            and obj.metadata.name == "csi-rbdplugin-provisioner"
        ):
            obj.spec.replicas = replica = self.manifests.config.get("provisioner-replicas")
            log.info(f"Updating deployment replicas to {replica}")

            obj.spec.template.spec.hostNetwork = host_network = self.manifests.config.get(
                "enable-host-networking"
            )
            log.info(f"Updating deployment hostNetwork to {host_network}")


class RBDManifests(SafeManifest):
    """Deployment Specific details for the rbd.csi.ceph.com."""

    def __init__(self, charm: "CephCsiCharm"):
        super().__init__(
            "rbd",
            charm.model,
            "upstream/rbd",
            [
                StorageSecret(self),
                ManifestLabel(self),
                ConfigRegistry(self),
                ProvisionerAdjustments(self),
                CephStorageClass(self, "xfs"),  # creates ceph-xfs
                CephStorageClass(self, "ext4"),  # creates ceph-ext4
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
        return config

    def evaluate(self) -> Optional[str]:
        """Determine if manifest_config can be applied to manifests."""
        props = StorageSecret.REQUIRED_CONFIG.keys() | CephStorageClass.REQUIRED_CONFIG
        for prop in sorted(props):
            value = self.config.get(prop)
            if not value:
                return f"RBD manifests require the definition of '{prop}'"
        return None
