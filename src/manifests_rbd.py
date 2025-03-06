# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
"""Implementation of rbd specific details of the kubernetes manifests."""

import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, cast

from lightkube.codecs import AnyResource
from lightkube.resources.core_v1 import Secret
from lightkube.resources.storage_v1 import StorageClass
from ops.manifests import Addition, ConfigRegistry, ManifestLabel

from manifests_base import (
    AdjustNamespace,
    CephToleration,
    ConfigureLivenessPrometheus,
    CSIDriverAdjustments,
    ProvisionerAdjustments,
    RbacAdjustments,
    SafeManifest,
    StorageClassFactory,
)

if TYPE_CHECKING:
    from charm import CephCsiCharm

log = logging.getLogger(__name__)


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
        return Secret.from_dict(
            dict(metadata=dict(name=self.SECRET_NAME), stringData=secret_config)
        )


class CephStorageClass(StorageClassFactory):
    """Create ceph storage classes."""

    REQUIRED_CONFIG = {"fsid"}

    def __call__(self) -> Optional[AnyResource]:
        """Craft the storage class object."""
        driver_name = cast(SafeManifest, self.manifests).csidriver.formatted

        if cast(SafeManifest, self.manifests).purging:
            # If we are purging, we may not be able to create any storage classes
            # Just return a fake storage class to satisfy delete_manifests method
            # which will look up all storage classes installed by this app/manifest
            return StorageClass.from_dict(dict(metadata={}, provisioner=driver_name))

        clusterID = self.manifests.config.get("fsid")
        fs_type = self._fs_type.split("-")[1]
        if not clusterID:
            log.error(f"Ceph {fs_type.capitalize()} is missing required storage item: 'fsid'")
            return None

        ns = self.manifests.config["namespace"]
        metadata: Dict = {"name": self.name()}
        if self.manifests.config.get("default-storage") == metadata["name"]:
            metadata["annotations"] = {"storageclass.kubernetes.io/is-default-class": "true"}

        log.info(f"Modelling storage class {metadata['name']}")
        parameters = {
            "clusterID": clusterID,
            "csi.storage.k8s.io/controller-expand-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/controller-expand-secret-namespace": ns,
            "csi.storage.k8s.io/fstype": fs_type,
            "csi.storage.k8s.io/node-stage-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/node-stage-secret-namespace": ns,
            "csi.storage.k8s.io/provisioner-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/provisioner-secret-namespace": ns,
            "pool": f"{fs_type}-pool",
        }

        self.update_params(parameters)

        return StorageClass.from_dict(
            dict(
                metadata=metadata,
                provisioner=driver_name,
                allowVolumeExpansion=True,
                mountOptions=["discard"],
                reclaimPolicy="Delete",
                parameters=parameters,
            )
        )


class RBDProvAdjustments(ProvisionerAdjustments):
    """Update RBD provisioner."""

    PROVISIONER_NAME = "csi-rbdplugin-provisioner"
    PLUGIN_NAME = "csi-rbdplugin"

    def tolerations(self) -> Tuple[List[CephToleration], bool]:
        cfg = self.manifests.config.get("ceph-rbd-tolerations") or ""
        return CephToleration.from_space_separated(cfg), False


class RBDManifests(SafeManifest):
    """Deployment Specific details for the rbd.csi.ceph.com."""

    DRIVER_NAME = "rbd.csi.ceph.com"

    def __init__(self, charm: "CephCsiCharm"):
        super().__init__(
            "rbd",
            charm.model,
            "upstream/rbd",
            [
                StorageSecret(self),
                ConfigRegistry(self),
                RBDProvAdjustments(self),
                CephStorageClass(self, "ceph-xfs"),  # creates ceph-xfs
                CephStorageClass(self, "ceph-ext4"),  # creates ceph-ext4
                RbacAdjustments(self),
                CSIDriverAdjustments(self, self.DRIVER_NAME),
                AdjustNamespace(self),
                ConfigureLivenessPrometheus(
                    self, "Deployment", "csi-rbdplugin-provisioner", "rbdplugin-provisioner"
                ),
                ConfigureLivenessPrometheus(
                    self, "Service", "csi-rbdplugin-provisioner", "rbdplugin-provisioner"
                ),
                ConfigureLivenessPrometheus(self, "DaemonSet", "csi-rbdplugin", "rbdplugin"),
                ConfigureLivenessPrometheus(self, "Service", "csi-metrics-rbdplugin", "rbdplugin"),
                ManifestLabel(self),
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

        config["release"] = config.get("release", None)
        config["namespace"] = self.charm.stored.namespace
        config["csidriver-name-formatter"] = self.charm.stored.drivername
        return config

    def evaluate(self) -> Optional[str]:
        """Determine if manifest_config can be applied to manifests."""
        props = (
            StorageSecret.REQUIRED_CONFIG.keys()
            | CephStorageClass.REQUIRED_CONFIG
            | RbacAdjustments.REQUIRED_CONFIG
        )
        for prop in sorted(props):
            value = self.config.get(prop)
            if not value:
                return f"RBD manifests require the definition of '{prop}'"

        pa_manipulator = next(
            m for m in self.manipulations if isinstance(m, ProvisionerAdjustments)
        )
        try:
            pa_manipulator.tolerations()
        except ValueError as err:
            return f"Cannot adjust CephRBD Pods: {err}"

        for storage_class in self.manipulations:
            if isinstance(storage_class, CephStorageClass):
                try:
                    storage_class.evaluate()
                except ValueError as err:
                    return f"RBD manifests failed to create storage classes: {err}"
        return None
