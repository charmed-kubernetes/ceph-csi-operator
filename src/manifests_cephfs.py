# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
"""Implementation of rbd specific details of the kubernetes manifests."""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, cast

from lightkube.codecs import AnyResource
from lightkube.resources.storage_v1 import StorageClass
from ops.manifests import ConfigRegistry, ManifestLabel

import literals
from manifests_base import (
    AdjustNamespace,
    CephToleration,
    ConfigureLivenessPrometheus,
    CSIDriverAdjustments,
    ProvisionerAdjustments,
    RbacAdjustments,
    RemoveResource,
    SafeManifest,
    StorageClassFactory,
    StorageSecret,
)

if TYPE_CHECKING:
    from charm import CephCsiCharm

log = logging.getLogger(__name__)
STORAGE_TYPE = "cephfs"


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
    is_default: bool


class CephFSSecret(StorageSecret):
    """Create secret for the deployment."""

    NAME = "csi-cephfs-secret"
    REQUIRED_CONFIG = {
        "user": ["userID", "adminID"],
        "kubernetes_key": ["userKey", "adminKey"],
    }


class CephStorageClass(StorageClassFactory):
    """Create ceph storage classes."""

    FILESYSTEM_LISTING = "fs_list"
    REQUIRED_CONFIG = {"fsid", FILESYSTEM_LISTING}

    def create(self, param: CephStorageClassParameters) -> AnyResource:
        """Create a storage class object."""
        driver_name = cast(SafeManifest, self.manifests).csidriver.formatted

        ns = self.manifests.config["namespace"]
        metadata: Dict[str, Any] = dict(name=param.storage_class_name)
        if param.is_default:
            metadata["annotations"] = literals.DEFAULT_SC_ANNOTATION

        log.info(f"Modelling storage class sc='{param.storage_class_name}'")
        parameters = {
            "clusterID": param.cluster_id,
            "fsName": param.filesystem_name,
            "csi.storage.k8s.io/controller-expand-secret-name": CephFSSecret.NAME,
            "csi.storage.k8s.io/controller-expand-secret-namespace": ns,
            "csi.storage.k8s.io/provisioner-secret-name": CephFSSecret.NAME,
            "csi.storage.k8s.io/provisioner-secret-namespace": ns,
            "csi.storage.k8s.io/node-stage-secret-name": CephFSSecret.NAME,
            "csi.storage.k8s.io/node-stage-secret-namespace": ns,
            "pool": param.data_pool,
        }
        mounter = self.manifests.config.get("cephfs-mounter")
        if mounter in ["kernel", "fuse"]:
            parameters["mounter"] = mounter

        self.update_params(parameters)

        return StorageClass.from_dict(
            dict(
                metadata=metadata,
                provisioner=driver_name,
                allowVolumeExpansion=True,
                reclaimPolicy="Delete",
                parameters=parameters,
            )
        )

    def parameter_list(self) -> List[CephStorageClassParameters]:
        """Accumulate names and settings of the storage classes.

        This can be a difficult problem to resolve the actual names
        of the storage classes this method creates when not all the data
        is available from the ceph cluster.

        For example, the fs_data not being available from the cluster
        means we cannot determine the data pools and cannot format a
        name for the storage class for that pool.

        In some event we cannot generate the parameter_list, this method
        will raise a ValueError exception indicated the value missing.
        """
        fsid = self.manifests.config.get("fsid")
        fs_data: List[CephFilesystem] = self.manifests.config.get(self.FILESYSTEM_LISTING) or []

        if not fsid:
            log.error("CephFS is missing a filesystem: 'fsid'")
            raise ValueError("missing fsid")

        if not fs_data:
            log.error("CephFS is missing a filesystem listing: '%s'", self.FILESYSTEM_LISTING)
            raise ValueError("missing filesystem listing")

        self.evaluate()

        sc_names: List[CephStorageClassParameters] = []
        for fs in fs_data:
            for idx, data_pool in enumerate(fs.data_pools):
                context = {
                    "name": fs.name,
                    "pool": data_pool,
                    "pool-id": fs.data_pool_ids[idx],
                }
                sc_name = self.name(context)
                sc_default = self.is_default(context)
                sc_names += [
                    CephStorageClassParameters(fsid, sc_name, fs.name, data_pool, sc_default)
                ]

        if len(set(n.storage_class_name for n in sc_names)) != len(sc_names):
            log.error(
                "The formatter cannot generate unique storage class names for all the available pools. "
                "Consider improving the config '%s' to expand to meet the number of pools."
                "\n\tfile systems    = %s"
                "\n\tstorage_classes = %s",
                self.name_formatter_key,
                fs_data,
                sc_names,
            )
            raise ValueError(f"{self.name_formatter_key} does not generate unique names")
        return sc_names

    def __call__(self) -> List[AnyResource]:
        """Craft the storage class objects."""
        driver_name = cast(SafeManifest, self.manifests).csidriver.formatted

        if cast(SafeManifest, self.manifests).purging:
            # If we are purging, we may not be able to create any storage classes
            # Just return a fake storage class to satisfy delete_manifests method
            # which will look up all storage classes installed by this app/manifest
            return [StorageClass.from_dict(dict(metadata={}, provisioner=driver_name))]

        if not self.manifests.config.get("enabled"):
            log.info("Skipping CephFS storage class creation since it's disabled")
            return []

        try:
            parameter_list = self.parameter_list()
        except ValueError as err:
            # If we cannot generate the parameter list, we cannot add
            # any storage classes
            log.error("Failed to list storage classes to add: %s", err)
            return []

        return [self.create(class_param) for class_param in parameter_list]


class FSProvAdjustments(ProvisionerAdjustments):
    """Update Cephfs provisioner."""

    PROVISIONER_NAME = "csi-cephfsplugin-provisioner"
    PLUGIN_NAME = "csi-cephfsplugin"

    def tolerations(self) -> Tuple[List[CephToleration], bool]:
        cfg = self.manifests.config.get("cephfs-tolerations") or ""
        if cfg == "$csi-cephfsplugin-legacy$":
            return [], True
        return CephToleration.from_space_separated(cfg), False


class CephFSManifests(SafeManifest):
    """Deployment Specific details for the cephfs.csi.ceph.com driver."""

    DRIVER_NAME = "cephfs.csi.ceph.com"

    def __init__(self, charm: "CephCsiCharm"):
        super().__init__(
            STORAGE_TYPE,
            charm.model,
            "upstream/cephfs",
            [
                CephFSSecret(self),
                ConfigRegistry(self),
                FSProvAdjustments(self),
                CephStorageClass(self, STORAGE_TYPE),
                CSIDriverAdjustments(self, self.DRIVER_NAME),
                RbacAdjustments(self),
                RemoveResource(self),
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
        config["enabled"] = config.get(literals.CONFIG_CEPHFS_ENABLE, None)
        config["namespace"] = self.charm.stored.namespace
        config["csidriver-name-formatter"] = self.charm.stored.drivername
        return config

    def evaluate(self) -> Optional[str]:
        """Determine if manifest_config can be applied to manifests."""
        if not self.config.get("enabled"):
            log.info("Skipping CephFS evaluation since it's disabled")
            return None

        props = CephFSSecret.REQUIRED_CONFIG.keys() | RbacAdjustments.REQUIRED_CONFIG
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

        storage_class = next(m for m in self.manipulations if isinstance(m, CephStorageClass))
        try:
            storage_class.parameter_list()
        except ValueError as err:
            return f"CephFS manifests failed to create storage classes: {err}"

        return None
