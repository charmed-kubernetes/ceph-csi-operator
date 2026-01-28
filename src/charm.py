#!/usr/bin/env python3
# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
"""Dispatch logic for the ceph-csi storage charm."""

import json
import logging
from functools import cached_property
from typing import Any, Dict, List, Optional, cast

import charms.contextual_status as status
import charms.operator_libs_linux.v0.apt as apt
import ceph_csi
import ops
from charms.reconciler import Reconciler
from interface_ceph_client import ceph_client  # type: ignore
from lightkube import Client, KubeConfig
from lightkube.core.exceptions import ApiError
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Namespace
from lightkube.resources.storage_v1 import StorageClass
from ops.manifests import Collector, ManifestClientError, ResourceAnalysis

import literals
import utils
from manifests_base import Manifests, SafeManifest
from manifests_cephfs import CephFilesystem, CephFSManifests, CephStorageClass
from manifests_config import ConfigManifests
from manifests_rbd import RBDManifests

logger = logging.getLogger(__name__)


class CephCsiCharm(ops.CharmBase):
    """Charm the service."""

    stored = ops.StoredState()

    def __init__(self, *args: Any) -> None:
        """Setup even observers and initial storage values."""
        super().__init__(*args)
        self.ceph_client = ceph_client.CephClientRequires(self, literals.CEPH_CLIENT_RELATION)
        self.ceph_csi = ceph_csi.CephCSIRequires(self, literals.CEPH_CSI_RELATION)

        self.framework.observe(
            self.ceph_client.on.broker_available, self._on_ceph_client_broker_available
        )
        self.framework.observe(
            self.ceph_csi.on.ceph_csi_available, self._on_ceph_csi_available
        )
        self.framework.observe(
            self.ceph_csi.on.ceph_csi_connected, self._on_ceph_csi_connected
        )
        self.framework.observe(
            self.ceph_csi.on.ceph_csi_departed, self._on_ceph_csi_departed
        )
        self.cli = utils.CephCLI(self)
        self.reconciler = Reconciler(self, self.reconcile)

        self.framework.observe(self.on.list_versions_action, self._list_versions)
        self.framework.observe(self.on.list_resources_action, self._list_resources)
        self.framework.observe(self.on.scrub_resources_action, self._scrub_resources)
        self.framework.observe(self.on.sync_resources_action, self._sync_resources)
        self.framework.observe(self.on.delete_storage_class_action, self._delete_storage_class)
        self.framework.observe(self.on.update_status, self._on_update_status)

        self.stored.set_default(config_hash=0)  # hashed value of the provider config once valid
        self.stored.set_default(destroying=False)  # True when the charm is being shutdown
        self.stored.set_default(namespace=self._configured_ns)
        self.stored.set_default(drivername=self._configured_drivername)

        self.collector = Collector(
            ConfigManifests(self),
            CephFSManifests(self),
            RBDManifests(self),
        )

    def _list_versions(self, event: ops.ActionEvent) -> None:
        self.collector.list_versions(event)

    def _list_resources(self, event: ops.ActionEvent) -> None:
        manifests = event.params.get("manifest", "")
        resources = event.params.get("resources", "")
        self.collector.list_resources(event, manifests, resources)

    def _scrub_resources(self, event: ops.ActionEvent) -> None:
        manifests = event.params.get("manifest", "")
        resources = event.params.get("resources", "")
        self.collector.scrub_resources(event, manifests, resources)

    def _sync_resources(self, event: ops.ActionEvent) -> None:
        manifests = event.params.get("manifest", "")
        resources = event.params.get("resources", "")
        try:
            self.collector.apply_missing_resources(event, manifests, resources)
        except ManifestClientError as e:
            msg = "Failed to sync missing resources: "
            msg += " -> ".join(map(str, e.args))
            event.set_results({"result": msg})
        else:
            self.stored.deployed = True

    def _delete_storage_class(self, event: ops.ActionEvent) -> None:
        storage_class: Optional[str] = event.params.get("name")
        if storage_class not in ["cephfs", "ceph-xfs", "ceph-ext4"]:
            msg = "Invalid storage class name. Must be one of: cephfs, ceph-xfs, ceph-ext4"
            event.fail(msg)
            return

        manifest, *_ = self.collector.manifests.values()
        try:
            manifest.client.delete(StorageClass, name=storage_class)
        except ManifestClientError as e:
            msg = "Failed to delete storage class: "
            msg += " -> ".join(map(str, e.args))
            event.fail(msg)
        else:
            event.set_results({"result": f"Successfully deleted StorageClass/{storage_class}"})

    def _update_status(self) -> None:
        if not (self.config["ceph-rbd-enable"] or self.config["cephfs-enable"]):
            msg = "Neither ceph-rbd nor cephfs is enabled."
            status.add(ops.BlockedStatus(msg))
            raise status.ReconcilerError(msg)
        elif unready := self.collector.unready:
            status.add(ops.WaitingStatus(", ".join(unready)))
            raise status.ReconcilerError("Waiting for deployment")
        elif self.stored.namespace != self._configured_ns:
            status.add(ops.BlockedStatus("Namespace cannot be changed after deployment"))
        elif self.stored.drivername != self._configured_drivername:
            status.add(
                ops.BlockedStatus("csidriver-name-formatter cannot be changed after deployment")
            )
        else:
            self.unit.set_workload_version(self.collector.short_version)
            if self.unit.is_leader():
                self.app.status = ops.ActiveStatus(self.collector.long_version)

    def _on_update_status(self, _: ops.EventBase) -> None:
        if not self.reconciler.stored.reconciled:
            return
        try:
            with status.context(self.unit):
                self._update_status()
        except status.ReconcilerError:
            logger.exception("Can't update_status")

    @property
    def _configured_ns(self) -> str:
        """Currently configured namespace."""
        return str(self.config.get("namespace") or literals.DEFAULT_NAMESPACE)

    @property
    def _configured_drivername(self) -> str:
        """Currently configured csi drivername."""
        return str(self.config.get("csidriver-name-formatter") or "{name}")

    @property
    def ceph_data(self) -> Dict[str, Any]:
        """Return Ceph data from ceph-client relation"""
        csi_data = self.ceph_csi.get_relation_data()
        if csi_data:
            return {
                "auth": "cephx",
                "key": csi_data.get("user_key"),
                "mon_hosts": csi_data.get("mon_hosts"),
                "user_id": csi_data.get("user_id"),
                "fsid": csi_data.get("fsid"),
            }

        r_data = self.ceph_client.get_relation_data()
        return {k: r_data.get(k) for k in ("auth", "key", "mon_hosts")}

    @property
    def auth(self) -> Optional[str]:
        """Return Ceph auth mode from ceph-client relation"""
        return self.ceph_data["auth"]

    @property
    def key(self) -> Optional[str]:
        """Return Ceph key from ceph-client relation"""
        return self.ceph_data["key"]

    @property
    def mon_hosts(self) -> List[str]:
        """Return Ceph monitor hosts from ceph-client relation"""
        return self.ceph_data["mon_hosts"] or []

    @property
    def ceph_user(self) -> str:
        """Return the Ceph user name for CLI operations."""
        csi_data = self.ceph_csi.get_relation_data()
        if csi_data and csi_data.get("user_id"):
            return csi_data["user_id"]
        return self.app.name

    @status.on_error(ops.BlockedStatus("Failed to install ceph apt packages."))
    def install_ceph_packages(self, event: ops.EventBase) -> None:
        """Install ceph deb packages"""
        for package in literals.CEPH_PACKAGES:
            self.unit.status = ops.MaintenanceStatus(f"Ensuring {package} package")
            latest = isinstance(event, (ops.InstallEvent, ops.UpgradeCharmEvent))
            state = apt.PackageState.Latest if latest else apt.PackageState.Present
            ceph = apt.DebianPackage.from_system(package)
            ceph.ensure(state)
            logger.info("Installing %s to version: %s", package, ceph.fullversion)

    @property
    def provisioner_replicas(self) -> int:
        """Get the number of csi-*plugin-provisioner replicas."""
        return int(self.config.get("provisioner-replicas") or 3)

    @property
    def enable_host_network(self) -> bool:
        """Get the hostNetwork enabling of csi-*plugin-provisioner deployments."""
        return bool(self.config.get("enable-host-networking"))

    @property
    def kubelet_dir(self) -> str:
        """Get the kubelet directory from the kubernetes-info relation (if available)"""
        relation = self.model.get_relation("kubernetes-info")
        if relation is None or relation.app is None:
            return "/var/lib/kubelet"

        return relation.data[relation.app].get("kubelet-root-dir") or "/var/lib/kubelet"

    @property
    def kubernetes_context(self) -> Dict[str, Any]:
        """Return context that can be used to render ceph resource files in templates/ folder."""
        return {"kubelet_dir": self.kubelet_dir}

    @cached_property
    def _client(self) -> Client:
        """Lightkube Client instance."""
        return Client(field_manager=f"{self.model.app.name}")

    @property
    def ceph_context(self) -> Dict[str, Any]:
        """Return context that can be used to render ceph resource files in templates/ folder."""
        csi_data = self.ceph_csi.get_relation_data()
        fsid = csi_data.get("fsid") if csi_data else utils.fsid(self.cli)
        user = csi_data.get("user_id") if csi_data else self.app.name
        user_key = csi_data.get("user_key") if csi_data else self.key

        # Get filesystem listing - try CLI first, fall back to relation data
        fs_list = utils.ls_ceph_fs(self.cli)
        if not fs_list and csi_data:
            fs_list = self._cephfs_from_relation(csi_data)

        return {
            "auth": self.auth,
            "fsid": fsid,
            "kubernetes_key": user_key,
            "mon_hosts": self.mon_hosts,
            "user": user,
            "provisioner_replicas": self.provisioner_replicas,
            "enable_host_network": json.dumps(self.enable_host_network),
            CephStorageClass.FILESYSTEM_LISTING: fs_list,
            "rbd_pool": csi_data.get("rbd_pool") if csi_data else None,
        }

    def _cephfs_from_relation(self, csi_data: Dict[str, Any]) -> List[CephFilesystem]:
        """Construct CephFilesystem list from ceph-csi relation data."""
        fs_name = csi_data.get("cephfs_fs_name")
        if not fs_name:
            return []

        # Use Ceph's modern naming convention for pools
        # MicroCeph creates pools as: cephfs.{fs_name}.meta and cephfs.{fs_name}.data
        return [CephFilesystem(
            name=fs_name,
            metadata_pool=f"cephfs.{fs_name}.meta",
            metadata_pool_id=0,
            data_pool_ids=[0],
            data_pools=[f"cephfs.{fs_name}.data"],
        )]

    @status.on_error(ops.WaitingStatus("Waiting for kubeconfig"))
    def check_kube_config(self) -> None:
        self.unit.status = ops.MaintenanceStatus("Evaluating kubernetes authentication")
        KubeConfig.from_env()

    def _create_namespace(self, namespace: str) -> Optional[ops.StatusBase]:
        """Create the namespace if it does not exist.

        Args:
            namespace (str): The name of the namespace to create.

        Returns:
            Optional[ops.StatusBase]: Returns None if the namespace was created or already exists,
            or a status indicating an error if the namespace could not be created.
        """
        if not self.unit.is_leader():
            logger.info("Waiting for namespace creation, not the leader")
            return ops.WaitingStatus(f"Waiting for namespace '{namespace}'")

        if not self.config["create-namespace"]:
            logger.info("Skipping namespace creation, create-namespace is False")
            return ops.BlockedStatus(f"Missing namespace '{namespace}'")

        ns_resource = Namespace(metadata=ObjectMeta(name=namespace))
        logger.info("Creating namespace '%s'", namespace)
        status.add(ops.MaintenanceStatus(f"Creating namespace: '{namespace}'"))
        try:
            self._client.create(ns_resource)
        except ApiError as e:
            if e.status.code == 409:  # Conflict
                # Namespace already exists, do not raise an error
                logger.info("Namespace '%s' already exists", namespace)
            else:
                logger.exception("Failed to create namespace '%s': %s", namespace, e)
                return ops.WaitingStatus(f"Waiting for namespace: {namespace}")
        return None

    def check_namespace(self) -> None:
        namespace = str(self.stored.namespace)
        self.unit.status = ops.MaintenanceStatus(f"Evaluating namespace: '{namespace}'")
        try:
            self._client.get(Namespace, name=namespace)
        except ApiError as e:
            if e.status.code == 404:
                if error := self._create_namespace(namespace):
                    status.add(error)
                    raise status.ReconcilerError(error.message)
            else:
                # surface any other errors besides not found
                status.add(ops.WaitingStatus("Waiting for Kubernetes API"))
                raise status.ReconcilerError("Waiting for Kubernetes API")

    @status.on_error(ops.WaitingStatus("Waiting for kubeconfig"))
    def _ceph_rbd_enabled(self) -> None:
        """Determine if CephRBD should be enabled or disabled."""

        if self.config["ceph-rbd-enable"]:
            self.unit.status = ops.MaintenanceStatus("Enabling CephRBD")
        else:
            self.unit.status = ops.MaintenanceStatus("Disabling CephRBD")
            if self.unit.is_leader():
                self._purge_manifest_by_name("rbd")

    @status.on_error(ops.WaitingStatus("Waiting for kubeconfig"))
    def _cephfs_enabled(self) -> None:
        """Determine if CephFS should be enabled or disabled."""
        if self.config["cephfs-enable"]:
            self.unit.status = ops.MaintenanceStatus("Enabling CephFS")
            groups = {literals.CEPHFS_SUBVOLUMEGROUP}
            for volume in utils.ls_ceph_fs(self.cli):
                utils.ensure_subvolumegroups(self.cli, volume.name, groups)
        else:
            self.unit.status = ops.MaintenanceStatus("Disabling CephFS")
            if self.unit.is_leader():
                self._purge_manifest_by_name("cephfs")

    def prevent_collisions(self, event: ops.EventBase) -> None:
        """Prevent manifest collisions."""
        if self.unit.is_leader():
            self.unit.status = ops.MaintenanceStatus("Detecting manifest collisions")
            analyses: List[ResourceAnalysis] = self.collector.analyze_resources(event, "", "")
            count = sum(len(a.conflicting) for a in analyses)
            if count > 0:
                msg = f"{count} Kubernetes resource collision{'s'[:count^1]} (action: list-resources)"
                logger.error(msg)
                for analysis in analyses:
                    if analysis.conflicting:
                        logger.error(
                            " Collision count in '%s' is %d",
                            analysis.manifest,
                            len(analysis.conflicting),
                        )
                        for _ in sorted(map(str, analysis.conflicting)):
                            logger.error("   %s", _)
                status.add(ops.BlockedStatus(msg))
                raise status.ReconcilerError(msg)

    def evaluate_manifests(self) -> int:
        """Evaluate all manifests."""
        self.unit.status = ops.MaintenanceStatus("Evaluating CephCSI")
        new_hash = 0
        for manifest in self.collector.manifests.values():
            manifest = cast(SafeManifest, manifest)
            if evaluation := manifest.evaluate():
                status.add(ops.BlockedStatus(evaluation))
                raise status.ReconcilerError(evaluation)
            new_hash += manifest.hash()
        return new_hash

    def install_manifests(self, config_hash: int) -> None:
        if cast(int, self.stored.config_hash) == config_hash:
            logger.info(f"No config changes detected. config_hash={config_hash}")
            return
        if self.unit.is_leader():
            self.unit.status = ops.MaintenanceStatus("Deploying CephCSI")
            self.unit.set_workload_version("")
            for manifest in self.collector.manifests.values():
                try:
                    manifest.apply_manifests()
                except ManifestClientError as e:
                    failure_msg = " -> ".join(map(str, e.args))
                    status.add(ops.WaitingStatus(failure_msg))
                    logger.warning("Encountered retriable installation error: %s", e)
                    raise status.ReconcilerError(failure_msg)

        self.stored.config_hash = config_hash

    def check_ceph_client(self) -> None:
        """Load data from ceph-mon:client relation and store it in StoredState.

        This method expects all the required data (key, mon_hosts) to be present in the
        relation data. If any of the expected keys is missing, none of the data will be loaded.

        :param relation: Instance of ceph-mon:client relation.
        :param remote_unit: Unit instance representing remote ceph-mon unit.
        :return: `True` if all the data successfully loaded, otherwise `False`
        """
        self.unit.status = ops.MaintenanceStatus("Checking Relations")

        # Check for mutual exclusivity of ceph-csi and ceph-client relations
        csi_relation = self.model.get_relation(literals.CEPH_CSI_RELATION)
        client_relation = self.model.get_relation(literals.CEPH_CLIENT_RELATION)

        if csi_relation and client_relation:
            msg = "Both ceph-csi and ceph-client relations are active. Only one is allowed."
            status.add(ops.BlockedStatus(msg))
            raise status.ReconcilerError(msg)

        csi_data = self.ceph_csi.get_relation_data()
        if csi_data:
            expected_relation_keys = ("fsid", "mon_hosts", "user_id", "user_key")
            missing_data = [key for key in expected_relation_keys if not csi_data.get(key)]
            if missing_data:
                logger.warning("ceph-csi relation is missing data: %s", missing_data)
                status.add(ops.WaitingStatus("ceph-csi relation is missing data."))
                raise status.ReconcilerError("ceph-csi relation is missing data.")
            return

        try:
            relation = self.model.get_relation(literals.CEPH_CLIENT_RELATION)
        except ops.model.TooManyRelatedAppsError:
            status.add(ops.BlockedStatus("Multiple ceph-client relations"))
            raise status.ReconcilerError("Multiple ceph-client relations")

        if not relation:
            status.add(ops.BlockedStatus("Missing relation: ceph-client"))
            raise status.ReconcilerError("Missing relation: ceph-client")

        relation_data = self.ceph_client.get_relation_data()
        expected_relation_keys = ("auth", "key", "mon_hosts")

        missing_data = [key for key in expected_relation_keys if key not in relation_data]
        if missing_data:
            logger.warning("Ceph relation is missing data: %s", missing_data)
            status.add(ops.WaitingStatus("Ceph relation is missing data."))
            raise status.ReconcilerError("Ceph relation is missing data.")

    def reconcile(self, event: ops.EventBase) -> None:
        """Reconcile the charm state."""
        if self._destroying(event):
            leader = self.unit.is_leader()
            logger.info("purge manifests if leader(%s) event(%s)", leader, event)
            if leader:
                self._purge_all_manifests()
            return

        self.install_ceph_packages(event)
        self.check_kube_config()
        self.check_namespace()
        self.check_ceph_client()
        self.cli.configure()
        self._ceph_rbd_enabled()
        self._cephfs_enabled()
        hash = self.evaluate_manifests()
        self.prevent_collisions(event)
        self.install_manifests(config_hash=hash)
        self._update_status()

    def request_ceph_pools(self) -> None:
        """Request creation of Ceph pools from the ceph-client relation"""
        for pool_name in literals.REQUIRED_CEPH_POOLS:
            self.ceph_client.create_replicated_pool(name=pool_name)

    def request_ceph_permissions(self) -> None:
        """Request Ceph permissions from the ceph-client relation"""
        # Permissions needed for Ceph CSI
        # https://github.com/ceph/ceph-csi/blob/v3.6.0/docs/capabilities.md
        permissions = [
            "mon",
            "profile rbd, allow r",
            "mds",
            "allow rw",
            "mgr",
            "allow rw",
            "osd",
            "profile rbd, allow rw tag cephfs metadata=*",
        ]
        self.ceph_client.request_ceph_permissions(self.app.name, permissions)

    def _on_ceph_client_broker_available(self, event: ops.EventBase) -> None:
        """Use ceph-mon:client relation to request creation of ceph-pools and
        ceph user permissions
        """
        self.request_ceph_pools()
        self.request_ceph_permissions()
        self.reconciler.reconcile(event)

    def _on_ceph_csi_available(self, event: ops.EventBase) -> None:
        """Handle ceph-csi available event."""
        self.ceph_csi.request_workloads(["rbd", "cephfs"])

    def _on_ceph_csi_connected(self, event: ops.EventBase) -> None:
        """Handle ceph-csi connected event."""
        self.reconciler.reconcile(event)

    def _on_ceph_csi_departed(self, event: ops.EventBase) -> None:
        """Handle ceph-csi departed event."""
        if self.unit.is_leader():
            self._purge_all_manifests()

    def _purge_all_manifests(self) -> None:
        """Purge resources created by this charm."""
        self.unit.status = ops.MaintenanceStatus("Removing Kubernetes resources")
        for manifest in self.collector.manifests.values():
            self._purge_manifest(manifest)
        ceph_pools = ", ".join(literals.REQUIRED_CEPH_POOLS)
        logger.warning(
            "Ceph pools %s won't be removed. If you want to clean up pools manually,"
            "use `juju run ceph-mon/leader delete-pool`",
            ceph_pools,
        )
        self.stored.config_hash = 0

    def _purge_manifest_by_name(self, name: str) -> None:
        """Purge resources created by this charm by manifest name."""
        self.unit.status = ops.MaintenanceStatus(f"Removing {name} resources")
        for manifest in self.collector.manifests.values():
            if manifest.name == name:
                self._purge_manifest(manifest)

    @status.on_error(ops.WaitingStatus("Manifest purge failed."))
    def _purge_manifest(self, manifest: Manifests) -> None:
        """Purge resources created by this charm by manifest."""
        manifest = cast(SafeManifest, manifest)
        manifest.purging = True
        manifest.delete_manifests(ignore_unauthorized=True, ignore_not_found=True)
        manifest.purging = False

    def _destroying(self, event: ops.EventBase) -> bool:
        """Check if the charm is being destroyed."""
        if cast(bool, self.stored.destroying):
            return True
        if isinstance(event, (ops.StopEvent, ops.RemoveEvent)):
            self.stored.destroying = True
            return True
        elif isinstance(event, ops.RelationBrokenEvent) and event.relation.name in (
            literals.CEPH_CLIENT_RELATION,
            literals.CEPH_CSI_RELATION,
        ):
            return True
        return False


if __name__ == "__main__":  # pragma: no cover
    ops.main.main(CephCsiCharm)
