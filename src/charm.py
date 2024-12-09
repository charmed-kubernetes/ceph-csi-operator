#!/usr/bin/env python3
# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
"""Dispatch logic for the ceph-csi storage charm."""

import configparser
import json
import logging
import subprocess
from functools import cached_property, lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

import charms.contextual_status as status
import charms.operator_libs_linux.v0.apt as apt
import ops
from charms.reconciler import Reconciler
from interface_ceph_client import ceph_client  # type: ignore
from lightkube import Client, KubeConfig
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import Namespace
from lightkube.resources.storage_v1 import StorageClass
from ops.manifests import Collector, ManifestClientError

from manifests_base import Manifests, SafeManifest
from manifests_cephfs import CephFSManifests, CephStorageClass
from manifests_config import ConfigManifests
from manifests_rbd import RBDManifests

logger = logging.getLogger(__name__)


def ceph_config_dir() -> Path:
    return Path.cwd() / "ceph-conf"


def ceph_config_file() -> Path:
    return ceph_config_dir() / "ceph.conf"


def ceph_keyring_file(app_name: str) -> Path:
    return ceph_config_dir() / f"ceph.client.{app_name}.keyring"


class CephCsiCharm(ops.CharmBase):
    """Charm the service."""

    CEPH_CLIENT_RELATION = "ceph-client"
    REQUIRED_CEPH_POOLS = ["xfs-pool", "ext4-pool"]
    DEFAULT_NAMESPACE = "default"

    stored = ops.StoredState()

    def __init__(self, *args: Any) -> None:
        """Setup even observers and initial storage values."""
        super().__init__(*args)
        self.ceph_client = ceph_client.CephClientRequires(self, "ceph-client")

        self.framework.observe(
            self.ceph_client.on.broker_available, self._on_ceph_client_broker_available
        )
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
        return self.collector.list_resources(event, manifests, resources)

    def _scrub_resources(self, event: ops.ActionEvent) -> None:
        manifests = event.params.get("manifest", "")
        resources = event.params.get("resources", "")
        return self.collector.scrub_resources(event, manifests, resources)

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
        unready = self.collector.unready
        if unready:
            status.add(ops.WaitingStatus(", ".join(unready)))
            raise status.ReconcilerError("Waiting for deployment")
        elif self.stored.namespace != self._configured_ns:
            status.add(ops.BlockedStatus("Namespace cannot be changed after deployment"))
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
        return str(self.config.get("namespace") or self.DEFAULT_NAMESPACE)

    @property
    def ceph_data(self) -> Dict[str, Any]:
        """Return Ceph data from ceph-client relation"""
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

    @status.on_error(ops.BlockedStatus("Failed to install ceph-common apt package."))
    def install_ceph_common(self, event: ops.EventBase) -> None:
        """Install ceph-common apt package"""
        self.unit.status = ops.MaintenanceStatus("Ensuring ceph-common package")
        latest = isinstance(event, (ops.InstallEvent, ops.UpgradeCharmEvent))
        state = apt.PackageState.Latest if latest else apt.PackageState.Present
        ceph = apt.DebianPackage.from_system("ceph-common")
        ceph.ensure(state)
        logger.info("Installing ceph-common to version: %s", ceph.fullversion)

    def write_ceph_cli_config(self) -> None:
        """Write Ceph CLI .conf file"""
        config = configparser.ConfigParser()
        unit_name = self.unit.name.replace("/", "-")
        config["global"] = {
            "auth cluster required": self.auth or "",
            "auth service required": self.auth or "",
            "auth client required": self.auth or "",
            "keyring": f"{ceph_config_dir()}/$cluster.$name.keyring",
            "mon host": " ".join(self.mon_hosts),
            "log to syslog": "true",
            "err to syslog": "true",
            "clog to syslog": "true",
            "mon cluster log to syslog": "true",
            "debug mon": "1/5",
            "debug osd": "1/5",
        }
        config["client"] = {"log file": f"/var/log/ceph/{unit_name}.log"}

        with ceph_config_file().open("w") as fp:
            config.write(fp)

    def write_ceph_cli_keyring(self) -> None:
        """Write Ceph CLI keyring file"""
        config = configparser.ConfigParser()
        config[f"client.{self.app.name}"] = {"key": self.key or ""}
        with ceph_keyring_file(self.app.name).open("w") as fp:
            config.write(fp)

    def configure_ceph_cli(self) -> None:
        """Configure Ceph CLI"""
        ceph_config_dir().mkdir(mode=0o700, parents=True, exist_ok=True)
        self.write_ceph_cli_config()
        self.write_ceph_cli_keyring()

    def ceph_cli(self, *args: str, timeout: int = 60) -> str:
        """Run Ceph CLI command"""
        conf = ceph_config_file().absolute().as_posix()
        cmd = ["/usr/bin/ceph", "--conf", conf, "--user", self.app.name, *args]
        return subprocess.check_output(cmd, timeout=timeout).decode("UTF-8")

    @lru_cache(maxsize=None)
    def get_ceph_fsid(self) -> str:
        """Get the Ceph FSID (cluster ID)"""
        try:
            return self.ceph_cli("fsid").strip()
        except subprocess.SubprocessError:
            logger.error("get_ceph_fsid: Failed to get CephFS ID, reporting as empty string")
            return ""

    @lru_cache(maxsize=None)
    def get_ceph_fsname(self) -> Optional[str]:
        """Get the Ceph FS Name."""
        try:
            data = json.loads(self.ceph_cli("fs", "ls", "-f", "json"))
        except (subprocess.SubprocessError, ValueError) as e:
            logger.error(
                "get_ceph_fsname: Failed to get CephFS name, reporting as None, error: %s", e
            )
            return None
        for fs in data:
            if CephStorageClass.POOL in fs["data_pools"]:
                logger.error("get_ceph_fsname: got cephfs name: %s", fs["name"])
                return fs["name"]
        return None

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
        return {
            "kubelet_dir": self.kubelet_dir,
        }

    @cached_property
    def _client(self) -> Client:
        """Lightkube Client instance."""
        return Client(field_manager=f"{self.model.app.name}")

    @property
    def ceph_context(self) -> Dict[str, Any]:
        """Return context that can be used to render ceph resource files in templates/ folder."""
        return {
            "auth": self.auth,
            "fsid": self.get_ceph_fsid(),
            "kubernetes_key": self.key,
            "mon_hosts": self.mon_hosts,
            "user": self.app.name,
            "provisioner_replicas": self.provisioner_replicas,
            "enable_host_network": json.dumps(self.enable_host_network),
            "fsname": self.get_ceph_fsname(),
        }

    @status.on_error(ops.WaitingStatus("Waiting for kubeconfig"))
    def check_kube_config(self) -> None:
        self.unit.status = ops.MaintenanceStatus("Evaluating kubernetes authentication")
        KubeConfig.from_env()

    def check_namespace(self) -> None:
        self.unit.status = ops.MaintenanceStatus("Evaluating namespace")
        try:
            self._client.get(Namespace, name=self.stored.namespace)  # type: ignore
        except ApiError as e:
            if "not found" in str(e.status.message):
                status.add(ops.BlockedStatus(f"Missing namespace '{self.stored.namespace}'"))
                raise status.ReconcilerError("Namespace not found")
            else:
                # surface any other errors besides not found
                status.add(ops.WaitingStatus("Waiting for Kubernetes API"))
                raise status.ReconcilerError("Waiting for Kubernetes API")

    @status.on_error(ops.BlockedStatus("CephFS is not usable; set 'cephfs-enable=False'"))
    def check_cephfs(self) -> None:
        self.unit.status = ops.MaintenanceStatus("Evaluating CephFS capability")
        disabled = self.config.get("cephfs-enable") is False
        if disabled:
            # not enabled, not a problem
            logger.info("CephFS is disabled")
        elif not self.ceph_context.get("fsname", None):
            logger.error(
                "Ceph CLI failed to find a CephFS fsname. Run 'juju config cephfs-enable=False' until ceph-fs is usable."
            )
            raise status.ReconcilerError("CephFS is not usable; set 'cephfs-enable=False'")

        if disabled and self.unit.is_leader():
            self._purge_manifest_by_name("cephfs")

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
        if not self.model.get_relation(self.CEPH_CLIENT_RELATION):
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

        self.install_ceph_common(event)
        self.check_kube_config()
        self.check_namespace()
        self.check_ceph_client()
        self.configure_ceph_cli()
        self.check_cephfs()
        hash = self.evaluate_manifests()
        self.install_manifests(config_hash=hash)
        self._update_status()

    def request_ceph_pools(self) -> None:
        """Request creation of Ceph pools from the ceph-client relation"""
        for pool_name in self.REQUIRED_CEPH_POOLS:
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

    def _purge_all_manifests(self) -> None:
        """Purge resources created by this charm."""
        self.unit.status = ops.MaintenanceStatus("Removing Kubernetes resources")
        for manifest in self.collector.manifests.values():
            self._purge_manifest(manifest)
        ceph_pools = ", ".join(self.REQUIRED_CEPH_POOLS)
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
        manifest.purgeable = True
        manifest.delete_manifests(ignore_unauthorized=True, ignore_not_found=True)
        manifest.purgeable = False

    def _destroying(self, event: ops.EventBase) -> bool:
        """Check if the charm is being destroyed."""
        if cast(bool, self.stored.destroying):
            return True
        if isinstance(event, (ops.StopEvent, ops.RemoveEvent)):
            self.stored.destroying = True
            return True
        elif isinstance(event, ops.RelationBrokenEvent) and event.relation.name == "ceph-client":
            return True
        return False


if __name__ == "__main__":  # pragma: no cover
    ops.main.main(CephCsiCharm)
