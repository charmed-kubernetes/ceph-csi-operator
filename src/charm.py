#!/usr/bin/env python3
# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
"""Dispatch logic for the ceph-csi storage charm."""

import configparser
import json
import logging
import subprocess
from functools import cached_property, lru_cache, wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, cast

import charms.operator_libs_linux.v0.apt as apt
from interface_ceph_client import ceph_client  # type: ignore
from lightkube import Client, KubeConfig
from lightkube.core.exceptions import ApiError, ConfigError
from lightkube.resources.core_v1 import Namespace
from lightkube.resources.storage_v1 import StorageClass
from ops.charm import ActionEvent, CharmBase, EventBase, UpdateStatusEvent
from ops.framework import StoredState
from ops.main import main
from ops.manifests import Collector, ManifestClientError
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus, WaitingStatus

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


def needs_leader(func: Callable) -> Callable:
    """Ensure that function with this decorator is executed only if on leader units."""

    @wraps(func)
    def leader_check(self: CharmBase, *args: Any, **kwargs: Any) -> Any:
        if self.unit.is_leader():
            return func(self, *args, **kwargs)
        logger.info(
            "Execution of function '%s' skipped. This function can be executed only by the leader"
            " unit.",
            func.__name__,
        )
        return None

    return leader_check


class CephCsiCharm(CharmBase):
    """Charm the service."""

    CEPH_CLIENT_RELATION = "ceph-client"
    REQUIRED_CEPH_POOLS = ["xfs-pool", "ext4-pool"]
    DEFAULT_NAMESPACE = "default"

    stored = StoredState()

    def __init__(self, *args: Any) -> None:
        """Setup even observers and initial storage values."""
        super().__init__(*args)
        self.ceph_client = ceph_client.CephClientRequires(self, "ceph-client")
        self.framework.observe(
            self.ceph_client.on.broker_available, self._on_ceph_client_broker_available
        )
        self.framework.observe(self.on.ceph_client_relation_changed, self._merge_config)
        self.framework.observe(self.on.ceph_client_relation_broken, self._on_ceph_client_removed)

        self.framework.observe(self.on.list_versions_action, self._list_versions)
        self.framework.observe(self.on.list_resources_action, self._list_resources)
        self.framework.observe(self.on.scrub_resources_action, self._scrub_resources)
        self.framework.observe(self.on.sync_resources_action, self._sync_resources)
        self.framework.observe(self.on.delete_storage_class_action, self._delete_storage_class)
        self.framework.observe(self.on.update_status, self._update_status)

        self.framework.observe(self.on.install, self._on_install_or_upgrade)
        self.framework.observe(self.on.upgrade_charm, self._on_install_or_upgrade)
        self.framework.observe(self.on.leader_elected, self._merge_config)
        self.framework.observe(self.on.config_changed, self._merge_config)
        self.framework.observe(self.on.stop, self._cleanup)

        self.framework.observe(self.on.kubernetes_info_relation_joined, self._merge_config)
        self.framework.observe(self.on.kubernetes_info_relation_changed, self._merge_config)
        self.framework.observe(self.on.kubernetes_info_relation_broken, self._merge_config)

        self.stored.set_default(ceph_data={})
        self.stored.set_default(config_hash=0)  # hashed value of the provider config once valid
        self.stored.set_default(deployed=False)  # True if config has been applied after new hash
        self.stored.set_default(namespace=self._configured_ns)

        self.collector = Collector(
            ConfigManifests(self),
            CephFSManifests(self),
            RBDManifests(self),
        )

    @property
    def _configured_ns(self) -> str:
        """Currently configured namespace."""
        return str(self.config.get("namespace") or self.DEFAULT_NAMESPACE)

    def _ops_wait_for(self, event: EventBase, msg: str) -> str:
        self.unit.status = WaitingStatus(msg)
        if not isinstance(event, UpdateStatusEvent):
            event.defer()
        return msg

    def _ops_blocked_by(self, msg: str, exc_info: bool = False) -> str:
        self.unit.status = BlockedStatus(msg)
        if exc_info:
            logger.exception(msg)
        return msg

    def _list_versions(self, event: ActionEvent) -> None:
        self.collector.list_versions(event)

    def _list_resources(self, event: ActionEvent) -> None:
        manifests = event.params.get("manifest", "")
        resources = event.params.get("resources", "")
        return self.collector.list_resources(event, manifests, resources)

    def _scrub_resources(self, event: ActionEvent) -> None:
        manifests = event.params.get("manifest", "")
        resources = event.params.get("resources", "")
        return self.collector.scrub_resources(event, manifests, resources)

    def _sync_resources(self, event: ActionEvent) -> None:
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

    def _delete_storage_class(self, event: ActionEvent) -> None:
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
            self.stored.deployed = True

    def _update_status(self, event: EventBase) -> None:
        if not cast(bool, self.stored.deployed):
            self._merge_config(event)
            return
        self.unit.status = MaintenanceStatus("Updating Status")

        unready = self.collector.unready
        current_ns, config_ns = self.stored.namespace, self._configured_ns
        if unready:
            self.unit.status = WaitingStatus(", ".join(unready))
        elif current_ns != config_ns:
            self._ops_blocked_by(f"Namespace '{current_ns}' cannot be configured to '{config_ns}'")
        else:
            self.unit.status = ActiveStatus("Unit is ready")
            self.unit.set_workload_version(self.collector.short_version)
            if self.unit.is_leader():
                self.app.status = ActiveStatus(self.collector.long_version)

    @property
    def _ceph_data(self) -> Dict[str, Any]:
        return cast(Dict[str, Any], self.stored.ceph_data)

    @property
    def auth(self) -> Optional[str]:
        """Return stored Ceph auth mode from ceph-client relation"""
        return self._ceph_data.get("auth")

    @property
    def key(self) -> Optional[str]:
        """Return stored Ceph key from ceph-client relation"""
        return self._ceph_data.get("key")

    @property
    def mon_hosts(self) -> List[str]:
        """Return stored Ceph monitor hosts from ceph-client relation"""
        return list(self._ceph_data.get("mon_hosts", []))

    def _install_ceph_common(self) -> bool:
        """Install ceph-common apt package"""
        self.unit.status = MaintenanceStatus("Installing Binaries")
        packages = ["ceph-common"]
        logger.info(f"Installing apt packages {', '.join(packages)}")
        try:
            # Run `apt-get update` and add packages
            apt.add_package(packages, update_cache=True)
        except apt.PackageNotFoundError:
            self._ops_blocked_by("Apt packages not found.", exc_info=True)
            return False
        except apt.PackageError:
            self._ops_blocked_by("Could not apt install packages.", exc_info=True)
            return False

        return True

    def write_ceph_cli_config(self) -> None:
        """Write Ceph CLI .conf file"""
        config = configparser.ConfigParser()
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
        config["client"] = {"log file": "/var/log/ceph.log"}

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
        cmd = ["/usr/bin/ceph", "--conf", str(ceph_config_file()), "--user", self.app.name, *args]
        return subprocess.check_output(cmd, timeout=timeout).decode("UTF-8")

    @lru_cache(maxsize=None)
    def get_ceph_fsid(self) -> str:
        """Get the Ceph FSID (cluster ID)"""
        try:
            return self.ceph_cli("fsid").strip()
        except subprocess.SubprocessError:
            logger.error("get_ceph_fsid: Failed to get CephFS ID, reporting as empty string")
            return ""

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
        client = Client(field_manager=f"{self.model.app.name}")
        return client

    @cached_property
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

    def _check_kube_config(self, event: EventBase) -> bool:
        self.unit.status = MaintenanceStatus("Evaluating kubernetes authentication")
        try:
            KubeConfig.from_env()
        except ConfigError:
            self._ops_wait_for(event, "Waiting for kubeconfig")
            return False
        return True

    def _check_namespace(self, event: EventBase, ns: str) -> bool:
        self.unit.status = MaintenanceStatus("Evaluating namespace")
        try:
            self._client.get(Namespace, name=ns)
        except ApiError as e:
            if "not found" in str(e.status.message):
                self._ops_blocked_by(f"Missing namespace '{ns}'", exc_info=True)
                event.defer()
                return False
            else:
                # surface any other errors besides not found
                logger.exception(e)
                self._ops_wait_for(event, "Waiting for Kubernetes API")
                return False
        return True

    def _check_cephfs(self) -> bool:
        self.unit.status = MaintenanceStatus("Evaluating CephFS capability")
        if not self.config["cephfs-enable"]:
            # not enabled, not a problem
            return True
        if not self.ceph_context.get("fsname", None):
            logger.error(
                "Ceph CLI failed to find a CephFS fsname. Run 'juju config cephfs-enable=False' until ceph-fs is usable."
            )
            self._ops_blocked_by("CephFS is not usable; set 'cephfs-enable=False'")
            return False
        return True

    def _merge_config(self, event: EventBase) -> None:
        if not self._check_required_relations():
            return

        if not self._check_kube_config(event):
            return

        if not self._check_namespace(event, str(self.stored.namespace)):
            return

        if not self._check_cephfs():
            return

        self.unit.status = MaintenanceStatus("Evaluating Manifests")
        new_hash = 0
        for manifest in self.collector.manifests.values():
            manifest = cast(SafeManifest, manifest)
            evaluation = manifest.evaluate()
            if evaluation:
                self.unit.status = BlockedStatus(evaluation)
                return
            new_hash += manifest.hash()

        self.stored.deployed = False
        if self._install_manifests(event, config_hash=new_hash):
            self.stored.config_hash = new_hash
            self.stored.deployed = True
        self._update_status(event)

    def _on_install_or_upgrade(self, event: EventBase) -> None:
        """Execute "on install" event callback."""
        no_error = self._install_ceph_common()
        no_error = no_error and self._check_required_relations()
        current_hash = no_error and self._install_manifests(event)
        self.stored.deployed = False
        if current_hash:
            self.stored.config_hash = current_hash
            self.stored.deployed = True

    def _install_manifests(self, event: EventBase, config_hash: int = 0) -> int:
        if cast(int, self.stored.config_hash) == config_hash:
            logger.info(f"No config changes detected. config_hash={config_hash}")
            return config_hash
        if self.unit.is_leader():
            self.unit.status = MaintenanceStatus("Deploying CephCSI")
            self.unit.set_workload_version("")
            for manifest in self.collector.manifests.values():
                try:
                    manifest.apply_manifests()
                except ManifestClientError as e:
                    self._ops_wait_for(event, " -> ".join(map(str, e.args)))
                    logger.warning(f"Encountered retryable installation error: {e}")
                    event.defer()
                    return 0

            disable_cephfs = not self.config["cephfs-enable"]
            if disable_cephfs and not self._purge_manifest_by_name(event, "cephfs"):
                # Failed to remove cephfs components when cephfs is disabled
                # _purge should defer
                return 0

        return config_hash

    def _check_required_relations(self) -> bool:
        """Run check if any required relations are missing"""
        self.unit.status = MaintenanceStatus("Checking Relations")
        required_relations = [self.CEPH_CLIENT_RELATION]
        missing_relations = [
            relation
            for relation in required_relations
            if self.model.get_relation(relation) is None
        ]

        if missing_relations:
            evaluation = "Missing relations: {}".format(", ".join(missing_relations))
            self.unit.status = BlockedStatus(evaluation)
            return False

        return self.safe_load_ceph_client_data()

    def safe_load_ceph_client_data(self) -> bool:
        """Load data from ceph-mon:client relation and store it in StoredState.

        This method expects all the required data (key, mon_hosts) to be present in the
        relation data. If any of the expected keys is missing, none of the data will be loaded.

        :param relation: Instance of ceph-mon:client relation.
        :param remote_unit: Unit instance representing remote ceph-mon unit.
        :return: `True` if all the data successfully loaded, otherwise `False`
        """
        relation_data = self.ceph_client.get_relation_data()
        expected_relation_keys = ("auth", "key", "mon_hosts")

        missing_data = [key for key in expected_relation_keys if key not in relation_data]
        if missing_data:
            logger.warning("Ceph relation is missing data: %s", missing_data)
            self.unit.status = WaitingStatus("Ceph relation is missing data.")
            success = False
        else:
            for relation_key in expected_relation_keys:
                self._ceph_data[relation_key] = relation_data.get(relation_key)
            success = True
            self.configure_ceph_cli()

        return success

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

    def _on_ceph_client_broker_available(self, event: EventBase) -> None:
        """Use ceph-mon:client relation to request creation of ceph-pools and
        ceph user permissions
        """
        self.request_ceph_pools()
        self.request_ceph_permissions()
        self._merge_config(event)

    @needs_leader
    def _purge_all_manifests(self, event: EventBase) -> None:
        """Purge resources created by this charm."""
        self.unit.status = MaintenanceStatus("Removing Kubernetes resources")
        for manifest in self.collector.manifests.values():
            if not self._purge_manifest(event, manifest):
                return
        ceph_pools = ", ".join(self.REQUIRED_CEPH_POOLS)
        logger.warning(
            "Ceph pools %s won't be removed. If you want to clean up pools manually, use juju "
            "action 'delete-pool' on 'ceph-mon' units",
            ceph_pools,
        )
        self.stored.deployed = False

    def _purge_manifest_by_name(self, event: EventBase, name: str) -> bool:
        """Purge resources created by this charm by manifest name."""
        self.unit.status = MaintenanceStatus(f"Removing {name} resources")
        for manifest in self.collector.manifests.values():
            if manifest.name == name:
                if not self._purge_manifest(event, manifest):
                    return False
        return True

    def _purge_manifest(self, event: EventBase, manifest: Manifests) -> bool:
        """Purge resources created by this charm by manifest."""
        try:
            manifest = cast(SafeManifest, manifest)
            manifest.purgeable = True
            manifest.delete_manifests(ignore_unauthorized=True, ignore_not_found=True)
            manifest.purgeable = False
        except ManifestClientError:
            self.unit.status = WaitingStatus("Waiting for kube-apiserver")
            event.defer()
            return False
        return True

    def _on_ceph_client_removed(self, event: EventBase) -> None:
        """Remove resources when relation removed"""
        self._purge_all_manifests(event)
        self._merge_config(event)

    def _cleanup(self, event: EventBase) -> None:
        """Remove resources when charm is stopped removed"""
        if cast(bool, self.stored.deployed):
            self.unit.status = MaintenanceStatus("Cleaning up...")
            if self._purge_all_manifests(event):
                self.unit.status = MaintenanceStatus("Shutting down")


if __name__ == "__main__":  # pragma: no cover
    main(CephCsiCharm)
