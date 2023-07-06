#!/usr/bin/env python3
# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
"""Dispatch logic for the ceph-csi storage charm."""

import configparser
import json
import logging
import pickle
import subprocess
from functools import wraps
from hashlib import md5
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, cast

import charms.operator_libs_linux.v0.apt as apt
from interface_ceph_client import ceph_client  # type: ignore
from ops.charm import ActionEvent, CharmBase, EventBase
from ops.framework import StoredState
from ops.main import main
from ops.manifests import Collector, ManifestClientError, Manifests
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus, WaitingStatus

from cephfs_manifests import CephFSManifests
from config_manifest import ConfigManifests
from rbd_manifests import RBDManifests

logger = logging.getLogger(__name__)


UNIT_READY_STATUS = ActiveStatus("Unit is ready")
BAD_CONFIG_PREFIX = "Bad configuration option for"


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


class SafeManifest(Manifests):
    @property
    def config(self) -> Dict:
        return {}

    def hash(self) -> int:
        """Calculate a hash of the current configuration."""
        return int(md5(pickle.dumps(self.config)).hexdigest(), 16)

    def evaluate(self) -> Optional[str]:
        ...


class CephCsiCharm(CharmBase):
    """Charm the service."""

    CEPH_CLIENT_RELATION = "ceph-client"
    K8S_NS = "default"

    DEFAULT_STORAGE = "ceph-xfs"
    REQUIRED_CEPH_POOLS = ["xfs-pool", "ext4-pool"]

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
        self.framework.observe(self.on.update_status, self._update_status)

        self.framework.observe(self.on.install, self._on_install_or_upgrade)
        self.framework.observe(self.on.upgrade_charm, self._on_install_or_upgrade)
        self.framework.observe(self.on.config_changed, self._merge_config)
        self.framework.observe(self.on.stop, self._cleanup)

        self.stored.set_default(ceph_data={})
        self.stored.set_default(config_hash=0)  # hashed value of the provider config once valid
        self.stored.set_default(deployed=False)  # True if config has been applied after new hash

        self.collector = Collector(
            ConfigManifests(self),
            CephFSManifests(self),
            RBDManifests(self),
        )

    def _ops_wait_for(self, event: EventBase, msg: str) -> str:
        self.unit.status = WaitingStatus(msg)
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
        except ManifestClientError:
            msg = "Failed to apply missing resources. API Server unavailable."
            event.set_results({"result": msg})
        else:
            self.stored.deployed = True

    def _update_status(self, _: EventBase) -> None:
        if not cast(bool, self.stored.deployed):
            return

        unready = self.collector.unready
        if unready:
            self.unit.status = WaitingStatus(", ".join(unready))
        else:
            self.unit.status = ActiveStatus("Ready")
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
            self._ops_blocked_by("Could not apt install packages", exc_info=True)
            return False

        return True

    def write_ceph_cli_config(self) -> None:
        """Write Ceph CLI .conf file"""
        config = configparser.ConfigParser()
        config["global"] = {
            "auth_cluster_required": self.auth or "",
            "auth_service_required": self.auth or "",
            "auth_client_required": self.auth or "",
            "keyring": "/etc/ceph/$cluster.$name.keyring",
            "mon host": " ".join(self.mon_hosts),
            "log to syslog": "true",
            "err to syslog": "true",
            "clog to syslog": "true",
            "mon cluster log to syslog": "true",
            "debug mon": "1/5",
            "debug osd": "1/5",
        }
        config["client"] = {"log file": "/var/log/ceph.log"}

        with Path("/etc/ceph/ceph.conf").open("w") as fp:
            config.write(fp)

    def write_ceph_cli_keyring(self) -> None:
        """Write Ceph CLI keyring file"""
        ceph_keyring_path = "/etc/ceph/ceph.client.{}.keyring".format(self.app.name)
        ceph_keyring = f"[client.{self.app.name}]\n\tkey = {self.key}\n"
        with open(ceph_keyring_path, "w") as f:
            f.write(ceph_keyring)

    def configure_ceph_cli(self) -> None:
        """Configure Ceph CLI"""
        Path("/etc/ceph").mkdir(parents=True, exist_ok=True)
        self.write_ceph_cli_config()
        self.write_ceph_cli_keyring()

    def ceph_cli(self, *args: str, timeout: int = 60) -> str:
        """Run Ceph CLI command"""
        cmd = ["ceph", "--user", self.app.name] + list(args)
        return subprocess.check_output(cmd, timeout=timeout).decode("UTF-8")

    def get_ceph_fsid(self) -> str:
        """Get the Ceph FSID (cluster ID)"""
        fsid = self.ceph_cli("fsid").strip()
        return fsid

    def get_cephfs_fsname(self) -> str | None:
        """Get the Ceph FS Name."""
        data = json.loads(self.ceph_cli("fs", "ls", "-f", "json"))
        for fs in data:
            if "ceph-fs_data" in fs["data_pools"]:
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
    def ceph_context(self) -> Dict[str, Any]:
        """Return context that can be used to render ceph resource files in templates/ folder."""
        return {
            "auth": self.auth,
            "fsid": self.get_ceph_fsid(),
            "kubernetes_key": self.key,
            "mon_hosts": json.dumps(self.mon_hosts),
            "user": self.app.name,
            "provisioner_replicas": self.provisioner_replicas,
            "enable_host_network": json.dumps(self.enable_host_network),
            "fsname": self.get_cephfs_fsname(),
        }

    def _merge_config(self, event: EventBase) -> None:
        if not self._check_required_relations():
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
        if self._install_or_upgrade(event, config_hash=new_hash):
            self.stored.config_hash = new_hash
            self.stored.deployed = True

    def _on_install_or_upgrade(self, event: EventBase) -> None:
        self._install_or_upgrade(event)

    def _install_or_upgrade(self, event: EventBase, config_hash: Optional[int] = None) -> bool:
        """Execute "on install" event callback."""
        error = self._install_ceph_common()
        error = error or self._check_required_relations()
        self.stored.installed = not error

        error or self._install_manifests(event, config_hash=config_hash)
        return bool(error)

    def _check_required_relations(self) -> bool:
        """Run check if any required relations are missing"""
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

    @needs_leader
    def _install_manifests(self, event: EventBase, config_hash: Optional[int] = None) -> bool:
        if cast(int, self.stored.config_hash) == config_hash:
            logger.info("Skipping until the config is evaluated.")
            return True
        if self.unit.is_leader():
            self.unit.status = MaintenanceStatus("Deploying CephCSI")
            self.unit.set_workload_version("")
            for controller in self.collector.manifests.values():
                try:
                    controller.apply_manifests()
                except ManifestClientError as e:
                    self._ops_wait_for(event, "Waiting for kube-apiserver")
                    logger.warn(f"Encountered retryable installation error: {e}")
                    event.defer()
                    return False
        return True

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

    def _on_ceph_client_broker_available(self, _: EventBase) -> None:
        """Use ceph-mon:client relation to request creation of ceph-pools and
        ceph user permissions
        """
        self.request_ceph_pools()
        self.request_ceph_permissions()

    @needs_leader
    def purge_k8s_resources(self, event: EventBase) -> None:
        """Purge k8s resources created by this charm."""
        logger.info("Removing Kubernetes resources.")
        for controller in self.collector.manifests.values():
            try:
                controller.delete_manifests(ignore_unauthorized=True)
            except ManifestClientError:
                self.unit.status = WaitingStatus("Waiting for kube-apiserver")
                event.defer()
                return
        ceph_pools = ", ".join(self.REQUIRED_CEPH_POOLS)
        logger.warning(
            "Ceph pools %s wont be removed. If you want to clean up pools manually, use juju "
            "action 'delete-pool' on 'ceph-mon' units",
            ceph_pools,
        )
        self.stored.deployed = False

    def _on_ceph_client_removed(self, event: EventBase) -> None:
        """Remove resources when relation removed"""
        self.purge_k8s_resources(event)
        self._merge_config(event)

    def _cleanup(self, event: EventBase) -> None:
        """Remove resources when charm is stopped removed"""
        if cast(int, self.stored.config_hash):
            self.unit.status = MaintenanceStatus("Cleaning up...")
            if self.purge_k8s_resources(event):
                self.unit.status = MaintenanceStatus("Shutting down")


if __name__ == "__main__":  # pragma: no cover
    main(CephCsiCharm)
