#!/usr/bin/env python3
# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
#
# Learn more at: https://juju.is/docs/sdk

"""Charm the service.

Refer to the following post for a quick-start guide that will help you
develop a new k8s charm using the Operator Framework:

    https://discourse.charmhub.io/t/4208
"""

import itertools
import json
import logging
from functools import wraps
from resource import (
    ClusterRole,
    ClusterRoleBinding,
    ConfigMap,
    DaemonSet,
    Deployment,
    Resource,
    Role,
    RoleBinding,
    Secret,
    Service,
    ServiceAccount,
    StorageClass,
)
from typing import Any, Callable, Dict, List

import yaml
from charms.ceph_csi.v0.ceph_client import CephRequest, CreatePoolConfig
from jinja2 import Environment, FileSystemLoader
from kubernetes import client, config, utils
from kubernetes.client.exceptions import ApiException
from ops.charm import (
    CharmBase,
    ConfigChangedEvent,
    InstallEvent,
    RelationChangedEvent,
    RelationDepartedEvent,
    RelationJoinedEvent,
)
from ops.framework import StoredDict, StoredState
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus, Relation, Unit, WaitingStatus

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


class CephCsiCharm(CharmBase):
    """Charm the service."""

    CEPH_ADMIN_RELATION = "ceph-admin"
    CEPH_CLIENT_RELATION = "ceph-client"
    K8S_NS = "default"

    XFS_STORAGE = "ceph-xfs"
    EXT4_STORAGE = "ceph-ext4"
    REQUIRED_CEPH_POOLS = ["xfs-pool", "ext4-pool"]

    RESOURCE_TEMPLATES = [
        "ceph-csi-encryption-kms-config.yaml.j2",
        "ceph-secret.yaml.j2",
        "csi-config-map.yaml.j2",
        "csi-nodeplugin-rbac.yaml.j2",
        "csi-provisioner-rbac.yaml.j2",
        "csi-rbdplugin-provisioner.yaml.j2",
        "csi-rbdplugin.yaml.j2",
    ]
    STORAGE_CLASS_TEMPLATE = "ceph-storageclass.yaml.j2"

    _stored = StoredState()

    def __init__(self, *args: Any) -> None:
        """Setup even observers and initial storage values."""
        super().__init__(*args)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.ceph_admin_relation_joined, self._on_ceph_admin_joined)
        self.framework.observe(self.on.ceph_admin_relation_changed, self._on_ceph_admin_changed)
        self.framework.observe(self.on.ceph_admin_relation_broken, self.purge_k8s_resources)
        self.framework.observe(self.on.ceph_client_relation_joined, self._on_ceph_client_joined)
        self.framework.observe(self.on.ceph_client_relation_broken, self._on_ceph_client_removed)
        self._stored.set_default(ceph_data={})
        self._stored.set_default(resources_created=False)
        self._stored.set_default(default_storage_class=self.XFS_STORAGE)

        self.template_dir = self.charm_dir.joinpath("templates")

    @staticmethod
    def copy_stored_dict(stored_dict: StoredDict) -> dict:
        """Return copy of StoredDict as a basic dict.

        This is a convenience function as the StoredDict object does not implement
        copy() method.

        :param stored_dict: StoredDict instance to be copied
        :return: copy of stored_dict
        """
        return dict(stored_dict.items())

    @property
    def ceph_context(self) -> Dict[str, str]:
        """Return context that can be used to render ceph resource files in templates/ folder."""
        raw_mon_hosts = self._stored.ceph_data.get("mon_hosts", "").split()
        return {
            "fsid": self._stored.ceph_data.get("fsid"),
            "kubernetes_key": self._stored.ceph_data.get("key"),
            "mon_hosts": json.dumps(raw_mon_hosts),
        }

    @property
    def resources(self) -> List[Resource]:
        """Return list of k8s resources tied to the ceph-csi charm.

        Returned list contains instances that inherit from `Resource` class which provides
        convenience methods to work with Kubernetes resources.
        Each instance in the list maps to a specific resources in Kubernetes cluster.

        Example:
            for resource in self.resources:
                try:
                    resource.remove()
                except ApiException as exc:
                    if exc.status != 404: # Don't raise exception if it's is already gone
                        raise exc
        """
        config.load_kube_config()

        core_api = client.CoreV1Api()
        auth_api = client.RbacAuthorizationV1Api()
        storage_api = client.StorageV1Api()
        app_api = client.AppsV1Api()

        return [
            Secret(core_api, "csi-rbd-secret", self.K8S_NS),
            ServiceAccount(core_api, "rbd-csi-nodeplugin", self.K8S_NS),
            ServiceAccount(core_api, "rbd-csi-provisioner", self.K8S_NS),
            ClusterRole(auth_api, "rbd-csi-nodeplugin"),
            ClusterRole(auth_api, "rbd-csi-nodeplugin-rules"),
            ClusterRole(auth_api, "rbd-external-provisioner-runner-rules"),
            ClusterRole(auth_api, "rbd-external-provisioner-runner"),
            ClusterRoleBinding(auth_api, "rbd-csi-nodeplugin"),
            ClusterRoleBinding(auth_api, "rbd-csi-provisioner-role"),
            Role(auth_api, "rbd-external-provisioner-cfg", self.K8S_NS),
            RoleBinding(auth_api, "rbd-csi-provisioner-role-cfg", self.K8S_NS),
            StorageClass(storage_api, "ceph-ext4"),
            StorageClass(storage_api, "ceph-xfs"),
            Service(core_api, "csi-metrics-rbdplugin", self.K8S_NS),
            Service(core_api, "csi-rbdplugin-provisioner", self.K8S_NS),
            Deployment(app_api, "csi-rbdplugin-provisioner", self.K8S_NS),
            ConfigMap(core_api, "ceph-csi-config", self.K8S_NS),
            ConfigMap(core_api, "ceph-csi-encryption-kms-config", self.K8S_NS),
            DaemonSet(app_api, "csi-rbdplugin", self.K8S_NS),
        ]

    def update_stored_state(self, config_name: str, stored_state_name: str) -> bool:
        """Update value in stored stated based on the value from config.

        Since ConfigChangedEvent does not provide information about which config options
        were updated, this convenience method allows you to pass name of the config
        option and name of the corresponding variable in StoredState. If the values
        do not match, StoredState variable is updated with new value and `True` is
        returned.

        :param config_name: Name of the config option
        :param stored_state_name: Corresponding StoredState variable name
        :return: True if value change otherwise False
        """
        new_value = self.config.get(config_name)
        old_value = self._stored.__getattr__(stored_state_name)

        if old_value != new_value:
            logger.info("Updating stored state: %s=%s", stored_state_name, new_value)
            self._stored.__setattr__(stored_state_name, new_value)
            return True

        return False

    def _on_install(self, _: InstallEvent) -> None:
        """Execute "on install" event callback."""
        self.check_required_relations()

    def check_required_relations(self) -> None:
        """Run check if any required relations are missing"""
        required_relations = [self.CEPH_ADMIN_RELATION, self.CEPH_CLIENT_RELATION]
        missing_relations = [
            relation
            for relation in required_relations
            if self.model.get_relation(relation) is None
        ]

        if missing_relations:
            self.unit.status = BlockedStatus(
                "Missing relations: {}".format(", ".join(missing_relations))
            )
        else:
            self.unit.status = UNIT_READY_STATUS

    @needs_leader
    def create_ceph_resources(self, resources: List[Dict]) -> None:  # pylint: disable=R0201
        """Use kubernetes api to create resources like Pods, Secrets, etc.

        :param resources: list of dictionaries describing resources
        :return: None
        """
        config.load_kube_config()
        k8s_api = client.ApiClient()

        logger.info("Creating Kubernetes resources.")
        for resource in resources:
            logger.debug(
                "Creating resource %s (%s)", resource["metadata"]["name"], resource["kind"]
            )
            utils.create_from_dict(k8s_api, resource)

    def render_resource_definitions(self) -> List[Dict]:
        """Render resource definitions from templates in self.RESOURCE_TEMPLATES."""
        env = Environment(loader=FileSystemLoader(self.template_dir))
        context = self.ceph_context

        resources = [
            env.get_template(template).render(context) for template in self.RESOURCE_TEMPLATES
        ]

        resource_dicts = [yaml.safe_load_all(res) for res in resources]
        return list(itertools.chain.from_iterable(resource_dicts))

    def render_storage_definitions(self) -> List[Dict]:
        """Render StorageClass definitions for supported filesystem types."""
        env = Environment(loader=FileSystemLoader(self.template_dir))
        default_storage = self._stored.default_storage_class
        storage_classes = []

        ext4_ctx = self.ceph_context
        ext4_ctx["default"] = default_storage == self.EXT4_STORAGE
        ext4_ctx["pool_name"] = "ext4-pool"
        ext4_ctx["fs_type"] = "ext4"
        ext4_ctx["sc_name"] = "ceph-ext4"
        resource = env.get_template(self.STORAGE_CLASS_TEMPLATE).render(ext4_ctx)
        storage_classes.append(yaml.safe_load(resource))

        xfs_ctx = self.ceph_context
        xfs_ctx["default"] = default_storage == self.XFS_STORAGE
        xfs_ctx["pool_name"] = "xfs-pool"
        xfs_ctx["fs_type"] = "xfs"
        xfs_ctx["sc_name"] = "ceph-xfs"
        resource = env.get_template(self.STORAGE_CLASS_TEMPLATE).render(xfs_ctx)
        storage_classes.append(yaml.safe_load(resource))

        return storage_classes

    def render_all_resource_definitions(self) -> List[Dict]:
        """Render all resources required for ceph-csi."""
        return self.render_resource_definitions() + self.render_storage_definitions()

    @needs_leader
    def update_default_storage_class(self, default_class_name: str) -> None:
        """Set selected StorageClass as a default option."""
        available_classes = [sc for sc in self.resources if isinstance(sc, StorageClass)]

        # Find StorageClass that matches `default_class_name`
        try:
            default_class = [sc for sc in available_classes if sc.name == default_class_name][0]
        except IndexError as exc:
            error_msg = (
                "Unexpected config value for 'default-storage'. "
                "Valid values: {}. Got: {}".format(
                    [sc.name for sc in available_classes], default_class_name
                )
            )
            logger.error(error_msg)
            raise ValueError(error_msg) from exc

        logger.info("Setting '%s' StorageClass as default.", default_class_name)

        # according to documentation, proper way to set new default SC is to first unset current
        # default StorageClass and then set the new default.
        # https://kubernetes.io/docs/tasks/administer-cluster/change-default-storage-class/
        for storage_class in available_classes:
            if storage_class.name != default_class_name:
                storage_class.set_default(False)

        default_class.set_default(True)

    @needs_leader
    def safe_load_ceph_admin_data(self, relation: Relation, remote_unit: Unit) -> bool:
        """Load data from ceph-mon:admin relation and store it in StoredState.

        This method expects all the required data (fsid, key, mon_hosts) to be present in the
        relation data. If any of the expected keys is missing, none of the data will be loaded.

        :param relation: Instance of ceph-mon:admin relation.
        :param remote_unit: Unit instance representing remote ceph-mon unit.
        :return: `True` if all the data successfully loaded, otherwise `False`
        """
        unit_data = relation.data[remote_unit]
        expected_relation_keys = (
            "fsid",
            "key",
            "mon_hosts",
        )

        missing_data = [key for key in expected_relation_keys if key not in unit_data]
        if missing_data:
            logger.warning(
                "Ceph relation with %s is missing data: %s", remote_unit.name, missing_data
            )
            self.unit.status = WaitingStatus("Ceph relation is missing data.")
            success = False
        else:
            for relation_key in expected_relation_keys:
                self._stored.ceph_data[relation_key] = unit_data.get(relation_key)
            success = True

        return success

    def _on_ceph_admin_joined(self, event: RelationJoinedEvent) -> None:
        """Create necessary k8s resources when relation is formed with ceph-mon."""
        self.check_required_relations()
        if not self.unit.is_leader():
            # Skip resource creation on non-leader unit
            logger.info("Skipping Kubernetes resource creation from non-leader unit")
            return

        if self._stored.resources_created:
            # Skip silently if other ceph_relation_joined event already
            # created resources
            return

        if self.safe_load_ceph_admin_data(event.relation, event.unit):
            all_resources = self.render_all_resource_definitions()
            self.create_ceph_resources(all_resources)
            self._stored.resources_created = True

    def _on_ceph_admin_changed(self, event: RelationChangedEvent) -> None:
        self.check_required_relations()
        if not self.unit.is_leader():
            # Skip resource update on non-leader unit
            logger.info("Skipping Kubernetes resource update from non-leader unit")
            return

        if self.safe_load_ceph_admin_data(event.relation, event.unit):
            if self._stored.resources_created:
                # Update already existing resources that depend on ceph-admin data
                secret_key = self.ceph_context["kubernetes_key"]
                fsid = self.ceph_context["fsid"]
                mon_hosts = self._stored.ceph_data.get("mon_hosts", "").split()

                for resource in self.resources:
                    if isinstance(resource, Secret):
                        resource.update_opaque_data("userKey", secret_key)
                    elif isinstance(resource, StorageClass):
                        resource.update_cluster_id(fsid)
                    elif isinstance(resource, ConfigMap):
                        config_data = [{"clusterID": fsid, "monitors": mon_hosts}]
                        resource.update_config_json(json.dumps(config_data, indent=4))
            else:
                # No kubernetes resources exist yet. This is the first time that ceph-admin
                # relation has all the required data
                all_resources = self.render_all_resource_definitions()
                self.create_ceph_resources(all_resources)
                self._stored.resources_created = True

    def _on_ceph_client_joined(self, event: RelationJoinedEvent) -> None:
        """Use ceph-mon:client relation to request creation of ceph-pools."""
        self.check_required_relations()
        if not self.unit.is_leader():
            # Don't request ceph pool creation from non-leader units
            logger.info("Skipping Ceph pool creation requests from non-leader unit")
            return

        request = CephRequest(self.unit, event.relation)
        for pool_name in self.REQUIRED_CEPH_POOLS:
            pool = CreatePoolConfig(pool_name)
            request.add_replicated_pool(pool)

        request.execute()

    def _on_ceph_client_removed(self, _: RelationDepartedEvent) -> None:
        """Warn that ceph pools wont be automatically removed on ceph-mon:client departure.

        There does not seem to be a functionality to request ceph pool removal from ceph broker in
        a same way that there's a functionality to add one.
        """
        ceph_pools = ", ".join(self.REQUIRED_CEPH_POOLS)
        logger.warning(
            "Ceph pools %s wont be removed. If you want to clean up pools manually, use juju "
            "action 'delete-pool' on 'ceph-mon' units",
            ceph_pools,
        )

    @needs_leader
    def purge_k8s_resources(self, _: RelationDepartedEvent) -> None:
        """Purge k8s resources created by this charm."""
        logger.info("Removing Kubernetes resources.")
        for resource in self.resources:
            try:
                resource.remove()
            except ApiException as exc:
                if exc.status != 404:
                    raise exc
                logger.debug("Resource %s is already removed.", resource.name)

        self._stored.resources_created = False
        self.unit.status = BlockedStatus("Missing relations: ceph")

    def _on_config_changed(self, _: ConfigChangedEvent) -> None:
        """Handle configuration Change."""
        if self.update_stored_state("default-storage", "default_storage_class"):
            default_class = self.config.get("default-storage")
            logger.info("Config changed. New value: 'default-storage' = '%s'", default_class)
            try:
                self.update_default_storage_class(default_class)
            except ValueError:
                self.unit.status = BlockedStatus(
                    "{} 'default-storage'. See logs for more info.".format(BAD_CONFIG_PREFIX)
                )
                return

        # Resolve blocked state caused by bad configuration
        status = self.unit.status
        if status.name == BlockedStatus.name and status.message.startswith(BAD_CONFIG_PREFIX):
            self.unit.status = UNIT_READY_STATUS


if __name__ == "__main__":  # pragma: no cover
    main(CephCsiCharm)
