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
    RelationDepartedEvent,
    RelationJoinedEvent,
)
from ops.framework import StoredDict, StoredState
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus, WaitingStatus

logger = logging.getLogger(__name__)


def needs_leader(func: Callable) -> Callable:
    """Ensure that function with this decorator is executed only if on leader units."""

    @wraps(func)
    def leader_check(self: CharmBase, *args: Any, **kwargs: Any) -> Any:
        if self.unit.is_leader():
            func(self, *args, **kwargs)
        else:
            logger.info(
                "Execution of function '%s' skipped. This function can be executed only "
                "by the leader unit.",
                func.__name__,
            )

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
    def resources(self) -> List[Resource]:
        """Return list of k8s resources tied to the ceph-csi charm.

        Returned list contains instances that inherit from `Resource` class
        which provides convenience method `remove()` that removes resources
        from cluster or namespace.

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
        missing_relations = []
        if self.model.get_relation(self.CEPH_ADMIN_RELATION) is None:
            missing_relations.append("ceph-admin")
        if self.model.get_relation(self.CEPH_CLIENT_RELATION) is None:
            missing_relations.append("ceph-client")

        if missing_relations:
            self.unit.status = BlockedStatus(
                "Missing relations: {}".format(", ".join(missing_relations))
            )
        else:
            self.unit.status = ActiveStatus("Unit is ready")

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

        resources = [
            env.get_template(template).render(self._stored.ceph_data)
            for template in self.RESOURCE_TEMPLATES
        ]

        resource_dicts = [yaml.safe_load_all(res) for res in resources]
        return list(itertools.chain.from_iterable(resource_dicts))

    def render_storage_definitions(self) -> List[Dict]:
        """Render StorageClass definitions for supported filesystem types."""
        env = Environment(loader=FileSystemLoader(self.template_dir))
        default_storage = self._stored.default_storage_class
        storage_classes = []

        ext4_ctx = self.copy_stored_dict(self._stored.ceph_data)
        ext4_ctx["default"] = default_storage == self.EXT4_STORAGE
        ext4_ctx["pool_name"] = "ext4-pool"
        ext4_ctx["fs_type"] = "ext4"
        ext4_ctx["sc_name"] = "ceph-ext4"
        resource = env.get_template(self.STORAGE_CLASS_TEMPLATE).render(ext4_ctx)
        storage_classes.append(yaml.safe_load(resource))

        xfs_ctx = self.copy_stored_dict(self._stored.ceph_data)
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
    def update_storage_classes(self) -> None:
        """Re-render templates and update StorageClass resources in k8s cluster."""
        logger.info("Updating StorageClass definitions in Kubernetes.")
        for resource in self.resources:
            if isinstance(resource, StorageClass):
                resource.remove()
        storage_classes = self.render_storage_definitions()
        self.create_ceph_resources(storage_classes)

    def _on_ceph_admin_joined(self, event: RelationJoinedEvent) -> None:  # pragma: no cover
        """Create necessary k8s resources when relation is formed with ceph-mon."""
        if not self.unit.is_leader():
            # Skip resource creation on non-leader unit
            logger.info("Skipping Kubernetes resource creation from non-leader unit")
            self.check_required_relations()

        if self._stored.resources_created:
            # Skip silently if other ceph_relation_joined event already
            # created resources
            return

        unit_data = event.relation.data[event.unit]
        expected_data = (
            ("fsid", "fsid"),
            ("key", "kubernetes_key"),
            ("mon_hosts", "mon_hosts"),
        )  # mapping between relation data and template context keys.

        for relation_data_key, ceph_context_key in expected_data:
            self._stored.ceph_data[ceph_context_key] = unit_data.get(relation_data_key)

        missing_data = [key for key, value in self._stored.ceph_data.items() if value is None]
        if missing_data:
            logger.warning(
                "Ceph relation with %s is missing data: %s", event.unit.name, missing_data
            )
            self.unit.status = WaitingStatus("Ceph relation is missing data.")
            return

        all_resources = self.render_all_resource_definitions()
        self.create_ceph_resources(all_resources)
        self._stored.resources_created = True
        self.check_required_relations()

    def _on_ceph_client_joined(self, event: RelationJoinedEvent) -> None:
        """Use ceph-mon:client relation to request creation of ceph-pools."""
        if not self.unit.is_leader():
            # Don't request ceph pool creation from non-leader units
            logger.info("Skipping Ceph pool creation requests from non-leader unit")
            self.check_required_relations()
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
            logger.info(
                "Config changed. New value: 'default-storage' = '%s'",
                self.config.get("default-storage"),
            )
            self.update_storage_classes()


if __name__ == "__main__":  # pragma: no cover
    main(CephCsiCharm)
