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

from functools import wraps
import itertools
import logging
from typing import List, Dict, Callable

from jinja2 import Environment, FileSystemLoader
from kubernetes import client, config, utils
from kubernetes.client.exceptions import ApiException
from ops.charm import CharmBase, RelationJoinedEvent
from ops.framework import StoredState, StoredDict
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus
import yaml

from resource import (
    ClusterRole,
    ClusterRoleBinding,
    ConfigMap,
    DaemonSet,
    Deployment,
    Secret,
    Service,
    ServiceAccount,
    StorageClass,
    Resource,
    Role,
    RoleBinding,
)

logger = logging.getLogger(__name__)


def needs_leader(func: Callable) -> Callable:
    @wraps(func)
    def leader_check(self: CharmBase, *args, **kwargs):
        if self.unit.is_leader():
            func(self, *args, **kwargs)

    return leader_check


class CephCsiCharm(CharmBase):
    """Charm the service."""

    CEPH_RELATION = 'ceph'
    K8S_NS = 'default'

    XFS_STORAGE = 'ceph-xfs'
    EXT4_STORAGE = 'ceph-ext4'

    RESOURCE_TEMPLATES = [
        'ceph-csi-encryption-kms-config.yaml.j2',
        'ceph-secret.yaml.j2',
        'csi-config-map.yaml.j2',
        'csi-nodeplugin-rbac.yaml.j2',
        'csi-provisioner-rbac.yaml.j2',
        'csi-rbdplugin-provisioner.yaml.j2',
        'csi-rbdplugin.yaml.j2',
    ]
    STORAGE_CLASS_TEMPLATE = 'ceph-storageclass.yaml.j2'

    _stored = StoredState()

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.ceph_relation_joined, self._on_ceph_joined)
        self.framework.observe(self.on.ceph_relation_broken, self.purge_k8s_resources)
        self._stored.set_default(ceph_data=dict())
        self._stored.set_default(resources_created=False)
        self._stored.set_default(default_storage_class=self.XFS_STORAGE)

        self.template_dir = self.charm_dir.joinpath('templates')

    @staticmethod
    def copy_stored_dict(stored_dict: StoredDict) -> dict:
        return {key: value for key, value in stored_dict.items()}

    @property
    def resources(self) -> List[Resource]:
        config.load_kube_config()

        core_api = client.CoreV1Api()
        auth_api = client.RbacAuthorizationV1Api()
        storage_api = client.StorageV1Api()
        app_api = client.AppsV1Api()

        return [
            Secret(core_api, 'csi-rbd-secret', self.K8S_NS),
            ServiceAccount(core_api, 'rbd-csi-nodeplugin', self.K8S_NS),
            ServiceAccount(core_api, 'rbd-csi-provisioner', self.K8S_NS),
            ClusterRole(auth_api, 'rbd-csi-nodeplugin'),
            ClusterRole(auth_api, 'rbd-csi-nodeplugin-rules'),
            ClusterRole(auth_api, 'rbd-external-provisioner-runner-rules'),
            ClusterRole(auth_api, 'rbd-external-provisioner-runner'),
            ClusterRoleBinding(auth_api, 'rbd-csi-nodeplugin'),
            ClusterRoleBinding(auth_api, 'rbd-csi-provisioner-role'),
            Role(auth_api, 'rbd-external-provisioner-cfg', self.K8S_NS),
            RoleBinding(auth_api, 'rbd-csi-provisioner-role-cfg', self.K8S_NS),
            StorageClass(storage_api, 'ceph-ext4'),
            StorageClass(storage_api, 'ceph-xfs'),
            Service(core_api, 'csi-metrics-rbdplugin', self.K8S_NS),
            Service(core_api, 'csi-rbdplugin-provisioner', self.K8S_NS),
            Deployment(app_api, 'csi-rbdplugin-provisioner', self.K8S_NS),
            ConfigMap(core_api, 'ceph-csi-config', self.K8S_NS),
            ConfigMap(core_api, 'ceph-csi-encryption-kms-config', self.K8S_NS),
            DaemonSet(app_api, 'csi-rbdplugin', self.K8S_NS),
        ]

    def update_stored_state(self, config_name, stored_state_name) -> bool:
        new_value = self.config.get(config_name)
        old_value = self._stored.__getattr__(stored_state_name)

        if old_value != new_value:
            self._stored.__setattr__(stored_state_name, new_value)
            return True

        return False

    def _on_install(self, _):
        self.check_required_relations()

    def check_required_relations(self):
        if self.model.get_relation(self.CEPH_RELATION) is None:
            self.unit.status = BlockedStatus("Missing relations: ceph")
        else:
            self.unit.status = ActiveStatus()

    @needs_leader
    def create_ceph_resources(self, resources: List[Dict]):
        config.load_kube_config()
        k8s_api = client.ApiClient()

        for resource in resources:
            utils.create_from_dict(k8s_api, resource)

    def render_resource_definitions(self) -> List[Dict]:
        env = Environment(loader=FileSystemLoader(self.template_dir))

        resources = [env.get_template(template).render(self._stored.ceph_data)
                     for template in self.RESOURCE_TEMPLATES]

        resource_dicts = [yaml.safe_load_all(res) for res in resources]
        return list(itertools.chain.from_iterable(resource_dicts))

    def render_storage_definitions(self) -> List[Dict]:
        env = Environment(loader=FileSystemLoader(self.template_dir))
        default_storage = self._stored.default_storage_class
        storage_classes = []

        ext4_ctx = self.copy_stored_dict(self._stored.ceph_data)
        ext4_ctx['default'] = default_storage == self.EXT4_STORAGE
        ext4_ctx['pool_name'] = 'ext4-pool'
        ext4_ctx['fs_type'] = 'ext4'
        ext4_ctx['sc_name'] = 'ceph-ext4'
        resource = env.get_template(self.STORAGE_CLASS_TEMPLATE).render(ext4_ctx)
        storage_classes.append(yaml.safe_load(resource))

        xfs_ctx = self.copy_stored_dict(self._stored.ceph_data)
        xfs_ctx['default'] = default_storage == self.XFS_STORAGE
        xfs_ctx['pool_name'] = 'xfs-pool'
        xfs_ctx['fs_type'] = 'xfs'
        xfs_ctx['sc_name'] = 'ceph-xfs'
        resource = env.get_template(self.STORAGE_CLASS_TEMPLATE).render(xfs_ctx)
        storage_classes.append(yaml.safe_load(resource))

        return storage_classes

    def render_all_resource_definitions(self) -> List[Dict]:
        return self.render_resource_definitions() + self.render_storage_definitions()

    @needs_leader
    def update_storage_classes(self):
        for resource in self.resources:
            if isinstance(resource, StorageClass):
                resource.remove()
        storage_classes = self.render_storage_definitions()
        self.create_ceph_resources(storage_classes)

    @needs_leader
    def _on_ceph_joined(self, event: RelationJoinedEvent):
        if self._stored.resources_created:
            # Skip silently if other ceph_relation_joined event already
            # created resources
            return

        unit_data = event.relation.data[event.unit]
        expected_data = ('fsid', 'key', 'mon_hosts')
        for key in expected_data:
            self._stored.ceph_data[key] = unit_data.get(key)
        missing_data = [key for key, value in self._stored.ceph_data.items() if value is None]
        if missing_data:
            logger.warning("Ceph relation with %s is missing data: %s", event.unit.name, missing_data)
            self.unit.status = BlockedStatus('Ceph relation is missing data.')
            return

        all_resources = self.render_all_resource_definitions()
        self.create_ceph_resources(all_resources)
        self._stored.resources_created = True
        self.check_required_relations()

    @needs_leader
    def purge_k8s_resources(self, _):
        for resource in self.resources:
            try:
                logger.debug('Removing resource %s (namespace=%s)',
                             resource.name, (resource.namespace or None))
                resource.remove()
            except ApiException as exc:
                if exc.status == 404:
                    logger.info('Resource %s is already removed.',
                                resource.name)
                else:
                    raise exc

        self._stored.resources_created = False
        self.unit.status = BlockedStatus("Missing relations: ceph")

    def _on_config_changed(self, _):
        if self.update_stored_state('default-storage', 'default_storage_class'):
            self.update_storage_classes()


if __name__ == "__main__":
    main(CephCsiCharm)
