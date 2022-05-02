# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing
"""Collection of tests related to src/charm.py"""

import json
import os
import subprocess
import unittest
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
from unittest.mock import MagicMock, PropertyMock, call, patch

from interface_ceph_client.ceph_client import CephClientRequires
from kubernetes.client import ApiException
from ops.testing import Harness

from charm import (
    BAD_CONFIG_PREFIX,
    UNIT_READY_STATUS,
    ActiveStatus,
    BlockedStatus,
    CephCsiCharm,
    client,
    config,
    logger,
    utils,
)


class TestCharm(unittest.TestCase):
    """Tests for charm.CephCsiCharm class."""

    def setUp(self):
        self.harness = Harness(CephCsiCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()

        # mock kubernetes lib
        self.kubernetes_config = self.patch(config, "load_kube_config")
        self.kubernetes_client = MagicMock()
        self.patch(client, "ApiClient").return_value = self.kubernetes_client

    def patch(self, obj, method) -> MagicMock:
        """Method mock scoped to the duration of single unit test."""
        _patch = patch.object(obj, method)
        mock_method = _patch.start()
        self.addCleanup(_patch.stop)
        return mock_method

    def patch_property(self, obj, method) -> PropertyMock:
        """Method property scoped to the duration of single unit test."""
        _patch = patch.object(obj, method, new_callable=PropertyMock)
        mock_method = _patch.start()
        self.addCleanup(_patch.stop)
        return mock_method

    def test_stored_dict_copy(self):
        """Test helper method that returns StoredDict as dictionary copy."""
        self.harness.charm._stored.ceph_data["foo"] = "bar"
        expected_dict = {"foo": "bar"}

        dict_copy = self.harness.charm.copy_stored_dict(self.harness.charm._stored.ceph_data)
        self.assertEqual(dict_copy, expected_dict)

    def test_write_ceph_cli_config(self):
        """Test writing of Ceph CLI config"""
        self.harness.charm._stored.ceph_data["auth"] = "cephx"
        self.harness.charm._stored.ceph_data["mon_hosts"] = ["10.0.0.1", "10.0.0.2"]
        with patch("builtins.open"):
            self.harness.charm.write_ceph_cli_config()
            with open("/etc/ceph/ceph.conf", "w") as f:
                f.write.assert_called_once_with(
                    """[global]
auth cluster required = cephx
auth service required = cephx
auth client required = cephx
keyring = /etc/ceph/$cluster.$name.keyring
mon host = 10.0.0.1 10.0.0.2

log to syslog = true
err to syslog = true
clog to syslog = true
mon cluster log to syslog = true
debug mon = 1/5
debug osd = 1/5

[client]
log file = /var/log/ceph.log
"""
                )

    def test_write_ceph_cli_keyring(self):
        """Test writing of Ceph CLI keyring file"""
        self.harness.charm._stored.ceph_data["key"] = "12345"
        with patch("builtins.open"):
            self.harness.charm.write_ceph_cli_keyring()
            with open("/etc/ceph/ceph.client.ceph-csi.keyring", "w") as f:
                f.write.assert_called_once_with("[client.ceph-csi]\n\tkey = 12345\n")

    def test_configure_ceph_cli(self):
        """Test configuration of Ceph CLI"""
        self.patch(os, "makedirs")
        self.patch(CephCsiCharm, "write_ceph_cli_config")
        self.patch(CephCsiCharm, "write_ceph_cli_keyring")
        self.harness.charm.configure_ceph_cli()
        os.makedirs.assert_called_once_with("/etc/ceph", exist_ok=True)
        self.harness.charm.write_ceph_cli_config.assert_called_once()
        self.harness.charm.write_ceph_cli_keyring.assert_called_once()

    def test_ceph_context_getter(self):
        """Test that ceph_context property returns properly formatted data."""
        fsid = "12345"
        key = "secret_key"
        monitors = ["10.0.0.1", "10.0.0.2"]
        expected_monitors_format = json.dumps(monitors)

        # stored data from ceph-mon:client relation
        self.harness.charm._stored.ceph_data["key"] = key
        self.harness.charm._stored.ceph_data["mon_hosts"] = monitors

        self.patch(subprocess, "check_output").return_value = fsid.encode("UTF-8")

        # key and value format expected in context for Kubernetes templates.
        expected_context = {
            "fsid": fsid,
            "kubernetes_key": key,
            "mon_hosts": expected_monitors_format,
            "user": "ceph-csi",
        }

        self.assertEqual(self.harness.charm.ceph_context, expected_context)
        subprocess.check_output.assert_called_once_with(
            ["ceph", "--user", "ceph-csi", "fsid"], timeout=60
        )

    def test_k8s_resources_getter(self):
        """Test that property 'resources' returns expected list of resources"""
        api_mock = MagicMock()
        namespace = self.harness.charm.K8S_NS
        expected_resources = [
            Secret(api_mock, "csi-rbd-secret", namespace),
            ServiceAccount(api_mock, "rbd-csi-nodeplugin", namespace),
            ServiceAccount(api_mock, "rbd-csi-provisioner", namespace),
            ClusterRole(api_mock, "rbd-csi-nodeplugin"),
            ClusterRole(api_mock, "rbd-csi-nodeplugin-rules"),
            ClusterRole(api_mock, "rbd-external-provisioner-runner-rules"),
            ClusterRole(api_mock, "rbd-external-provisioner-runner"),
            ClusterRoleBinding(api_mock, "rbd-csi-nodeplugin"),
            ClusterRoleBinding(api_mock, "rbd-csi-provisioner-role"),
            Role(api_mock, "rbd-external-provisioner-cfg", namespace),
            RoleBinding(api_mock, "rbd-csi-provisioner-role-cfg", namespace),
            StorageClass(api_mock, "ceph-ext4"),
            StorageClass(api_mock, "ceph-xfs"),
            Service(api_mock, "csi-metrics-rbdplugin", namespace),
            Service(api_mock, "csi-rbdplugin-provisioner", namespace),
            Deployment(api_mock, "csi-rbdplugin-provisioner", namespace),
            ConfigMap(api_mock, "ceph-csi-config", namespace),
            ConfigMap(api_mock, "ceph-csi-encryption-kms-config", namespace),
            DaemonSet(api_mock, "csi-rbdplugin", namespace),
        ]

        self.assertEqual(self.harness.charm.resources, expected_resources)

    def test_update_stored_state(self):
        """Test method that updates stored state with values from config."""
        config_name = "default-storage"
        stored_state_name = "default_storage_class"
        new_value = "foo"

        # Charms _on_config_changed handler must be mocked otherwise it would trigger
        # update_stored_state() method on its own
        self.patch(CephCsiCharm, "_on_config_changed")

        self.harness.update_config({config_name: new_value})
        updated = self.harness.charm.update_stored_state(config_name, stored_state_name)

        # method should return True if StoredState was updated
        self.assertTrue(updated)
        self.assertEqual(self.harness.charm._stored.default_storage_class, new_value)

        # second call without value change should return False
        updated = self.harness.charm.update_stored_state(config_name, stored_state_name)
        self.assertFalse(updated)

    def test_install(self):
        """Test that on.install hook will call expected methods"""
        self.patch(subprocess, "check_call")
        mock_relation_check = self.patch(CephCsiCharm, "check_required_relations")
        self.harness.charm.on.install.emit()

        mock_relation_check.assert_called_once_with()
        self.assertEqual(subprocess.check_call.call_count, 2)
        subprocess.check_call.assert_any_call(["apt-get", "update"])
        subprocess.check_call.assert_any_call(["apt-get", "install", "-y", "ceph-common"])

    def test_required_relation_check(self):
        """Test that check_required_relations sets expected unit states."""
        self.patch_property(CephCsiCharm, "model")
        get_relation_mock = self.patch(CephCsiCharm.model, "get_relation")

        # Return object on any call, indicating existing relations
        get_relation_mock.return_value = object()
        self.harness.charm.check_required_relations()

        self.assertEqual(self.harness.charm.unit.status.name, "active")
        self.assertEqual(self.harness.charm.unit.status.message, "Unit is ready")

        # Return None on any call, indicating that all required relations are  missing
        get_relation_mock.return_value = None
        self.harness.charm.check_required_relations()

        self.assertEqual(self.harness.charm.unit.status.name, "blocked")
        self.assertEqual(self.harness.charm.unit.status.message, "Missing relations: ceph-client")

    def test_create_ceph_resources(self):
        """Test that create_ceph_resources() calls kubernetes api."""
        resources = [
            {"kind": "Foo", "metadata": {"name": "resource1"}},
            {"kind": "Bar", "metadata": {"name": "resource2"}},
        ]
        expected_calls = [call(self.kubernetes_client, res) for res in resources]
        k8s_api_call_mock = self.patch(utils, "create_from_dict")
        self.harness.set_leader(True)

        self.harness.charm.create_ceph_resources(resources)

        k8s_api_call_mock.assert_has_calls(expected_calls, True)

        # Reset mock and test that method is not executed on non-leader units
        k8s_api_call_mock.reset_mock()
        self.harness.set_leader(False)

        self.harness.charm.create_ceph_resources(resources)
        k8s_api_call_mock.assert_not_called()

    def test_render_resources(self):
        """Test that yaml templates get properly serialized into dicts."""
        self.patch(CephCsiCharm, "get_ceph_fsid").return_value = "12345"
        resources = self.harness.charm.render_resource_definitions()
        # Assert that all resources get properly unpacked to dictionaries
        self.assertTrue(all(isinstance(resource, dict) for resource in resources))

    def test_render_storage_class_definitions(self):
        """Test that storage class definitions are properly rendered."""

        def assert_default_storage_class(storage_class, storage_definitions):
            """Assert that `storage_class` is set as default in `storage_definitions.`"""
            default_attribute = "storageclass.kubernetes.io/is-default-class"
            for definition in storage_definitions:
                annotation = definition.get("metadata", {}).get("annotations", {})
                if definition["metadata"]["name"] == storage_class:
                    self.assertTrue(annotation[default_attribute])
                else:
                    self.assertNotIn(default_attribute, annotation)

        self.patch(CephCsiCharm, "get_ceph_fsid").return_value = "12345"
        self.patch(CephCsiCharm, "update_default_storage_class")
        xfs = self.harness.charm.XFS_STORAGE
        ext4 = self.harness.charm.EXT4_STORAGE

        self.harness.update_config({"default-storage": xfs})
        storage_definitions = self.harness.charm.render_storage_definitions()

        self.assertEqual(len(storage_definitions), 2)
        assert_default_storage_class(xfs, storage_definitions)

        self.harness.update_config({"default-storage": ext4})
        storage_definitions = self.harness.charm.render_storage_definitions()

        self.assertEqual(len(storage_definitions), 2)
        assert_default_storage_class(ext4, storage_definitions)

    def test_render_all_resources(self):
        """Test that `render_all_resource_definitions` renders all expected k8s resources."""
        resource_definitions = self.patch(CephCsiCharm, "render_resource_definitions")
        storage_definitions = self.patch(CephCsiCharm, "render_storage_definitions")

        self.harness.charm.render_all_resource_definitions()

        resource_definitions.assert_called_once_with()
        storage_definitions.assert_called_once_with()

    def test_update_default_storage_class(self):
        """Test that update_default_storage_class() patches StorageClass resources."""
        ext4_class_update_mock = MagicMock()
        xfs_class_update_mock = MagicMock()
        ext4_storage = self.harness.charm.EXT4_STORAGE
        xfs_storage = self.harness.charm.XFS_STORAGE

        # Setup mock for all expected StorageClasses
        all_resources = self.harness.charm.resources
        for resource in all_resources:
            if isinstance(resource, StorageClass):
                if resource.name == xfs_storage:
                    xfs_class_update_mock = self.patch(resource, "set_default")
                elif resource.name == ext4_storage:
                    ext4_class_update_mock = self.patch(resource, "set_default")
                else:
                    self.fail("Storage class '{}' not covered by unit tests".format(resource.name))

        self.patch_property(CephCsiCharm, "resources").return_value = all_resources

        # Expect no action on non-leader unit
        self.harness.charm.update_default_storage_class(ext4_storage)
        self.harness.charm.update_default_storage_class(xfs_storage)
        self.harness.charm.update_default_storage_class("Foo")

        ext4_class_update_mock.assert_not_called()
        xfs_class_update_mock.assert_not_called()

        # Set unit as leader and execute actual updates
        self.harness.set_leader(True)

        # Set ext4 as default storage class
        self.harness.charm.update_default_storage_class(ext4_storage)
        ext4_class_update_mock.assert_called_once_with(True)
        xfs_class_update_mock.assert_called_once_with(False)

        # reset mocks
        ext4_class_update_mock.reset_mock()
        xfs_class_update_mock.reset_mock()

        # Set xfs as default storage class
        self.harness.charm.update_default_storage_class(xfs_storage)
        ext4_class_update_mock.assert_called_once_with(False)
        xfs_class_update_mock.assert_called_once_with(True)

        # Set unknown default storage class
        with self.assertRaises(ValueError):
            self.harness.charm.update_default_storage_class("Foo")

    def test_update_config(self):
        """Test handling of charm config updates."""
        update_default_storage_mock = self.patch(CephCsiCharm, "update_default_storage_class")
        update_stored_state_mock = self.patch(CephCsiCharm, "update_stored_state")

        update_stored_state_mock.return_value = True

        # Update default storage class
        ext4_storage = self.harness.charm.EXT4_STORAGE
        xfs_storage = self.harness.charm.XFS_STORAGE
        bad_storage = "foo"

        self.harness.update_config({"default-storage": ext4_storage})

        update_default_storage_mock.assert_called_once_with(ext4_storage)

        # reset mocks
        update_default_storage_mock.reset_mock()

        # Update default storage class with bad value and assert that it sets Blocked state
        update_default_storage_mock.side_effect = ValueError
        self.harness.update_config({"default-storage": bad_storage})

        self.assertEqual(self.harness.charm.unit.status.name, BlockedStatus.name)
        self.assertTrue(self.harness.charm.unit.status.message.startswith(BAD_CONFIG_PREFIX))

        # reset mocks
        update_default_storage_mock.reset_mock()
        update_default_storage_mock.side_effect = None

        # Assert that setting valid value for default storage class sets unit status back to Active
        self.harness.update_config({"default-storage": xfs_storage})

        update_default_storage_mock.assert_called_once_with(xfs_storage)
        self.assertEqual(self.harness.charm.unit.status.name, ActiveStatus.name)
        self.assertEqual(self.harness.charm.unit.status.message, UNIT_READY_STATUS.message)

    def test_resource_removal(self):
        """Test removal of the k8s resources that happens when ceph-mon relation is removed."""
        k8s_api_mock = MagicMock()
        all_resources = [
            Secret(k8s_api_mock, "mock_secret"),
            StorageClass(k8s_api_mock, "mock_storage"),
            Deployment(k8s_api_mock, "mock_deployment"),
        ]

        remove_call_mocks = [self.patch(resource, "remove") for resource in all_resources]
        self.patch_property(CephCsiCharm, "resources").return_value = all_resources
        self.harness.set_leader(True)

        self.harness.charm.purge_k8s_resources(MagicMock())

        for remove_call in remove_call_mocks:
            remove_call.assert_called_once_with()

        self.assertEqual(self.harness.charm.unit.status.name, "blocked")
        self.assertEqual(self.harness.charm.unit.status.message, "Missing relations: ceph")
        self.assertFalse(self.harness.charm._stored.resources_created)

    def test_resource_removal_missing_resources(self):
        """Test that attempt to remove already removed resources does not cause exception."""
        resource_name = "mock_resource"
        all_resources = [StorageClass(MagicMock(), resource_name)]
        self.patch(Resource, "remove").side_effect = ApiException(status=404)
        logger_mock = self.patch(logger, "debug")
        self.patch_property(CephCsiCharm, "resources").return_value = all_resources

        self.harness.set_leader(True)

        self.harness.charm.purge_k8s_resources(MagicMock())

        logger_mock.assert_called_with("Resource %s is already removed.", resource_name)

    def test_resource_removal_api_failure(self):
        """Test that non-404 Api exceptions are re-raised when removing k8s resources."""
        api_status = 401
        api_reason = "Unauthenticated"
        expected_exception = ApiException(status=api_status, reason=api_reason)
        self.patch(Resource, "remove").side_effect = expected_exception
        self.harness.set_leader(True)

        with self.assertRaises(ApiException) as raised:
            self.harness.charm.purge_k8s_resources(MagicMock())

        self.assertEqual(raised.exception, expected_exception)

    def test_ceph_client_broker_available(self):
        """Test that when the ceph client broker is available, a pool is
        requested and Ceph capabilities are requested
        """
        self.patch(CephClientRequires, "create_replicated_pool")
        self.patch(CephClientRequires, "request_ceph_permissions")

        # Setup charm
        harness = Harness(CephCsiCharm)
        harness.begin()

        # add ceph-client relation
        relation_id = harness.add_relation(CephCsiCharm.CEPH_CLIENT_RELATION, "ceph-mon")
        harness.add_relation_unit(relation_id, "ceph-mon/0")

        self.assertEqual(harness.charm.ceph_client.create_replicated_pool.call_count, 2)
        harness.charm.ceph_client.create_replicated_pool.assert_any_call(name="ext4-pool")
        harness.charm.ceph_client.create_replicated_pool.assert_any_call(name="xfs-pool")
        harness.charm.ceph_client.request_ceph_permissions.assert_called_once_with(
            "ceph-csi",
            [
                "mon",
                "profile rbd, allow r",
                "mds",
                "allow rw",
                "mgr",
                "allow rw",
                "osd",
                "profile rbd, allow rw tag cephfs metadata=*",
            ],
        )

    def test_ceph_client_relation_changed_leader(self):
        """Test ceph-client-relation-changed hook on a leader unit"""
        self.patch(CephCsiCharm, "configure_ceph_cli")
        self.patch(CephCsiCharm, "get_ceph_fsid").return_value = "12345"
        self.patch(CephCsiCharm, "create_ceph_resources")
        self.harness.set_leader(True)
        relation_id = self.harness.add_relation(CephCsiCharm.CEPH_CLIENT_RELATION, "ceph-mon")
        self.harness.add_relation_unit(relation_id, "ceph-mon/0")

        self.harness.update_relation_data(
            relation_id,
            "ceph-mon/0",
            {"auth": "cephx", "key": "12345", "ceph-public-address": "10.0.0.1"},
        )

        self.assertEqual(
            self.harness.charm._stored.ceph_data,
            {"auth": "cephx", "key": "12345", "mon_hosts": ["10.0.0.1"]},
        )
        expected_resources = self.harness.charm.render_all_resource_definitions()
        self.harness.charm.create_ceph_resources.assert_called_once_with(expected_resources)

    def test_ceph_client_relation_changed_leader_resources_created(self):
        """Test ceph-client-relation-changed hook on a leader unit when
        resources have already been created
        """
        self.patch(CephCsiCharm, "configure_ceph_cli")
        self.patch(CephCsiCharm, "get_ceph_fsid").return_value = "abcde"
        self.patch(ConfigMap, "update_config_json")
        self.patch(Secret, "update_opaque_data")
        self.patch(StorageClass, "update_cluster_id")
        self.harness.charm._stored.resources_created = True
        self.harness.set_leader(True)
        relation_id = self.harness.add_relation(CephCsiCharm.CEPH_CLIENT_RELATION, "ceph-mon")
        self.harness.add_relation_unit(relation_id, "ceph-mon/0")

        self.harness.update_relation_data(
            relation_id,
            "ceph-mon/0",
            {"auth": "cephx", "key": "12345", "ceph-public-address": "10.0.0.1"},
        )

        self.assertEqual(
            self.harness.charm._stored.ceph_data,
            {"auth": "cephx", "key": "12345", "mon_hosts": ["10.0.0.1"]},
        )
        Secret.update_opaque_data.assert_called_once_with("userKey", "12345")
        StorageClass.update_cluster_id.assert_called_with("abcde")
        ConfigMap.update_config_json.assert_called_with(
            json.dumps([{"clusterID": "abcde", "monitors": ["10.0.0.1"]}], indent=4)
        )

    def test_ceph_client_relation_changed_non_leader(self):
        self.harness.set_leader(False)
        relation_id = self.harness.add_relation(CephCsiCharm.CEPH_CLIENT_RELATION, "ceph-mon")
        self.harness.add_relation_unit(relation_id, "ceph-mon/0")
        self.harness.update_relation_data(
            relation_id,
            "ceph-mon/0",
            {"auth": "cephx", "key": "12345", "ceph-public-address": "10.0.0.1"},
        )
        self.assertEqual(self.harness.charm._stored.ceph_data, {})

    def test_ceph_client_relation_departed(self):
        """Test that warning is logged about ceph-pools not being cleaned up after rel. removal."""
        logger_mock = self.patch(logger, "warning")
        pools = ", ".join(CephCsiCharm.REQUIRED_CEPH_POOLS)
        expected_msg = (
            "Ceph pools %s wont be removed. If you want to clean up pools manually, "
            "use juju action 'delete-pool' on 'ceph-mon' units"
        )

        # Operator testing harness does not provide a helper to "remove relation" so for now we'll
        # invoke method manually
        self.harness.charm._on_ceph_client_removed(MagicMock())

        logger_mock.assert_called_with(expected_msg, pools)

    def test_safe_load_ceph_client_data(self):
        """Test that `safe_load_ceph_admin_data` method loads data properly.

        Data are expected to be stored in the StoredState only if all the required keys are present
        in the relation data.
        """
        auth = "cephx"
        key = "bar"
        ceph_mons = ["10.0.0.1", "10.0.0.2"]
        relation_data = {"auth": auth, "key": key, "mon_hosts": ceph_mons}
        self.patch(CephClientRequires, "get_relation_data").return_value = relation_data

        self.harness.set_leader(True)

        # All data should be loaded
        result = self.harness.charm.safe_load_ceph_client_data()

        self.assertTrue(result)
        self.assertEqual(self.harness.charm._stored.ceph_data["auth"], auth)
        self.assertEqual(self.harness.charm._stored.ceph_data["key"], key)
        self.assertEqual(self.harness.charm._stored.ceph_data["mon_hosts"], ceph_mons)

        # reset
        self.harness.charm._stored.ceph_data = {}

        # don't load anything if relation data is missing
        CephClientRequires.get_relation_data.return_value = {"auth": auth, "key": key}

        result = self.harness.charm.safe_load_ceph_client_data()
        self.assertFalse(result)
        self.assertNotIn("auth", self.harness.charm._stored.ceph_data)
        self.assertNotIn("key", self.harness.charm._stored.ceph_data)
        self.assertNotIn("mon_hosts", self.harness.charm._stored.ceph_data)

        # don't execute anything on non-leader unit
        self.harness.set_leader(False)
        CephClientRequires.get_relation_data.return_value = relation_data

        result = self.harness.charm.safe_load_ceph_client_data()

        self.assertIs(result, None)
        self.assertNotIn("auth", self.harness.charm._stored.ceph_data)
        self.assertNotIn("key", self.harness.charm._stored.ceph_data)
        self.assertNotIn("mon_hosts", self.harness.charm._stored.ceph_data)
