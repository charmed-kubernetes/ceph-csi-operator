# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing
"""Collection of tests related to src/charm.py"""

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

from kubernetes.client import ApiException
from ops.testing import Harness

from charm import CephCsiCharm, client, config, logger, utils


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
        mock_relation_check = self.patch(CephCsiCharm, "check_required_relations")
        self.harness.charm.on.install.emit()

        mock_relation_check.assert_called_once()

    def test_required_relation_check(self):
        """Test that check_required_relations sets expected unit states."""
        self.patch_property(CephCsiCharm, "model")
        get_relation_mock = self.patch(CephCsiCharm.model, "get_relation")

        # Return object indicating existing relation
        get_relation_mock.return_value = object()
        self.harness.charm.check_required_relations()

        self.assertEqual(self.harness.charm.unit.status.name, "active")
        self.assertEqual(self.harness.charm.unit.status.message, "Unit is ready")

        # Return None indicating missing ceph relation
        get_relation_mock.return_value = None
        self.harness.charm.check_required_relations()

        self.assertEqual(self.harness.charm.unit.status.name, "blocked")
        self.assertEqual(self.harness.charm.unit.status.message, "Missing relations: ceph")

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

        resource_definitions.assert_called_once()
        storage_definitions.assert_called_once()

    def test_update_storage_class(self):
        """Test that update_storage_classes() removes old resources and re-adds new resources."""
        resources = [{"kind": "StorageClass", "metadata": {"name": "sc1"}}]
        self.harness.set_leader(True)
        remove_storage_mock = self.patch(StorageClass, "remove")
        render_storage_mock = self.patch(CephCsiCharm, "render_storage_definitions")
        render_storage_mock.return_value = resources
        create_resources_mock = self.patch(CephCsiCharm, "create_ceph_resources")

        self.harness.charm.update_storage_classes()

        remove_storage_mock.assert_called()
        create_resources_mock.assert_called_with(resources)

        # Test that nothing is executed on non-leader units
        self.harness.set_leader(False)
        remove_storage_mock.reset_mock()
        render_storage_mock.reset_mock()
        create_resources_mock.reset_mock()

        self.harness.charm.update_storage_classes()

        remove_storage_mock.assert_not_called()
        render_storage_mock.assert_not_called()
        create_resources_mock.assert_not_called()

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
            remove_call.assert_called_once()

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
