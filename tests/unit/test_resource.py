# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing
"""Collection of tests related to src/resource.py"""

import unittest
from resource import (
    ClusterRole,
    ClusterRoleBinding,
    ConfigMap,
    DaemonSet,
    Deployment,
    MissingMethod,
    Resource,
    Role,
    RoleBinding,
    Secret,
    Service,
    ServiceAccount,
    StorageClass,
)
from unittest.mock import MagicMock, PropertyMock, patch


class TestResourceBaseClass(unittest.TestCase):
    """Tests for resource.Resource base class."""

    def test_remove_action_props_not_implemented(self):
        """Test that base class does not have any remove action getters implemented."""
        resource = Resource("foo")

        with self.assertRaises(MissingMethod):
            _ = resource._remove_action

        with self.assertRaises(MissingMethod):
            _ = resource._remove_namespaced_action

    def test_remove_namespaced_resource(self):
        """Test that resource with namespace will call namespaced removal function."""
        mock_action = MagicMock()
        name = "foo"
        namespace = "bar"

        with patch.object(
            Resource, "_remove_namespaced_action", new_callable=PropertyMock
        ) as action_getter_mock:
            action_getter_mock.return_value = mock_action
            resource = Resource(name=name, namespace=namespace)

            resource.remove()
        mock_action.assert_called_once_with(name, namespace)

    def test_remove_global_resource(self):
        """Test that resource without namespace calls global removal function."""
        mock_action = MagicMock()
        name = "foo"

        with patch.object(
            Resource, "_remove_action", new_callable=PropertyMock
        ) as action_getter_mock:
            action_getter_mock.return_value = mock_action
            resource = Resource(name=name)

            resource.remove()
        mock_action.assert_called_once_with(name)

    def test_resource_equal(self):
        """Test cases in which two Resource instances should compare equal."""
        name = "foo"
        namespace = "bar"
        res1 = Resource(name, namespace)
        res2 = Resource(name, namespace)

        # class, name and namespace must be equal
        self.assertEqual(res1, res2)

    def test_resource_not_equal(self):
        """Test cases in which Resource instance is not equal with compared object."""
        matching_name = "good_resource"
        matching_namespace = "good_namespace"
        mismatched_name = "bad_resource"
        mismatched_namespace = "bad_namespace"

        resource = Resource(matching_name, matching_namespace)
        k8s_api_mock = MagicMock()

        # Comparing with other objects returns False
        self.assertFalse(resource == "random string")

        # Comparing two different subclasses of Resource returns False
        secret = Secret(k8s_api_mock, matching_name, matching_namespace)
        storage = StorageClass(k8s_api_mock, matching_name, matching_namespace)
        self.assertNotEqual(storage, secret)

        # Resources with mismatched names are not equal
        bad_name = Resource(mismatched_name, matching_namespace)
        self.assertNotEqual(resource, bad_name)

        # Resources with mismatched namespaces are not equal
        bad_namespace = Resource(matching_name, mismatched_namespace)
        self.assertNotEqual(resource, bad_namespace)

        # Resources with mismatched names and namespaces are not equal
        different_resource = Resource(mismatched_name, mismatched_namespace)
        self.assertNotEqual(resource, different_resource)


class TestResourceActions(unittest.TestCase):
    """Tests for predefined child classes of resource.Resource base class."""

    def setUp(self):
        self.res_name = "foo"
        self.res_namespace = "bar"
        self.api_mock = MagicMock()
        self.remove_action_mock = MagicMock()

    def test_secret_removal(self):
        """Test that removing Secret resource calls appropriate api method."""
        self.api_mock.delete_namespaced_secret = self.remove_action_mock

        secret = Secret(self.api_mock, self.res_name, self.res_namespace)
        secret.remove()

        self.remove_action_mock.assert_called_once_with(self.res_name, self.res_namespace)

    def test_service_removal(self):
        """Test that removing Service resource calls appropriate api method."""
        self.api_mock.delete_namespaced_service = self.remove_action_mock

        service = Service(self.api_mock, self.res_name, self.res_namespace)
        service.remove()

        self.remove_action_mock.assert_called_once_with(self.res_name, self.res_namespace)

    def test_service_account_removal(self):
        """Test that removing ServiceAccount resource calls appropriate api method."""
        self.api_mock.delete_namespaced_service_account = self.remove_action_mock

        service_account = ServiceAccount(self.api_mock, self.res_name, self.res_namespace)
        service_account.remove()

        self.remove_action_mock.assert_called_once_with(self.res_name, self.res_namespace)

    def test_config_map_removal(self):
        """Test that removing ConfigMap resource calls appropriate api method."""
        self.api_mock.delete_namespaced_config_map = self.remove_action_mock

        config_map = ConfigMap(self.api_mock, self.res_name, self.res_namespace)
        config_map.remove()

        self.remove_action_mock.assert_called_once_with(self.res_name, self.res_namespace)

    def test_cluster_role_removal(self):
        """Test that removing ClusterRole resource calls appropriate api method."""
        self.api_mock.delete_cluster_role = self.remove_action_mock

        cluster_role = ClusterRole(self.api_mock, self.res_name)
        cluster_role.remove()

        self.remove_action_mock.assert_called_once_with(self.res_name)

    def test_cluster_role_binding_removal(self):
        """Test that removing ClusterRoleBinding resource calls appropriate api method."""
        self.api_mock.delete_cluster_role_binding = self.remove_action_mock

        cluster_role_binding = ClusterRoleBinding(self.api_mock, self.res_name)
        cluster_role_binding.remove()

        self.remove_action_mock.assert_called_once_with(self.res_name)

    def test_role_removal(self):
        """Test that removing Role resource calls appropriate api method."""
        self.api_mock.delete_namespaced_role = self.remove_action_mock

        role = Role(self.api_mock, self.res_name, self.res_namespace)
        role.remove()

        self.remove_action_mock.assert_called_once_with(self.res_name, self.res_namespace)

    def test_role_binding_removal(self):
        """Test that removing RoleBinding resource calls appropriate api method."""
        self.api_mock.delete_namespaced_role_binding = self.remove_action_mock

        role_binding = RoleBinding(self.api_mock, self.res_name, self.res_namespace)
        role_binding.remove()

        self.remove_action_mock.assert_called_once_with(self.res_name, self.res_namespace)

    def test_storage_class_removal(self):
        """Test that removing StorageClass resource calls appropriate api method."""
        self.api_mock.delete_storage_class = self.remove_action_mock

        storage_class = StorageClass(self.api_mock, self.res_name)
        storage_class.remove()

        self.remove_action_mock.assert_called_once_with(self.res_name)

    def test_deployment_removal(self):
        """Test that removing Deployment resource calls appropriate api method."""
        self.api_mock.delete_namespaced_deployment = self.remove_action_mock

        deployment = Deployment(self.api_mock, self.res_name, self.res_namespace)
        deployment.remove()

        self.remove_action_mock.assert_called_once_with(self.res_name, self.res_namespace)

    def test_daemon_set_removal(self):
        """Test that removing DaemonSet resource calls appropriate api method."""
        self.api_mock.delete_namespaced_daemon_set = self.remove_action_mock

        daemon_set = DaemonSet(self.api_mock, self.res_name, self.res_namespace)
        daemon_set.remove()

        self.remove_action_mock.assert_called_once_with(self.res_name, self.res_namespace)
