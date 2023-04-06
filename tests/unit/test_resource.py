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
from unittest.mock import MagicMock, PropertyMock, call, patch


class TestResourceBaseClass(unittest.TestCase):
    """Tests for resource.Resource base class."""

    def test_remove_action_props_not_implemented(self):
        """Test that base class does not have any "remove action" getters implemented."""
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

    def test_update_action_props_not_implemented(self):
        """Test that base class does not have any "update action" getters implemented."""
        resource = Resource("foo")

        with self.assertRaises(MissingMethod):
            _ = resource._update_action

        with self.assertRaises(MissingMethod):
            _ = resource._update_namespaced_action

    def test_update_namespaced_resource(self):
        """Test that resource with namespace will call namespaced update function."""
        mock_action = MagicMock()
        name = "foo"
        namespace = "bar"
        update_body = {"property": "new_value"}

        with patch.object(
            Resource, "_update_namespaced_action", new_callable=PropertyMock
        ) as action_getter_mock:
            action_getter_mock.return_value = mock_action
            resource = Resource(name=name, namespace=namespace)

            resource.update(update_body)

        mock_action.assert_called_once_with(name, namespace, body=update_body)

    def test_update_global_resource(self):
        """Test that resource without namespace calls global update function."""
        mock_action = MagicMock()
        name = "foo"
        update_body = {"property": "new_value"}

        with patch.object(
            Resource, "_update_action", new_callable=PropertyMock
        ) as action_getter_mock:
            action_getter_mock.return_value = mock_action
            resource = Resource(name=name)

            resource.update(update_body)

        mock_action.assert_called_once_with(name, body=update_body)

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
        self.update_data = {"prop": "new_value"}
        self.api_mock = MagicMock()
        self.remove_action_mock = MagicMock()
        self.update_action_mock = MagicMock()

    def patch(self, obj, method) -> MagicMock:
        """Method mock scoped to the duration of single unit test."""
        _patch = patch.object(obj, method)
        mock_method = _patch.start()
        self.addCleanup(_patch.stop)
        return mock_method

    def test_secret_removal(self):
        """Test that removing Secret resource calls appropriate api method."""
        self.api_mock.delete_namespaced_secret = self.remove_action_mock

        secret = Secret(self.api_mock, self.res_name, self.res_namespace)
        secret.remove()

        self.remove_action_mock.assert_called_once_with(self.res_name, self.res_namespace)

    def test_secret_update(self):
        """Test that updating Secret resource calls appropriate api method."""
        self.api_mock.patch_namespaced_secret = self.update_action_mock

        secret = Secret(self.api_mock, self.res_name, self.res_namespace)
        secret.update(self.update_data)

        self.update_action_mock.assert_called_once_with(
            self.res_name, self.res_namespace, body=self.update_data
        )

    def test_secret_update_opaque_data(self):
        """Test that `Secret.update_opaque_data` calls update method properly."""
        property_to_update = "userKey"
        new_data = "newSecretKey"
        expected_patch = {"stringData": {property_to_update: new_data}}

        update_mock = self.patch(Secret, "update")

        secret = Secret(self.api_mock, self.res_name, self.res_namespace)
        secret.update_opaque_data(property_to_update, new_data)

        update_mock.assert_called_once_with(expected_patch)

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

    def test_config_map_update(self):
        """Test that updating ConfigMap resource calls appropriate api method."""
        self.api_mock.patch_namespaced_config_map = self.update_action_mock

        config_map = ConfigMap(self.api_mock, self.res_name, self.res_namespace)
        config_map.update(self.update_data)

        self.update_action_mock.assert_called_once_with(
            self.res_name, self.res_namespace, body=self.update_data
        )

    def test_config_map_update_json(self):
        """Test that `ConfigMap.update_config_json` calls update method properly."""
        new_config = "new_data"
        expected_patch = {"data": {"config.json": new_config}}

        update_mock = self.patch(ConfigMap, "update")

        config_map = ConfigMap(self.api_mock, self.res_name, self.res_namespace)
        config_map.update_config_json(new_config)

        update_mock.assert_called_once_with(expected_patch)

    def test_config_map_update_conf(self):
        """Test that `ConfigMap.update_config_conf` calls update method properly."""
        new_config = "[global]\nauth_cluster_required = new_data\nauth_service_required = new_data\nauth_client_required = new_data\n"
        expected_patch = {"data": {"config.conf": new_config}}

        update_mock = self.patch(ConfigMap, "update")

        config_map = ConfigMap(self.api_mock, self.res_name, self.res_namespace)
        config_map.update_config_conf("new_data")

        update_mock.assert_called_once_with(expected_patch)

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

    def test_storage_class_update(self):
        """Test that updating StorageClass resource calls appropriate api method."""
        self.api_mock.patch_storage_class = self.update_action_mock

        storage_class = StorageClass(self.api_mock, self.res_name)
        storage_class.update(self.update_data)

        self.update_action_mock.assert_called_once_with(self.res_name, body=self.update_data)

    def test_storage_class_update_cluster_id(self):
        """Test that `StorageClass.update_cluster_id` calls update method properly."""
        new_id = "new_id"
        expected_patch = {"parameters": {"clusterID": new_id}}

        update_mock = self.patch(StorageClass, "update")

        storage_class = StorageClass(self.api_mock, self.res_name)
        storage_class.update_cluster_id(new_id)

        update_mock.assert_called_once_with(expected_patch)

    def test_storage_class_set_default(self):
        """Test that `StorageClass.set_default` calls update method properly."""

        def get_patch(is_default: bool) -> dict:
            return {
                "metadata": {
                    "annotations": {
                        "storageclass.kubernetes.io/is-default-class": str(is_default).lower()
                    }
                }
            }

        update_mock = self.patch(StorageClass, "update")
        expected_calls = [call(get_patch(True)), call(get_patch(False))]

        storage_class = StorageClass(self.api_mock, self.res_name)
        storage_class.set_default(True)
        storage_class.set_default(False)

        update_mock.assert_has_calls(expected_calls)

    def test_deployment_removal(self):
        """Test that removing Deployment resource calls appropriate api method."""
        self.api_mock.delete_namespaced_deployment = self.remove_action_mock

        deployment = Deployment(self.api_mock, self.res_name, self.res_namespace)
        deployment.remove()

        self.remove_action_mock.assert_called_once_with(self.res_name, self.res_namespace)

    def test_deployment_update_replicas(self):
        """Test that `Deployment.update_replicas` calls update method properly."""
        new_value = "~~patched~~"
        expected_patch = {"spec": {"replicas": new_value}}

        deployment = Deployment(self.api_mock, self.res_name, self.res_namespace)
        update_mock = self.patch(deployment.api, "patch_namespaced_deployment")
        deployment.update_replicas(new_value)

        update_mock.assert_called_once_with(self.res_name, self.res_namespace, body=expected_patch)

    def test_daemon_set_removal(self):
        """Test that removing DaemonSet resource calls appropriate api method."""
        self.api_mock.delete_namespaced_daemon_set = self.remove_action_mock

        daemon_set = DaemonSet(self.api_mock, self.res_name, self.res_namespace)
        daemon_set.remove()

        self.remove_action_mock.assert_called_once_with(self.res_name, self.res_namespace)
