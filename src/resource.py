#!/usr/bin/env python3
# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
#
# Learn more at: https://juju.is/docs/sdk

"""Abstraction of Kubernetes Resources.

This module contains classes that wrap kubernetes resources and enable unified
interface for their removal.
"""
# Remove this pylint skip when functionality is added to `Resource` class
# pylint: disable=too-few-public-methods
import logging
from typing import Any, Callable, Dict

from kubernetes.client import AppsV1Api, CoreV1Api
from kubernetes.client import RbacAuthorizationV1Api as RbacAuthApi
from kubernetes.client import StorageV1Api

logger = logging.getLogger(__name__)


class MissingMethod(BaseException):
    """Exception that represents method that is not implemented."""


class Resource:
    """Base class for Kubernetes resources.

    So far, main purpose of this class is to provide unified `remove()` method
    that calls appropriate k8s api method.
    """

    def __init__(self, name: str, namespace: str = ""):
        """
        Initialize k8s resource with name and optionally namespace.

        If namespace is not provided, resource is treated as cluster-wide.
        :param name: resource name
        :param namespace: resource namespace
        """
        self.name = name
        self.namespace = namespace

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Resource):
            return NotImplemented

        return (
            other.__class__ == self.__class__
            and other.name == self.name
            and other.namespace == self.namespace
        )

    @property
    def _remove_action(self) -> Callable:
        """Return method of k8s api client that removes cluster resource."""
        raise MissingMethod(
            "Removal of {} {} is not implemented".format(self.__class__.__name__, self.name)
        )

    @property
    def _remove_namespaced_action(self) -> Callable:
        """Return method of k8s api client that removes namespaced resource."""
        raise MissingMethod(
            "Removal of namespaced {} {} is not "
            "implemented".format(self.__class__.__name__, self.name)
        )

    @property
    def _update_action(self) -> Callable:
        """Return method of k8s api client that updates cluster resource."""
        raise MissingMethod(
            "Update of {} resource is not implemented.".format(self.__class__.__name__)
        )

    @property
    def _update_namespaced_action(self) -> Callable:
        """Return method of k8s api client that updates namespaced resource."""
        raise MissingMethod(
            "Update of {} namespaced resource is not implemented.".format(self.__class__.__name__)
        )

    def remove(self) -> None:
        """Call appropriate api method to remove k8s resource."""
        if self.namespace:
            logger.debug(
                "Removing Kubernetes resource '%s' (%s) from namespace '%s'",
                self.name,
                self.__class__.__name__,
                self.namespace,
            )
            self._remove_namespaced_action(self.name, self.namespace)
        else:
            logger.debug(
                "Removing cluster-wide Kubernetes resource '%s' (%s)",
                self.name,
                self.__class__.__name__,
            )
            self._remove_action(self.name)

    def update(self, patch: Dict[str, Any]) -> None:
        """Updates k8s resource with new data.

        Argument "patch" is expected to be a dict consisting of keys that identify resource
        attributes and their values. For example, to update name of the Pod use:

            {"metadata": {"name": "newName"}}

        :param patch: attribute paths to update and their values (see docstring).
        :return: None
        """
        if self.namespace:
            logger.debug(
                "Updating Kubernetes resource '%s' (%s) in namespace '%s'",
                self.name,
                self.__class__.__name__,
                self.namespace,
            )
            self._update_namespaced_action(self.name, self.namespace, body=patch)
        else:
            self._update_action(self.name, body=patch)
            logger.debug(
                "Updating cluster-wide Kubernetes resource '%s' (%s)",
                self.name,
                self.__class__.__name__,
            )


class CoreResource(Resource):
    """Base class for resources associated with k8s CoreApi."""

    def __init__(self, api: CoreV1Api, name: str, namespace: str = ""):
        """Initialize k8s resource managed via CoreApi.

        If namespace is not provided, resource is treated as cluster-wide.
        :param api: CoreV1Api instance
        :param name: resource name
        :param namespace: resource namespace
        """
        super().__init__(name=name, namespace=namespace)
        self.api = api


class AuthResource(Resource):
    """Base class for resources associated with k8s RbacAuthorizationApi."""

    def __init__(self, api: RbacAuthApi, name: str, namespace: str = ""):
        """Initialize k8s resource managed via RbacAuthorizationApi.

        If namespace is not provided, resource is treated as cluster-wide.
        :param api: RbacAuthorizationV1Api instance
        :param name: resource name
        :param namespace: resource namespace
        """
        super().__init__(name, namespace)
        self.api = api


class StorageResource(Resource):
    """Base class for resources associated with k8s StorageApi"""

    def __init__(self, api: StorageV1Api, name: str, namespace: str = ""):
        """Initialize k8s resource managed via StorageApi.

        If namespace is not provided, resource is treated as cluster-wide.
        :param api: StorageV1Api instance
        :param name: resource name
        :param namespace: resource namespace
        """
        super().__init__(name, namespace)
        self.api = api


class AppsResource(Resource):
    """Base class for resources associated with k8s AppsApi"""

    def __init__(self, api: AppsV1Api, name: str, namespace: str = ""):
        """Initialize k8s resource managed via Apps.

        If namespace is not provided, resource is treated as cluster-wide.
        :param api: AppsV1Api instance
        :param name: resource name
        :param namespace: resource namespace
        """
        super().__init__(name, namespace)
        self.api = api


class Secret(CoreResource):
    """Kubernetes 'Secret' resource."""

    @property
    def _remove_namespaced_action(self) -> Callable:
        return self.api.delete_namespaced_secret

    @property
    def _update_namespaced_action(self) -> Callable:
        return self.api.patch_namespaced_secret

    def update_opaque_data(self, key: str, value: str) -> None:
        """Update arbitrary data in opaque secret.

        :param key: Key in opaque secrete data.
        :param value: Value of the key
        :return: None
        """
        self.update({"stringData": {key: value}})


class ServiceAccount(CoreResource):
    """Kubernetes 'ServiceAccount' resource."""

    @property
    def _remove_namespaced_action(self) -> Callable:
        return self.api.delete_namespaced_service_account


class Service(CoreResource):
    """Kubernetes 'Service' resource."""

    @property
    def _remove_namespaced_action(self) -> Callable:
        return self.api.delete_namespaced_service


class ConfigMap(CoreResource):
    """Kubernetes 'ConfigMap' resource."""

    @property
    def _remove_namespaced_action(self) -> Callable:
        return self.api.delete_namespaced_config_map

    @property
    def _update_namespaced_action(self) -> Callable:
        return self.api.patch_namespaced_config_map

    def update_config_json(self, config: str) -> None:
        """Update value of "config.json" field in "data" attribute."""
        self.update({"data": {"config.json": config}})


class ClusterRole(AuthResource):
    """Kubernetes 'ClusterRole' resource."""

    @property
    def _remove_action(self) -> Callable:
        return self.api.delete_cluster_role


class ClusterRoleBinding(AuthResource):
    """Kubernetes 'ClusterRoleBinding' resource."""

    @property
    def _remove_action(self) -> Callable:
        return self.api.delete_cluster_role_binding


class Role(AuthResource):
    """Kubernetes 'Role' resource."""

    @property
    def _remove_namespaced_action(self) -> Callable:
        return self.api.delete_namespaced_role


class RoleBinding(AuthResource):
    """Kubernetes 'RoleBinding' resource."""

    @property
    def _remove_namespaced_action(self) -> Callable:
        return self.api.delete_namespaced_role_binding


class StorageClass(StorageResource):
    """Kubernetes 'StorageClass' resource."""

    @property
    def _remove_action(self) -> Callable:
        return self.api.delete_storage_class

    @property
    def _update_action(self) -> Callable:
        return self.api.patch_storage_class

    def update_cluster_id(self, id_: str) -> None:
        """Update clusterID."""
        self.update({"parameters": {"clusterID": id_}})

    def set_default(self, is_default: bool = True) -> None:
        """Set default status of the StorageClass."""
        patch = {
            "metadata": {
                "annotations": {
                    "storageclass.kubernetes.io/is-default-class": str(is_default).lower()
                }
            }
        }
        self.update(patch)


class Deployment(AppsResource):
    """Kubernetes 'Deployment' resource."""

    @property
    def _remove_namespaced_action(self) -> Callable:
        return self.api.delete_namespaced_deployment


class DaemonSet(AppsResource):
    """Kubernetes 'DaemonSet' resource."""

    @property
    def _remove_namespaced_action(self) -> Callable:
        return self.api.delete_namespaced_daemon_set
