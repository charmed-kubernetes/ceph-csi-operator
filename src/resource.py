from abc import ABC, abstractmethod
from typing import Callable

from kubernetes.client import (
    CoreV1Api,
    RbacAuthorizationV1Api as RbacAuthApi,
    StorageV1Api,
    AppsV1Api
)


class Resource(ABC):

    def __init__(self, name: str, namespace: str = ''):
        self.name = name
        self.namespace = namespace

    @property
    @abstractmethod
    def _remove_action(self) -> Callable:
        """Return method of k8s api client that removes cluster resource."""

    @property
    @abstractmethod
    def _remove_namespaced_action(self) -> Callable:
        """Return method of k8s api client that removes namespaced resource."""

    def remove(self):
        if self.namespace:
            self._remove_namespaced_action(self.name, self.namespace)
        else:
            self._remove_action(self.name)


class CoreResource(Resource):

    def __init__(self, api: CoreV1Api, name: str, namespace: str = ''):
        super().__init__(name=name, namespace=namespace)
        self.api = api

    @property
    def _remove_action(self) -> Callable:
        raise NotImplementedError()

    @property
    def _remove_namespaced_action(self) -> Callable:
        raise NotImplementedError()


class AuthResource(Resource):

    def __init__(self, api: RbacAuthApi, name: str, namespace: str = ''):
        super().__init__(name, namespace)
        self.api = api

    @property
    def _remove_action(self) -> Callable:
        raise NotImplementedError()

    @property
    def _remove_namespaced_action(self) -> Callable:
        raise NotImplementedError()


class StorageResource(Resource):

    def __init__(self, api: StorageV1Api, name: str, namespace: str = ''):
        super().__init__(name, namespace)
        self.api = api

    @property
    def _remove_action(self) -> Callable:
        raise NotImplementedError()

    @property
    def _remove_namespaced_action(self) -> Callable:
        raise NotImplementedError()


class AppsResource(Resource):

    def __init__(self, api: AppsV1Api, name: str, namespace: str = ''):
        super().__init__(name, namespace)
        self.api = api

    @property
    def _remove_action(self) -> Callable:
        raise NotImplementedError()

    @property
    def _remove_namespaced_action(self) -> Callable:
        raise NotImplementedError()


class Secret(CoreResource):

    @property
    def _remove_action(self) -> Callable:
        raise NotImplementedError()

    @property
    def _remove_namespaced_action(self) -> Callable:
        return self.api.delete_namespaced_secret


class ServiceAccount(CoreResource):

    @property
    def _remove_action(self) -> Callable:
        raise NotImplementedError()

    @property
    def _remove_namespaced_action(self) -> Callable:
        return self.api.delete_namespaced_service_account


class Service(CoreResource):

    @property
    def _remove_action(self) -> Callable:
        raise NotImplementedError()

    @property
    def _remove_namespaced_action(self) -> Callable:
        return self.api.delete_namespaced_service


class ConfigMap(CoreResource):

    @property
    def _remove_action(self) -> Callable:
        raise NotImplementedError()

    @property
    def _remove_namespaced_action(self) -> Callable:
        return self.api.delete_namespaced_config_map


class ClusterRole(AuthResource):

    @property
    def _remove_action(self) -> Callable:
        return self.api.delete_cluster_role

    @property
    def _remove_namespaced_action(self) -> Callable:
        raise NotImplementedError()


class ClusterRoleBinding(AuthResource):

    @property
    def _remove_action(self) -> Callable:
        return self.api.delete_cluster_role_binding

    @property
    def _remove_namespaced_action(self) -> Callable:
        raise NotImplementedError()


class Role(AuthResource):

    @property
    def _remove_action(self) -> Callable:
        raise NotImplementedError()

    @property
    def _remove_namespaced_action(self) -> Callable:
        return self.api.delete_namespaced_role


class RoleBinding(AuthResource):

    @property
    def _remove_action(self) -> Callable:
        raise NotImplementedError()

    @property
    def _remove_namespaced_action(self) -> Callable:
        return self.api.delete_namespaced_role_binding


class StorageClass(StorageResource):

    @property
    def _remove_action(self) -> Callable:
        return self.api.delete_storage_class

    @property
    def _remove_namespaced_action(self) -> Callable:
        raise NotImplementedError()


class Deployment(AppsResource):

    @property
    def _remove_action(self) -> Callable:
        raise NotImplementedError()

    @property
    def _remove_namespaced_action(self) -> Callable:
        return self.api.delete_namespaced_deployment


class DaemonSet(AppsResource):

    @property
    def _remove_action(self) -> Callable:
        raise NotImplementedError()

    @property
    def _remove_namespaced_action(self) -> Callable:
        return self.api.delete_namespaced_daemon_set
