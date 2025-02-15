import logging
import pickle
from hashlib import md5
from typing import Any, Dict, Generator, List, Optional

from lightkube.codecs import AnyResource
from lightkube.core.resource import NamespacedResource
from lightkube.models.core_v1 import Toleration
from ops.manifests import ManifestLabel, Manifests, Patch
from ops.manifests.literals import APP_LABEL

log = logging.getLogger(__name__)


class SafeManifest(Manifests):
    purging: bool = False

    def hash(self) -> int:
        """Calculate a hash of the current configuration."""
        return int(md5(pickle.dumps(self.config)).hexdigest(), 16)

    @property
    def config(self) -> Dict[str, Any]:
        return {}  # pragma: no cover

    def evaluate(self) -> Optional[str]: ...  # pragma: no cover


class AdjustNamespace(Patch):
    """Adjust metadata namespace."""

    def __call__(self, obj: AnyResource) -> None:
        """Replace namespace if object supports it."""
        if isinstance(obj, NamespacedResource) and obj.metadata:
            ns = self.manifests.config["namespace"]
            obj.metadata.namespace = ns


class ManifestLabelExcluder(ManifestLabel):
    """Exclude applying labels to CSIDriver."""

    def __call__(self, obj: AnyResource) -> None:
        super().__call__(obj)
        if obj.kind == "CSIDriver" and obj.metadata and obj.metadata.labels:
            # Remove the app label from the CSIDriver to disassociate it from the application
            obj.metadata.labels.pop(APP_LABEL, None)


class RbacAdjustments(Patch):
    """Update RBAC Attributes."""

    RBAC_NAME_FORMATTER = "ceph-rbac-name-formatter"
    REQUIRED_CONFIG = {RBAC_NAME_FORMATTER}

    def _rename(self, name: str) -> str:
        """Rename the object."""
        formatter = self.manifests.config[self.RBAC_NAME_FORMATTER]
        return formatter.format(name=name, app=self.manifests.model.app.name)

    def __call__(self, obj: AnyResource) -> None:
        ns = self.manifests.config["namespace"]
        if not obj.metadata or not obj.metadata.name:
            log.error("Object is missing metadata or name. %s", obj)
            return

        if obj.kind in ["ClusterRole", "ClusterRoleBinding"]:
            obj.metadata.name = self._rename(obj.metadata.name)

        if obj.kind in ["ClusterRoleBinding", "RoleBinding"]:
            for each in obj.subjects:
                if each.kind == "ServiceAccount":
                    each.namespace = ns
            if obj.roleRef.kind == "ClusterRole":
                obj.roleRef.name = self._rename(obj.roleRef.name)


class ConfigureLivenessPrometheus(Patch):
    """Configure liveness probe for Prometheus."""

    def __init__(self, manifests: Manifests, kind: str, name: str, config: str) -> None:
        super().__init__(manifests)
        self.kind = kind
        self.name = name
        self.config = config
        self._config_suffix = "metrics-port"

    def __call__(self, obj: AnyResource) -> None:
        """Configure liveness probe for Prometheus."""
        if obj.kind != self.kind or not obj.metadata or obj.metadata.name != self.name:
            return

        if obj.kind in ["Deployment", "DaemonSet"]:
            containers = self.filter_containers(obj.spec.template.spec.containers)
            obj.spec.template.spec.containers = list(containers)
        elif obj.kind == "Service":
            mapping = self.filter_portmap(obj.spec.ports)
            obj.spec.ports = list(mapping)

    def filter_portmap(self, portmap: list) -> Generator:
        """Update the http-metrics port mapping."""
        port = self.manifests.config.get(f"{self._config_suffix}-{self.config}")
        for mapping in portmap:
            if mapping.name != "http-metrics":
                yield mapping

            if port != -1:
                mapping.targetPort = port

            yield mapping

    def filter_containers(self, containers: list) -> Generator:
        """Update the prometheus-liveness container."""
        port = self.manifests.config.get(f"{self._config_suffix}-{self.config}")
        for container in containers:
            if container.name != "liveness-prometheus":
                yield container

            if port == -1:
                continue

            metrics_port_config = "metricsport"
            container.args = [
                (
                    f"--{metrics_port_config}={port}"
                    if arg.startswith(f"--{metrics_port_config}=")
                    else arg
                )
                for arg in container.args
            ]
            yield container


class CephToleration(Toleration):
    @classmethod
    def _from_string(cls, toleration_str: str) -> "CephToleration":
        """Parses a toleration string into a Toleration object.

        Raises:
            ValueError: If the string is not a valid toleration.
        """
        # A missing ',' will raise ValueError
        key_value, operator, *effects = toleration_str.split(",")
        # A missing '=' will raise ValueError
        key, value = key_value.split("=", 1)

        if operator not in ["Exists", "Equal"]:
            raise ValueError(f"Invalid {operator=}")
        if len(effects) > 1:
            raise ValueError(f"Too many effects='{','.join(effects)}'")
        effect = effects[0] if effects else ""
        if effect not in ["NoSchedule", "PreferNoSchedule", "NoExecute", ""]:
            raise ValueError(f"Invalid {effect=}")

        return cls(
            key=key if key else None,  # Convert empty string to None
            value=value if value else None,
            operator=operator,
            effect=effect if effect else None,
        )

    @classmethod
    def from_space_separated(cls, tolerations: str) -> List["CephToleration"]:
        """Parses a space separated string of tolerations into a list of Toleration objects.

        Raises:
            ValueError: If any of the tolerations are invalid
        """
        try:
            return [cls._from_string(toleration) for toleration in tolerations.split()]
        except ValueError as e:
            raise ValueError(f"Invalid tolerations: {e}") from e


def update_storage_params(ceph_type: str, config: Dict[str, Any], params: Dict[str, str]) -> None:
    """Adjust parameters for storage class."""
    cfg_name = f"{ceph_type}-storage-class-parameters"
    if not (adjustments := config.get(cfg_name)):
        log.info(f"No adjustments for {ceph_type} storage-class parameters")
        return

    for adjustment in adjustments.split(" "):
        key_value = adjustment.split("=", 1)
        if len(key_value) == 2:
            params[key_value[0]] = key_value[1]
        elif adjustment.endswith("-"):
            params.pop(adjustment[:-1], None)
        else:
            log.warning("Invalid parameter: %s in %s", adjustment, cfg_name)
