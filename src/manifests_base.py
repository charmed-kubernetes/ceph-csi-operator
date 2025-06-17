import logging
import pickle
from functools import cached_property
from hashlib import md5
from typing import Any, Dict, Generator, List, Optional, Tuple, cast

from lightkube.codecs import AnyResource
from lightkube.core.resource import NamespacedResource
from lightkube.models.core_v1 import Toleration
from lightkube.resources.core_v1 import Secret
from ops.manifests import Addition, Manifests, Patch
from ops.manifests.manipulations import Subtraction

log = logging.getLogger(__name__)


class SafeManifest(Manifests):
    purging: bool = False

    def hash(self) -> int:
        """Calculate a hash of the current configuration."""
        return int(md5(pickle.dumps(self.config)).hexdigest(), 16)

    @property
    def csidriver(self) -> "CSIDriverAdjustments":
        for manipulation in self.manipulations:
            if isinstance(manipulation, CSIDriverAdjustments):
                return manipulation
        raise ValueError("CSIDriverAdjustments not found")

    @property
    def config(self) -> Dict[str, Any]:
        return {}  # pragma: no cover

    def evaluate(self) -> Optional[str]: ...  # pragma: no cover


class StorageSecret(Addition):
    """Create secret for the Provider."""

    NAME: str
    REQUIRED_CONFIG: Dict[str, List[str]]

    def __call__(self) -> Optional[AnyResource]:
        """Craft the secrets object for the deployment."""
        manifest = self.manifests.name

        if cast(SafeManifest, self.manifests).purging:
            # If we are purging, we may not be able to create any storage classes
            # Just return a fake storage class to satisfy delete_manifests method
            # which will look up all storage classes installed by this app/manifest
            return Secret.from_dict(dict(metadata=dict(name=self.NAME)))

        if not self.manifests.config["enabled"]:
            log.info("Ignore Secret from %s", manifest)
            return None

        stringData = {}
        for k, keys in self.REQUIRED_CONFIG.items():
            for secret_key in keys:
                if value := self.manifests.config.get(k):
                    stringData[secret_key] = value
                else:
                    log.error("%s is missing required secret item: '%s'", manifest, k)
                    return None

        log.info("Modelling secret data for %s.", manifest)
        return Secret.from_dict(dict(metadata=dict(name=self.NAME), stringData=stringData))


class AdjustNamespace(Patch):
    """Adjust metadata namespace."""

    def __call__(self, obj: AnyResource) -> None:
        """Replace namespace if object supports it."""
        if isinstance(obj, NamespacedResource) and obj.metadata:
            ns = self.manifests.config["namespace"]
            obj.metadata.namespace = ns


class RbacAdjustments(Patch):
    """Update RBAC Attributes."""

    RBAC_NAME_FORMATTER = "ceph-rbac-name-formatter"
    REQUIRED_CONFIG = {RBAC_NAME_FORMATTER}

    def _rename(self, name: str) -> str:
        """Rename the object."""
        formatter = self.manifests.config[self.RBAC_NAME_FORMATTER]
        fmt_context = {
            "name": name,
            "app": self.manifests.model.app.name,
            "namespace": self.manifests.config["namespace"],
        }
        return formatter.format(**fmt_context)

    def __call__(self, obj: AnyResource) -> None:
        ns = self.manifests.config["namespace"]
        if not obj.metadata or not obj.metadata.name:
            log.error("Object is missing metadata or name. %s", obj)
            return

        if obj.kind in ["ClusterRole", "ClusterRoleBinding"]:
            obj.metadata.name = self._rename(obj.metadata.name)

        if obj.kind in ["ClusterRoleBinding", "RoleBinding"]:
            for each in obj.subjects:
                # RoleBinding and ClusterRoleBinding subjects
                # reference ServiceAccount in a different namespace
                # so we need to set the namespace to the one we are deploying
                if each.kind == "ServiceAccount":
                    each.namespace = ns
            if obj.roleRef.kind == "ClusterRole":
                # ClusterRoleBinding roleRef references a ClusterRole
                # which is not namespaced and has been renamed
                obj.roleRef.name = self._rename(obj.roleRef.name)


class StorageClassFactory(Addition):
    """Create ceph-csi storage classes."""

    def __init__(self, manifests: Manifests, fs_type: str):
        super().__init__(manifests)
        self._fs_type = fs_type

    @property
    def storage_class_parameters_key(self) -> str:
        """Storage class parameters key."""
        return f"{self._fs_type}-storage-class-parameters"

    @property
    def storage_class_parameters(self) -> Dict[str, Optional[str]]:
        """Storage class parameters."""
        params = {}
        cfg_name = self.storage_class_parameters_key
        adjustments = self.manifests.config.get(cfg_name)
        if not adjustments:
            log.info(f"No adjustments for {self._fs_type} storage-class parameters")
            return params

        for adjustment in adjustments.split():
            key_value = adjustment.split("=", 1)
            if len(key_value) == 2:
                params[key_value[0]] = key_value[1]
            elif adjustment.endswith("-"):
                params[adjustment[:-1]] = None
            else:
                log.error("Invalid storage-class-parameter: %s in %s", adjustment, cfg_name)
                raise ValueError(f"Invalid storage-class-parameter: {adjustment} in {cfg_name}")

        return params

    @property
    def name_formatter_key(self) -> str:
        """Storage class name formatter key."""
        return f"{self._fs_type}-storage-class-name-formatter"

    @property
    def name_formatter(self) -> str:
        """Storage class name formatter."""
        key = self.name_formatter_key
        return str(self.manifests.config.get(key) or "")

    def name(self, context: Dict[str, Any] = {}) -> str:
        """Create a storage-class name using the name formatter."""
        fmt_context = {
            "app": self.manifests.model.app.name,
            "namespace": self.manifests.config["namespace"],
            **context,
        }
        return self.name_formatter.format(**fmt_context)

    def update_params(self, params: Dict[str, str]) -> None:
        """Adjust parameters for storage class."""
        for key, value in self.storage_class_parameters.items():
            if value is None:
                params.pop(key, None)
            else:
                params[key] = value

    def evaluate(self) -> None:
        """Evaluate the storage class."""
        if not self.name_formatter:
            log.error("Missing storage class name %s", self.name_formatter_key)
            raise ValueError(f"Missing storage class name {self.name_formatter_key}")

        if not self.storage_class_parameters:
            log.warning("No storage class parameters for %s", self._fs_type)


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


class ProvisionerAdjustments(Patch):
    """Update provisioner manifest objects."""

    PROVISIONER_NAME: str
    PLUGIN_NAME: str

    def tolerations(self) -> Tuple[List[CephToleration], bool]:
        return [], False

    def adjust_container_specs(self, obj: AnyResource) -> None:
        csidriver = cast(SafeManifest, self.manifests).csidriver
        original_dn, updated_dn = csidriver.default_name, csidriver.formatted
        kubelet_dir = self.manifests.config.get("kubelet_dir", "/var/lib/kubelet")

        for c in obj.spec.template.spec.containers:
            for idx in range(len(c.args)):
                if original_dn in c.args[idx] and updated_dn not in c.args[idx]:
                    c.args[idx] = c.args[idx].replace(original_dn, updated_dn)
                if "/var/lib/kubelet" in c.args[idx]:
                    c.args[idx] = c.args[idx].replace("/var/lib/kubelet", kubelet_dir)
            for m in c.volumeMounts:
                m.mountPath = m.mountPath.replace("/var/lib/kubelet", kubelet_dir)
        for v in obj.spec.template.spec.volumes:
            if v.hostPath:
                v.hostPath.path = v.hostPath.path.replace("/var/lib/kubelet", kubelet_dir)
                v.hostPath.path = v.hostPath.path.replace(original_dn, updated_dn)

    def __call__(self, obj: AnyResource) -> None:
        """Use the provisioner-replicas and enable-host-networking to update obj."""
        tolerations, legacy = self.tolerations()

        if (
            obj.kind == "Deployment"
            and obj.metadata
            and obj.metadata.name == self.PROVISIONER_NAME
        ):
            obj.spec.replicas = replica = self.manifests.config.get("provisioner-replicas")
            log.info(f"Updating deployment replicas to {replica}")

            obj.spec.template.spec.tolerations = tolerations
            log.info("Updating deployment tolerations")

            obj.spec.template.spec.hostNetwork = host_network = self.manifests.config.get(
                "enable-host-networking"
            )
            log.info(f"Updating deployment hostNetwork to {host_network}")

            log.info("Updating deployment specs")
            self.adjust_container_specs(obj)

        if obj.kind == "DaemonSet" and obj.metadata and obj.metadata.name == self.PLUGIN_NAME:
            log.info("Updating daemonset tolerations")
            obj.spec.template.spec.tolerations = (
                tolerations if not legacy else [CephToleration(operator="Exists")]
            )

            log.info("Updating daemonset specs")
            self.adjust_container_specs(obj)


class CSIDriverAdjustments(Patch):
    """Update CSI driver."""

    NAME_FORMATTER = "csidriver-name-formatter"
    REQUIRED_CONFIG = {NAME_FORMATTER}

    def __init__(self, manifests: Manifests, default_name: str):
        super().__init__(manifests)
        self.default_name = default_name

    @cached_property
    def formatted(self) -> str:
        """Rename the object."""
        formatter = self.manifests.config[self.NAME_FORMATTER]
        fmt_context = {
            "name": self.default_name,
            "app": self.manifests.model.app.name,
            "namespace": self.manifests.config["namespace"],
        }
        return formatter.format(**fmt_context)

    def __call__(self, obj: AnyResource) -> None:
        """Format the name for the CSIDriver and and update obj."""
        if not obj.metadata or not obj.metadata.name:
            log.error("Object is missing metadata or name. %s", obj)
            return

        if obj.kind == "CSIDriver":
            obj.metadata.name = self.formatted


class RemoveResource(Subtraction):
    """Remove all resources when not purging and not enabled."""

    def __call__(self, _obj: AnyResource) -> bool:
        """Remove this obj if manifest is not enabled."""
        purging = cast(SafeManifest, self.manifests).purging
        enabled = self.manifests.config["enabled"]
        if purging:
            log.info("Purging, removing resource %s", _obj)
        elif not enabled:
            log.info("Disabled, skipping resource %s", _obj)
        return not purging and not enabled
