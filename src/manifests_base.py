import logging
import pickle
from abc import ABCMeta, abstractmethod
from hashlib import md5
from typing import Any, Dict, Optional

from lightkube.codecs import AnyResource
from lightkube.core.resource import NamespacedResource
from ops.manifests import Addition, Manifests, Patch

log = logging.getLogger(__name__)


class SafeManifest(Manifests):
    purgeable: bool = False

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


class StorageClassAddition(Addition):
    """Base class for storage class additions."""

    __metaclass__ = ABCMeta

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the storage class."""
        raise NotImplementedError

    def update_parameters(self, parameters: Dict[str, str]) -> None:
        """Adjust parameters for storage class."""
        config = f"{self.name}-storage-class-parameters"
        adjustments = self.manifests.config.get(config)
        if not adjustments:
            log.info(f"No adjustments for {self.name} storage-class parameters")
            return

        for adjustment in adjustments.split(" "):
            key_value = adjustment.split("=", 1)
            if len(key_value) == 2:
                parameters[key_value[0]] = key_value[1]
            elif adjustment.endswith("-"):
                parameters.pop(adjustment[:-1], None)
            else:
                log.warning("Invalid parameter: %s in %s", adjustment, config)
