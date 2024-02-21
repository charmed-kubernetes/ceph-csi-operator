import pickle
from hashlib import md5
from typing import Any, Dict, Optional

from lightkube.codecs import AnyResource
from lightkube.core.resource import NamespacedResource
from ops.manifests import Manifests, Patch


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
