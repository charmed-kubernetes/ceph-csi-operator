import pickle
from hashlib import md5
from typing import Any, Dict, Optional

from ops.manifests import Manifests


class SafeManifest(Manifests):
    purgeable: bool = False

    def hash(self) -> int:
        """Calculate a hash of the current configuration."""
        return int(md5(pickle.dumps(self.config)).hexdigest(), 16)

    @property
    def config(self) -> Dict[str, Any]:
        return {}  # pragma: no cover

    def evaluate(self) -> Optional[str]:
        ...  # pragma: no cover
