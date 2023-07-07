import pickle
from hashlib import md5
from typing import Dict, Optional

from ops.manifests import Manifests


class SafeManifest(Manifests):
    purgeable: bool = False

    @property
    def config(self) -> Dict:
        return {}

    def hash(self) -> int:
        """Calculate a hash of the current configuration."""
        return int(md5(pickle.dumps(self.config)).hexdigest(), 16)

    def evaluate(self) -> Optional[str]:
        ...
