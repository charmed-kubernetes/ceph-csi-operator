# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
"""Implementation of rbd specific details of the kubernetes manifests."""

import configparser
import contextlib
import io
import json
import logging
from typing import TYPE_CHECKING, Dict, Optional, cast

from lightkube.codecs import AnyResource
from lightkube.resources.core_v1 import ConfigMap
from ops.manifests import Addition, CreateNamespace, ManifestLabel

from manifests_base import AdjustNamespace, SafeManifest

if TYPE_CHECKING:
    from charm import CephCsiCharm

log = logging.getLogger(__name__)


class CephConfig(Addition):
    """Create configmap for the ceph-conf."""

    NAME = "ceph-config"
    REQUIRED_CONFIG = {"auth"}

    def __call__(self) -> Optional[AnyResource]:
        """Craft the configMap object."""
        auth = self.manifests.config.get("auth")
        if not auth:
            log.error(f"{self.__class__.__name__} is missing required item: 'auth'")
            return None

        log.info(f"Modelling configmap for {self.NAME}.")
        config = configparser.ConfigParser()
        config["global"] = {
            "auth_cluster_required": auth,
            "auth_service_required": auth,
            "auth_client_required": auth,
        }

        with contextlib.closing(io.StringIO()) as sio:
            config.write(sio)
            output_text = sio.getvalue()

        data = {"ceph.conf": output_text, "keyring": ""}
        return ConfigMap.from_dict(dict(metadata=dict(name=self.NAME), data=data))


class EncryptConfig(Addition):
    """Create configmap for the ceph-csi-encryption-kms-config."""

    NAME = "ceph-csi-encryption-kms-config"
    REQUIRED_CONFIG = set()

    def __call__(self) -> Optional[AnyResource]:
        log.info(f"Craft {self.NAME} ConfigMap.")
        data = {"config.json": "{}"}
        return ConfigMap.from_dict(dict(metadata=dict(name=self.NAME), data=data))


class CephCsiConfig(Addition):
    """Create configmap for the ceph-csi-config."""

    NAME = "ceph-csi-config"
    REQUIRED_CONFIG = {"fsid", "mon_hosts"}

    def __call__(self) -> Optional[AnyResource]:
        fsid = self.manifests.config.get("fsid")
        mon_hosts = self.manifests.config.get("mon_hosts")

        if not fsid:
            log.error(f"{self.NAME} is missing required config item: 'fsid'")
            return None

        if not mon_hosts:
            log.error(f"{self.NAME} is missing required config item: 'mon_hosts'")
            return None

        log.info(f"Modelling configmap for {self.NAME}.")
        config_json = [{"clusterID": fsid, "monitors": mon_hosts}]
        data = {"config.json": json.dumps(config_json)}
        return ConfigMap.from_dict(dict(metadata=dict(name=self.NAME), data=data))


class ConfigManifests(SafeManifest):
    """Deployment Specific details for the aws-ebs-csi-driver."""

    def __init__(self, charm: "CephCsiCharm"):
        self.ns = cast(str, charm.stored.namespace)
        super().__init__(
            "config",
            charm.model,
            "upstream/config",
            [
                CreateNamespace(self, self.ns),
                CephConfig(self),
                EncryptConfig(self),
                CephCsiConfig(self),
                ManifestLabel(self),
                AdjustNamespace(self),
            ],
        )
        self.charm = charm

    @property
    def config(self) -> Dict:
        """Returns current config available from charm config and joined relations."""
        config: Dict = {}
        config.update(**self.charm.ceph_context)
        config.update(**self.charm.config)

        for key, value in dict(**config).items():
            if value == "" or value is None:
                del config[key]

        # always selects a release where no manifest path exists
        config["release"] = "v0"
        config["namespace"] = self.ns
        return config

    def evaluate(self) -> Optional[str]:
        """Determine if manifest_config can be applied to manifests."""
        props = CephConfig.REQUIRED_CONFIG | CephCsiConfig.REQUIRED_CONFIG
        for prop in sorted(props):
            value = self.config.get(prop)
            if not value:
                return f"Config manifests require the definition of '{prop}'"
        return None
