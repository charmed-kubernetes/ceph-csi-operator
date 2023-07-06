# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
"""Implementation of rbd specific details of the kubernetes manifests."""

import configparser
import contextlib
import io
import json
import logging
from typing import Dict, Optional

from lightkube.codecs import AnyResource
from lightkube.resources.core_v1 import ConfigMap
from ops.manifests import Addition, ManifestLabel

from charm import CephCsiCharm, SafeManifest

log = logging.getLogger(__name__)
DEFAULT_NAMESPACE = "default"


class CreateCephConfig(Addition):
    """Create configmap for the ceph-conf."""

    NAME = "ceph-config"
    REQUIRED_CONFIG = {"auth"}

    def __call__(self) -> Optional[AnyResource]:
        """Craft the configMap object."""
        auth = self.manifests.config.get("auth")
        if not auth:
            log.error(f"{self.__class__.__name__}: auth is None")
            return None

        log.info(f"Create {self.NAME} ConfigMap.")
        ns = self.manifests.config.get("namespace")
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
        return ConfigMap.from_dict(dict(metadata=dict(name=self.NAME, namespace=ns), data=data))


class CreateEncryptConfig(Addition):
    """Create configmap for the ceph-csi-encryption-kms-config."""

    NAME = "ceph-csi-encryption-kms-config"
    REQUIRED_CONFIG = set()

    def __call__(self) -> Optional[AnyResource]:
        log.info(f"Create {self.NAME} ConfigMap.")
        ns = self.manifests.config.get("namespace")
        data = {"config.json": "{}"}
        return ConfigMap.from_dict(dict(metadata=dict(name=self.NAME, namespace=ns), data=data))


class CreateCephCsiConfig(Addition):
    """Create configmap for the ceph-csi-config."""

    NAME = "ceph-csi-config"
    REQUIRED_CONFIG = {"fsid", "mon_hosts"}

    def __call__(self) -> Optional[AnyResource]:
        fsid = self.manifests.config.get("fsid")
        mon_hosts = self.manifests.config.get("mon_hosts")

        if not fsid:
            log.error(f"{self.__class__.__name__}: fsid is None")
            return None

        if not mon_hosts:
            log.error(f"{self.__class__.__name__}: mon_hosts don't exist")
            return None

        log.info(f"Create {self.NAME} ConfigMap.")
        ns = self.manifests.config.get("namespace")
        config_json = [{"clusterID": fsid, "monitors": mon_hosts}]
        data = {"config.json": json.dumps(config_json)}
        return ConfigMap.from_dict(dict(metadata=dict(name=self.NAME, namespace=ns), data=data))


class ConfigManifests(SafeManifest):
    """Deployment Specific details for the aws-ebs-csi-driver."""

    def __init__(self, charm: CephCsiCharm):
        super().__init__(
            "config",
            charm.model,
            "/tmp/",  # there's no real manifest path
            [
                CreateCephConfig(self),
                CreateEncryptConfig(self),
                CreateCephCsiConfig(self),
                ManifestLabel(self),
            ],
        )
        self.charm = charm

    @property
    def config(self) -> Dict:
        """Returns current config available from charm config and joined relations."""
        config: Dict = {}
        config["namespace"] = DEFAULT_NAMESPACE

        config.update(**self.charm.ceph_context)
        config.update(**self.charm.config)

        for key, value in dict(**config).items():
            if value == "" or value is None:
                del config[key]

        # always selects a release where no manifest path exists
        config["release"] = True
        return config

    def evaluate(self) -> Optional[str]:
        """Determine if manifest_config can be applied to manifests."""
        props = CreateCephConfig.REQUIRED_CONFIG | CreateCephCsiConfig.REQUIRED_CONFIG
        for prop in props:
            value = self.config.get(prop)
            if not value:
                return f"Config manifests waiting for definition of {prop}"
        return None
