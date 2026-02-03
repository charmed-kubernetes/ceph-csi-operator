import json
import logging
import unittest.mock as mock
from textwrap import dedent

import pytest
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import ConfigMap

import literals
from manifests_config import CephConfig, CephCsiConfig, ConfigManifests


def test_ceph_config_modeled(caplog):
    caplog.set_level(logging.INFO)
    manifest = mock.MagicMock()
    cc = CephConfig(manifest)
    manifest.config = {}
    assert cc() is None
    assert "CephConfig is missing required item: 'auth'" in caplog.text

    caplog.clear()
    manifest.config = {"auth": "1234"}
    expected = ConfigMap(
        metadata=ObjectMeta(name=CephConfig.NAME),
        data={
            "ceph.conf": dedent("""\
                [global]
                auth_cluster_required = 1234
                auth_service_required = 1234
                auth_client_required = 1234

                """),
            "keyring": "",
        },
    )
    assert cc() == expected
    assert "Modelling configmap for ceph-config." in caplog.text


def test_ceph_csi_config_modeled(caplog):
    caplog.set_level(logging.INFO)
    manifest = mock.MagicMock()
    manifest.config = {}
    ccc = CephCsiConfig(manifest)

    assert ccc() is None
    assert f"{CephCsiConfig.NAME} is missing required config item: 'fsid'" in caplog.text

    caplog.clear()
    manifest.config = {
        "fsid": "abcd",
        "mon_hosts": ["10.10.10.1", "10.10.10.2"],
    }
    expected_config_json = [
        {
            "clusterID": manifest.config["fsid"],
            "monitors": manifest.config["mon_hosts"],
            "CephFS": {"subvolumeGroup": literals.CEPHFS_SUBVOLUMEGROUP},
        }
    ]

    expected = ConfigMap(
        metadata=ObjectMeta(
            name=ccc.NAME,
        ),
        data={
            "config.json": json.dumps(expected_config_json, sort_keys=True),
        },
    )
    assert ccc() == expected
    assert "Modelling configmap for ceph-csi-config." in caplog.text


def test_manifest_evaluation(caplog):
    caplog.set_level(logging.INFO)
    charm = mock.MagicMock()
    manifests = ConfigManifests(charm)
    assert manifests.evaluate() == "Config manifests require the definition of 'auth'"

    charm.config = {"auth": "1234", "mon_hosts": ["10.10.10.1", "10.10.10.2"], "fsid": "cluster"}
    assert manifests.evaluate() is None


@pytest.mark.parametrize(
    "config",
    [
        "metrics-port-cephfsplugin",
        "metrics-port-cephfsplugin-provisioner",
        "metrics-port-rbdplugin",
        "metrics-port-rbdplugin-provisioner",
    ],
)
def test_metrics_ports_config(caplog, config):
    caplog.set_level(logging.INFO)
    charm = mock.MagicMock()
    manifests = ConfigManifests(charm)
    charm.config = {"auth": "1234", "mon_hosts": ["10.10.10.1", "10.10.10.2"], "fsid": "cluster"}

    charm.config[config] = -1
    assert manifests.evaluate() is None

    charm.config[config] = 5000
    assert manifests.evaluate() is None

    charm.config[config] = 100
    assert (
        manifests.evaluate()
        == f"Invalid metrics port: Invalid value for {config}: 100. Must be between 1024 and 65535"
    )


def test_metrics_ports_config_duplicates(caplog):
    caplog.set_level(logging.INFO)
    charm = mock.MagicMock()
    manifests = ConfigManifests(charm)
    charm.config = {"auth": "1234", "mon_hosts": ["10.10.10.1", "10.10.10.2"], "fsid": "cluster"}

    charm.config["metrics-port-cephfsplugin"] = 5000
    charm.config["metrics-port-cephfsplugin-provisioner"] = 5000
    assert (
        manifests.evaluate()
        == "Invalid metrics port: Value for metrics-port-cephfsplugin-provisioner: 5000 conflicts with metrics-port-cephfsplugin"
    )
