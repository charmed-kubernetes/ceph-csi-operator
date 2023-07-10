import logging
import unittest.mock as mock
from textwrap import dedent

from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import ConfigMap

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
            "ceph.conf": dedent(
                """\
                [global]
                auth_cluster_required = 1234
                auth_service_required = 1234
                auth_client_required = 1234

                """
            ),
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
    alt_ns = "diff-ns"
    manifest.config = {
        "fsid": "abcd",
        "namespace": alt_ns,
        "mon_hosts": ["10.10.10.1", "10.10.10.2"],
    }

    expected = ConfigMap(
        metadata=ObjectMeta(
            name=ccc.NAME,
            namespace=alt_ns,
        ),
        data={"config.json": '[{"clusterID": "abcd", "monitors": ["10.10.10.1", "10.10.10.2"]}]'},
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
