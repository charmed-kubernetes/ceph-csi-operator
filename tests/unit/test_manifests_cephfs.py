import logging
import unittest.mock as mock

from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Secret
from lightkube.resources.storage_v1 import StorageClass

from manifests_cephfs import CephFSManifests, CephStorageClass, StorageSecret


def test_storage_secret_modeled(caplog):
    caplog.set_level(logging.INFO)
    manifest = mock.MagicMock()
    ss = StorageSecret(manifest)
    manifest.config = {"enabled": False}
    assert ss() is None
    assert "Ignore Cephfs Storage Secret" in caplog.text

    caplog.clear()
    manifest.config = {"enabled": True}
    assert ss() is None
    assert "Cephfs is missing required secret item: 'user'" in caplog.text

    caplog.clear()
    manifest.config = {"enabled": True, "user": "abcd"}
    assert ss() is None
    assert "Cephfs is missing required secret item: 'kubernetes_key'" in caplog.text

    caplog.clear()
    manifest.config = {"enabled": True, "user": "abcd", "kubernetes_key": "123"}
    expected = Secret(
        metadata=ObjectMeta(name=StorageSecret.SECRET_NAME),
        stringData={
            "userID": "abcd",
            "adminID": "abcd",
            "userKey": "123",
            "adminKey": "123",
        },
    )
    assert ss() == expected
    assert "Modelling secret data for cephfs storage." in caplog.text


def test_ceph_storage_class_modeled(caplog):
    caplog.set_level(logging.INFO)
    manifest = mock.MagicMock()
    csc = CephStorageClass(manifest)

    manifest.config = {"enabled": False}
    assert csc() is None
    assert "Ignore CephFS Storage Class" in caplog.text

    caplog.clear()
    manifest.config = {"enabled": True}
    assert csc() is None
    assert "CephFS is missing required storage item: 'clusterID'" in caplog.text

    caplog.clear()
    alt_ns = "diff-ns"
    manifest.config = {
        "enabled": True,
        "fsid": "abcd",
        "namespace": alt_ns,
        "default-storage": CephStorageClass.STORAGE_NAME,
        "cephfs-mounter": "fuse",
    }

    expected = StorageClass(
        metadata=ObjectMeta(
            name=CephStorageClass.STORAGE_NAME,
            annotations={"storageclass.kubernetes.io/is-default-class": "true"},
        ),
        provisioner=CephStorageClass.PROVISIONER,
        parameters={
            "clusterID": "abcd",
            "csi.storage.k8s.io/controller-expand-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/controller-expand-secret-namespace": alt_ns,
            "csi.storage.k8s.io/provisioner-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/provisioner-secret-namespace": alt_ns,
            "csi.storage.k8s.io/node-stage-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/node-stage-secret-namespace": alt_ns,
            "fsName": "default",
            "mounter": "fuse",
            "pool": "ceph-fs-pool",
        },
        allowVolumeExpansion=True,
        mountOptions=["debug"],
        reclaimPolicy="Delete",
    )
    assert csc() == expected
    assert "CephFS Storage Class using default fsName" in caplog.text
    assert f"Modelling storage class {CephStorageClass.STORAGE_NAME}" in caplog.text


def test_manifest_evaluation(caplog):
    caplog.set_level(logging.INFO)
    charm = mock.MagicMock()
    manifests = CephFSManifests(charm)
    assert manifests.evaluate() is None
    assert "Skipping CephFS evaluation since it's disabled" in caplog.text

    charm.config = {"cephfs-enable": True}
    assert manifests.evaluate() == "CephFS manifests require the definition of 'fsid'"

    charm.config = {
        "cephfs-enable": True,
        "user": "cephx",
        "fsid": "cluster",
        "kubernetes_key": "123",
    }
    assert manifests.evaluate() is None
