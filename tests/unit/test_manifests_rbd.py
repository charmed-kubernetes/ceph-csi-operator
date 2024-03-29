import logging
import unittest.mock as mock

import pytest
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Secret
from lightkube.resources.storage_v1 import StorageClass

from manifests_rbd import CephStorageClass, RBDManifests, StorageSecret


def test_storage_secret_modeled(caplog):
    caplog.set_level(logging.INFO)
    manifest = mock.MagicMock()
    ss = StorageSecret(manifest)
    manifest.config = {}
    assert ss() is None
    assert "RBD is missing required secret item: 'user'" in caplog.text

    caplog.clear()
    manifest.config = {"user": "abcd"}
    assert ss() is None
    assert "RBD is missing required secret item: 'kubernetes_key'" in caplog.text

    caplog.clear()
    manifest.config = {"user": "abcd", "kubernetes_key": "123"}
    expected = Secret(
        metadata=ObjectMeta(name=StorageSecret.SECRET_NAME),
        stringData={
            "userID": "abcd",
            "userKey": "123",
        },
    )
    assert ss() == expected
    assert "Modelling secret data for rbd storage." in caplog.text


@pytest.mark.parametrize("fs_type", ["xfs", "ext4"])
def test_ceph_storage_class_modeled(caplog, fs_type):
    caplog.set_level(logging.INFO)
    manifest = mock.MagicMock()
    manifest.config = {}
    csc = CephStorageClass(manifest, fs_type)

    assert csc() is None
    assert f"Ceph {fs_type.capitalize()} is missing required storage item: 'fsid'" in caplog.text

    caplog.clear()
    alt_ns = "diff-ns"
    manifest.config = {
        "fsid": "abcd",
        "namespace": alt_ns,
        "default-storage": f"ceph-{fs_type}",
    }

    expected = StorageClass(
        metadata=ObjectMeta(
            name=csc.fs_name,
            annotations={"storageclass.kubernetes.io/is-default-class": "true"},
        ),
        provisioner=CephStorageClass.PROVISIONER,
        parameters={
            "clusterID": "abcd",
            "csi.storage.k8s.io/controller-expand-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/controller-expand-secret-namespace": alt_ns,
            "csi.storage.k8s.io/fstype": csc.fs_type,
            "csi.storage.k8s.io/node-stage-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/node-stage-secret-namespace": alt_ns,
            "csi.storage.k8s.io/provisioner-secret-name": StorageSecret.SECRET_NAME,
            "csi.storage.k8s.io/provisioner-secret-namespace": alt_ns,
            "imageFeatures": "layering",
            "pool": f"{csc.fs_type}-pool",
            "thickProvision": "false",
        },
        allowVolumeExpansion=True,
        mountOptions=["discard"],
        reclaimPolicy="Delete",
    )
    assert csc() == expected
    assert f"Modelling storage class {csc.fs_name}" in caplog.text


def test_manifest_evaluation(caplog):
    caplog.set_level(logging.INFO)
    charm = mock.MagicMock()
    manifests = RBDManifests(charm)
    assert manifests.evaluate() == "RBD manifests require the definition of 'fsid'"

    charm.config = {"user": "cephx", "fsid": "cluster", "kubernetes_key": "123"}
    assert manifests.evaluate() is None
