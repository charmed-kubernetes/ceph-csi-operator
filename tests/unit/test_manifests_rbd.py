import logging
import unittest.mock as mock

import pytest
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Secret
from lightkube.resources.storage_v1 import StorageClass

from manifests_base import CSIDriverAdjustments
from manifests_rbd import CephRBDSecret, CephStorageClass, RBDManifests


def test_storage_secret_modelled(caplog):
    caplog.set_level(logging.INFO)
    manifest = mock.MagicMock()
    manifest.name = "rbd"
    manifest.purging = False
    manifest.config = {"enabled": False}

    ss = CephRBDSecret(manifest)
    assert ss() is None
    assert "Ignore Secret from " in caplog.text

    caplog.clear()
    manifest.config = {"enabled": True}
    assert ss() is None
    assert "rbd is missing required secret item: 'user'" in caplog.text

    caplog.clear()
    manifest.config = {"enabled": True, "user": "abcd"}
    assert ss() is None
    assert "rbd is missing required secret item: 'kubernetes_key'" in caplog.text

    caplog.clear()
    manifest.config = {"enabled": True, "user": "abcd", "kubernetes_key": "123"}
    expected = Secret(
        metadata=ObjectMeta(name=CephRBDSecret.NAME),
        stringData={
            "userID": "abcd",
            "userKey": "123",
        },
    )
    assert ss() == expected
    assert "Modelling secret data for rbd." in caplog.text


@pytest.mark.parametrize("fs_type", ["xfs", "ext4"])
def test_ceph_storage_class_modelled(caplog, fs_type):
    caplog.set_level(logging.INFO)
    alt_ns = "diff-ns"
    manifest = mock.MagicMock()
    manifest.csidriver = CSIDriverAdjustments(manifest, RBDManifests.DRIVER_NAME)
    manifest.purging = False
    manifest.config = {
        "csidriver-name-formatter": "{name}",
        "namespace": alt_ns,
    }
    csc = CephStorageClass(manifest, f"ceph-{fs_type}")

    assert csc() is None
    assert (
        f"Skipping Ceph {fs_type.capitalize()} storage class creation since it's disabled"
        in caplog.text
    )

    manifest.config["enabled"] = True
    caplog.clear()
    assert csc() is None
    assert f"Ceph {fs_type.capitalize()} is missing required storage item: 'fsid'" in caplog.text

    caplog.clear()
    sc_name = f"ceph-{fs_type}"
    sc_params = f"ceph-{fs_type}-storage-class-parameters"
    manifest.config.update(
        {
            "fsid": "abcd",
            "default-storage": f"ceph-{fs_type}",
            f"ceph-{fs_type}-storage-class-name-formatter": sc_name,
            sc_params: (
                "missing-key- "  # removes the missing-key key
                "invalid-key "  # errors on the invalid-key key
                "extra-parameter=value"  # adds the extra-parameter key
            ),
        }
    )
    with pytest.raises(ValueError):
        csc()
    assert f"Invalid storage-class-parameter: invalid-key in {sc_params}" in caplog.text

    manifest.config[sc_params] = (
        "missing-key- "  # removes the missing-key key
        "extra-parameter=value"  # adds the extra-parameter key
    )
    expected = StorageClass(
        metadata=ObjectMeta(
            name=sc_name,
            annotations={"storageclass.kubernetes.io/is-default-class": "true"},
        ),
        provisioner=RBDManifests.DRIVER_NAME,
        parameters={
            "clusterID": "abcd",
            "csi.storage.k8s.io/controller-expand-secret-name": CephRBDSecret.NAME,
            "csi.storage.k8s.io/controller-expand-secret-namespace": alt_ns,
            "csi.storage.k8s.io/fstype": fs_type,
            "csi.storage.k8s.io/node-stage-secret-name": CephRBDSecret.NAME,
            "csi.storage.k8s.io/node-stage-secret-namespace": alt_ns,
            "csi.storage.k8s.io/provisioner-secret-name": CephRBDSecret.NAME,
            "csi.storage.k8s.io/provisioner-secret-namespace": alt_ns,
            "pool": f"{fs_type}-pool",
            "extra-parameter": "value",
        },
        allowVolumeExpansion=True,
        mountOptions=["discard"],
        reclaimPolicy="Delete",
    )
    assert csc() == expected
    assert f"Modelling storage class {sc_name}" in caplog.text


@pytest.mark.parametrize("fs_type", ["xfs", "ext4"])
def test_ceph_storage_class_purging(caplog, fs_type):
    caplog.set_level(logging.INFO)
    manifest = mock.MagicMock()
    manifest.csidriver = CSIDriverAdjustments(manifest, RBDManifests.DRIVER_NAME)
    manifest.purging = True
    manifest.config = {
        "csidriver-name-formatter": "{name}",
        "namespace": "purge-ns",
    }
    csc = CephStorageClass(manifest, f"ceph-{fs_type}")

    caplog.clear()
    expected = StorageClass(
        metadata=ObjectMeta(),
        provisioner=RBDManifests.DRIVER_NAME,
    )
    assert csc() == expected


def test_manifest_evaluation(caplog):
    caplog.set_level(logging.INFO)
    charm = mock.MagicMock()
    manifests = RBDManifests(charm)
    assert manifests.evaluate() is None
    assert "Skipping CephRBD evaluation since it's disabled" in caplog.text

    charm.config = {"ceph-rbd-enable": True}
    assert (
        manifests.evaluate()
        == "RBD manifests require the definition of 'ceph-rbac-name-formatter'"
    )
    charm.config.update(
        {"user": "cephx", "ceph-rbac-name-formatter": "{name}", "kubernetes_key": "123"}
    )

    assert manifests.evaluate() == "RBD manifests require the definition of 'fsid'"

    charm.config["fsid"] = "cluster"
    err_formatter = "RBD manifests failed to create storage classes: Missing storage class name {}"

    assert manifests.evaluate() == err_formatter.format("ceph-xfs-storage-class-name-formatter")

    charm.config["ceph-xfs-storage-class-name-formatter"] = "ceph-xfs"
    assert manifests.evaluate() == err_formatter.format("ceph-ext4-storage-class-name-formatter")

    charm.config["ceph-ext4-storage-class-name-formatter"] = "ceph-ext4"
    assert manifests.evaluate() is None

    charm.config["ceph-rbd-tolerations"] = "key=value,Foo"
    assert (
        manifests.evaluate()
        == "Cannot adjust CephRBD Pods: Invalid tolerations: Invalid operator='Foo'"
    )

    charm.config["ceph-rbd-tolerations"] = "key=value,Exists,Foo"
    assert (
        manifests.evaluate()
        == "Cannot adjust CephRBD Pods: Invalid tolerations: Invalid effect='Foo'"
    )

    charm.config["ceph-rbd-tolerations"] = "key=value,Exists,NoSchedule,Foo"
    assert (
        manifests.evaluate()
        == "Cannot adjust CephRBD Pods: Invalid tolerations: Too many effects='NoSchedule,Foo'"
    )
