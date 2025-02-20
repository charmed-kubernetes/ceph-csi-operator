import logging
import unittest.mock as mock
from pathlib import Path

import yaml
from lightkube.models.apps_v1 import DaemonSet, Deployment
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Secret
from lightkube.resources.storage_v1 import StorageClass

from manifests_cephfs import (
    CephFilesystem,
    CephFSManifests,
    CephStorageClass,
    ProvisionerAdjustments,
    StorageSecret,
)

TEST_CEPH_FS = CephFilesystem(
    name="ceph-fs",
    metadata_pool="ceph-fs_metadata",
    metadata_pool_id=1,
    data_pool_ids=[2],
    data_pools=["ceph-fs_data"],
)

TEST_CEPH_FS_ALT = CephFilesystem(
    name="ceph-fs-alt",
    metadata_pool="ceph-fs-alt_metadata",
    metadata_pool_id=1,
    data_pool_ids=[2],
    data_pools=["ceph-fs-alt_data"],
)

upstream_path = Path(__file__).parent.parent.parent / "upstream"
cephfs_path = upstream_path / "cephfs"
current_path = cephfs_path / "manifests" / (cephfs_path / "version").read_text().strip()
provisioner_path = current_path / (ProvisionerAdjustments.PROVISIONER_NAME + ".yaml")
plugin_path = current_path / (ProvisionerAdjustments.PLUGIN_NAME + ".yaml")


def test_storage_secret_modelled(caplog):
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


def test_ceph_provisioner_adjustment_modelled(caplog):
    caplog.set_level(logging.INFO)
    manifest = mock.MagicMock()
    alt_path = "/var/lib/kubelet-alt"
    manifest.config = {
        "provisioner-replicas": 3,
        "enable-host-networking": False,
        "kubelet_dir": alt_path,
    }
    cpa = ProvisionerAdjustments(manifest)
    resources = list(yaml.safe_load_all(provisioner_path.read_text()))
    resource = Deployment.from_dict(resources[1])
    assert cpa(resource) is None
    assert "Updating deployment replicas to 3" in caplog.text
    assert "Updating deployment hostNetwork to False" in caplog.text
    caplog.clear()

    resources = list(yaml.safe_load_all(plugin_path.read_text()))
    resource = DaemonSet.from_dict(resources[0])
    assert cpa(resource) is None
    assert alt_path in resource.spec.template.spec.containers[1].args[2]
    assert alt_path in resource.spec.template.spec.containers[0].volumeMounts[1].mountPath
    assert "Updating daemonset tolerations" in caplog.text
    assert "Updating daemonset kubeletDir to /var/lib/kubelet-alt" in caplog.text


def test_ceph_storage_class_modelled(caplog):
    caplog.set_level(logging.INFO)
    manifest = mock.MagicMock()
    manifest.purging = False
    csc = CephStorageClass(manifest)
    alt_ns = "diff-ns"

    manifest.config = {
        "fs_list": [TEST_CEPH_FS_ALT],
        "namespace": alt_ns,
        "default-storage": TEST_CEPH_FS_ALT.name,
        "cephfs-mounter": "fuse",
        "cephfs-storage-class-name-formatter": "{name}",
    }

    assert csc() == []
    assert "Skipping CephFS storage class creation since it's disabled" in caplog.text

    manifest.config["enabled"] = True
    caplog.clear()
    assert csc() == []
    assert "CephFS is missing a filesystem: 'fsid'" in caplog.text

    manifest.config["fsid"] = "abcd"
    caplog.clear()
    expected = StorageClass(
        metadata=ObjectMeta(
            name=TEST_CEPH_FS_ALT.name,
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
            "fsName": "ceph-fs-alt",
            "mounter": "fuse",
            "pool": "ceph-fs-alt_data",
        },
        allowVolumeExpansion=True,
        reclaimPolicy="Delete",
    )
    assert csc() == [expected]
    assert f"Modelling storage class sc='{TEST_CEPH_FS_ALT.name}'" in caplog.text


def test_ceph_storage_class_purging(caplog):
    caplog.set_level(logging.INFO)
    manifest = mock.MagicMock()
    manifest.purging = True
    csc = CephStorageClass(manifest)

    caplog.clear()
    expected = StorageClass(
        metadata=ObjectMeta(),
        provisioner=CephStorageClass.PROVISIONER,
    )
    assert csc() == [expected]


def test_manifest_evaluation(caplog):
    caplog.set_level(logging.INFO)
    charm = mock.MagicMock()
    manifests = CephFSManifests(charm)
    sc_name_formatter_key = "cephfs-storage-class-name-formatter"
    assert manifests.evaluate() is None
    assert "Skipping CephFS evaluation since it's disabled" in caplog.text

    charm.config = {"cephfs-enable": True}
    assert (
        manifests.evaluate()
        == "CephFS manifests require the definition of 'ceph-rbac-name-formatter'"
    )

    charm.config["ceph-rbac-name-formatter"] = "{name}"
    assert manifests.evaluate() == "CephFS manifests require the definition of 'kubernetes_key'"

    charm.config["kubernetes_key"] = "123"
    assert manifests.evaluate() == "CephFS manifests require the definition of 'user'"

    charm.config["user"] = "cephx"
    err_formatter = "CephFS manifests failed to create storage classes: {}"
    assert manifests.evaluate() == err_formatter.format("missing fsid")

    charm.config["fsid"] = "cluster"
    assert manifests.evaluate() == err_formatter.format("missing filesystem listing")

    charm.config[CephStorageClass.FILESYSTEM_LISTING] = [TEST_CEPH_FS]
    assert manifests.evaluate() == err_formatter.format(
        "Missing storage class name " + sc_name_formatter_key
    )

    charm.config[CephStorageClass.FILESYSTEM_LISTING] = [TEST_CEPH_FS, TEST_CEPH_FS_ALT]
    charm.config[sc_name_formatter_key] = CephStorageClass.STORAGE_TYPE
    assert manifests.evaluate() == err_formatter.format(
        sc_name_formatter_key + " does not generate unique names"
    )

    charm.config[sc_name_formatter_key] = "cephfs-{name}"
    assert manifests.evaluate() is None

    charm.config["cephfs-tolerations"] = "key=value,Foo"
    assert (
        manifests.evaluate()
        == "Cannot adjust CephFS Pods: Invalid tolerations: Invalid operator='Foo'"
    )

    charm.config["cephfs-tolerations"] = "key=value,Exists,Foo"
    assert (
        manifests.evaluate()
        == "Cannot adjust CephFS Pods: Invalid tolerations: Invalid effect='Foo'"
    )

    charm.config["cephfs-tolerations"] = "key=value,Exists,NoSchedule,Foo"
    assert (
        manifests.evaluate()
        == "Cannot adjust CephFS Pods: Invalid tolerations: Too many effects='NoSchedule,Foo'"
    )
