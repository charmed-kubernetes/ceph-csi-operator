# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing
"""Collection of tests related to src/charm.py"""

import contextlib
import unittest.mock as mock

import charms.operator_libs_linux.v0.apt as apt
import ops
import pytest
from lightkube.core.exceptions import ApiError
from lightkube.resources.storage_v1 import StorageClass
from ops.manifests import HashableResource, ManifestClientError, ManifestLabel, ResourceAnalysis
from ops.testing import Harness

import literals
from charm import CephCsiCharm
from manifests_config import EncryptConfig


@pytest.fixture
def harness():
    harness = Harness(CephCsiCharm)
    try:
        yield harness
    finally:
        harness.cleanup()


@pytest.fixture(autouse=True)
def mock_apt():
    with mock.patch(
        "charms.operator_libs_linux.v0.apt.DebianPackage.from_system", autospec=True
    ) as mock_apt:
        yield mock_apt


@mock.patch("subprocess.check_output")
@mock.patch("charm.CephCsiCharm.ceph_data", new_callable=mock.PropertyMock)
def test_ceph_context_getter(ceph_data, check_output, harness, ceph_conf_file):
    """Test that ceph_context property returns properly formatted data."""
    fsid = "12345"
    key = "secret_key"
    monitors = ["10.0.0.1", "10.0.0.2"]

    # stored data from ceph-mon:client relation
    harness.begin()
    ceph_data.return_value = {
        "auth": None,
        "key": key,
        "mon_hosts": monitors,
    }
    check_output.side_effect = (fsid.encode("UTF-8"), "{}".encode("UTF-8"))

    # key and value format expected in context for Kubernetes templates.
    expected_context = {
        "auth": None,
        "fsid": fsid,
        "kubernetes_key": key,
        "mon_hosts": monitors,
        "user": "ceph-csi",
        "provisioner_replicas": 3,
        "enable_host_network": "false",
        "fs_list": [],
    }

    assert harness.charm.ceph_context == expected_context
    conf = ceph_conf_file.absolute().as_posix()
    check_output.assert_any_call(
        ["/usr/bin/ceph", "--conf", conf, "--user", "ceph-csi", "fsid"], timeout=60
    )
    check_output.assert_any_call(
        ["/usr/bin/ceph", "--conf", conf, "--user", "ceph-csi", "--format", "json", "fs", "ls"],
        timeout=60,
    )


@contextlib.contextmanager
def reconcile_this(harness, method):
    previous = harness.charm.reconciler.reconcile_function
    harness.charm.reconciler.reconcile_function = method
    yield
    harness.charm.reconciler.reconcile_function = previous


@contextlib.contextmanager
def mocked_handlers():
    handler_names = [
        "_destroying",
        "install_ceph_packages",
        "check_kube_config",
        "check_namespace",
        "check_ceph_client",
        "_cephfs_enabled",
        "_ceph_rbd_enabled",
        "evaluate_manifests",
        "prevent_collisions",
        "install_manifests",
        "_update_status",
    ]

    handlers = [mock.patch(f"charm.CephCsiCharm.{name}") for name in handler_names]
    yield dict(zip(handler_names, (h.start() for h in handlers)))
    for handler in handlers:
        handler.stop()


def test_set_leader(harness):
    """Test emitting the set_leader hook while not reconciled.

    Args:
        harness: the harness under test
    """
    harness.begin()
    harness.charm.reconciler.stored.reconciled = False  # Pretended to not be reconciled
    with mocked_handlers() as handlers:
        handlers["_destroying"].return_value = False
        harness.set_leader(True)
    assert harness.charm.unit.status.name == "active"
    assert harness.charm.unit.status.message == "Ready"
    assert harness.charm.reconciler.stored.reconciled
    not_called = {name: h for name, h in handlers.items() if not h.called}
    assert not_called == {}


def test_install(harness, mock_apt):
    """Test that on.install hook will call expected methods"""
    harness.begin()
    with reconcile_this(harness, harness.charm.install_ceph_packages):
        harness.charm.on.install.emit()

    mock_apt.assert_called_once_with("ceph-common")
    mock_apt.return_value.ensure.assert_called_once_with(apt.PackageState.Latest)


def test_install_pkg_missing(harness, mock_apt):
    """Test that on.install hook will call expected methods"""
    mock_apt.side_effect = apt.PackageNotFoundError

    harness.begin()
    with reconcile_this(harness, harness.charm.install_ceph_packages):
        harness.charm.on.install.emit()

    mock_apt.assert_called_once_with("ceph-common")
    assert harness.charm.unit.status.name == "blocked"
    assert harness.charm.unit.status.message == "Failed to install ceph apt packages."


@mock.patch("charm.KubeConfig.from_env", mock.MagicMock(side_effect=FileNotFoundError))
def test_check_kubeconfig(harness):
    """Test that check_kube_config method returns True if kubeconfig is available."""
    harness.begin()
    with reconcile_this(harness, lambda _: harness.charm.check_kube_config()):
        harness.charm.on.install.emit()

    assert harness.charm.unit.status.name == "waiting"
    assert harness.charm.unit.status.message == "Waiting for kubeconfig"


def test_check_namespace(harness, lk_charm_client):
    harness.begin()

    api_error = ApiError(response=mock.MagicMock())

    # should be blocked if the ns doesn't exist
    api_error.status.message = "not found"
    lk_charm_client.get.side_effect = api_error

    with reconcile_this(harness, lambda _: harness.charm.check_namespace()):
        harness.charm.on.install.emit()
    assert harness.charm.unit.status.name == "blocked"
    assert harness.charm.unit.status.message == "Missing namespace 'default'"

    # should be in waiting if the k8s api isn't ready
    api_error.status.message = "something else happened"
    lk_charm_client.get.side_effect = api_error
    with reconcile_this(harness, lambda _: harness.charm.check_namespace()):
        harness.charm.on.install.emit()
    assert harness.charm.unit.status.name == "waiting"

    # should be true if no api errors
    lk_charm_client.get.side_effect = None
    with reconcile_this(harness, lambda _: harness.charm.check_namespace()):
        harness.charm.on.install.emit()
    assert harness.charm.unit.status.name == "active"


def test_check_ceph_client(harness):
    """Test that check_ceph_client sets expected unit states."""

    # Add ceph-mon relation, indicating existing relations
    harness.begin()

    with reconcile_this(harness, lambda _: harness.charm.check_ceph_client()):
        harness.charm.on.install.emit()

    assert harness.charm.unit.status.name == "blocked"
    assert harness.charm.unit.status.message == "Missing relation: ceph-client"

    with reconcile_this(harness, lambda _: harness.charm.check_ceph_client()):
        rel_id = harness.add_relation(literals.CEPH_CLIENT_RELATION, "ceph-mon")

    assert harness.charm.unit.status.name == "waiting"
    assert harness.charm.unit.status.message == "Ceph relation is missing data."

    data = {"auth": "some-auth", "key": "1234", "ceph-public-address": "10.0.0.1"}
    with reconcile_this(harness, lambda _: harness.charm.check_ceph_client()):
        harness.add_relation_unit(rel_id, "ceph-mon/0")
        harness.update_relation_data(rel_id, "ceph-mon/0", data)
    assert harness.charm.unit.status.name == "active"
    assert harness.charm.ceph_data == {
        "auth": "some-auth",
        "key": "1234",
        "mon_hosts": ["10.0.0.1"],
    }


@mock.patch("charm.CephCsiCharm.ceph_context", new_callable=mock.PropertyMock)
def test_evaluate_manifests_blocks(ceph_context, harness):
    """Test that evaluate_manifests sets expected unit states."""
    harness.begin()
    ceph_context.return_value = {"fsid": "12345", "kubernetes_key": "123"}
    with reconcile_this(harness, lambda _: harness.charm.evaluate_manifests()):
        harness.charm.on.install.emit()

    assert harness.charm.unit.status.name == "blocked"
    assert harness.charm.unit.status.message == "Config manifests require the definition of 'auth'"


@mock.patch("charm.CephCsiCharm.ceph_context", new_callable=mock.PropertyMock)
def test_evaluate_manifests_ready(ceph_context, harness):
    """Test that evaluate_manifests sets expected unit states."""
    harness.begin()
    ceph_context.return_value = {
        "auth": "cephx",
        "fsid": "12345",
        "kubernetes_key": "123",
        "mon_hosts": ["10.0.0.1"],
        "user": "ceph-csi",
        "provisioner_replicas": 3,
        "enable_host_network": "false",
        "fsname": None,
    }
    with reconcile_this(harness, lambda _: harness.charm.evaluate_manifests()):
        harness.charm.on.install.emit()

    assert harness.charm.unit.status.name == "active"


def test_install_manifests_unchanged(harness):
    """Test that install_manifests detects no changes."""
    harness.begin()
    config_hash = harness.charm.stored.config_hash = 1
    with reconcile_this(harness, lambda _: harness.charm.install_manifests(config_hash)):
        harness.charm.on.install.emit()

    assert harness.charm.unit.status.name == "active"


def test_install_manifests_non_leader(harness):
    """Test that install_manifests idle on non-leaders."""
    harness.begin()
    harness.charm.stored.config_hash = 1
    with reconcile_this(harness, lambda _: harness.charm.install_manifests(0)):
        harness.charm.on.install.emit()

    assert harness.charm.unit.status.name == "active"
    assert harness.charm.stored.config_hash == 0


@mock.patch("charm.CephCsiCharm.ceph_context", new_callable=mock.PropertyMock)
def test_install_manifests_leader(ceph_context, harness, lk_client):
    """Test that install_manifests operates on leaders."""
    harness.begin()
    ceph_context.return_value = {
        "auth": "cephx",
        "fsid": "12345",
        "kubernetes_key": "123",
        "mon_hosts": ["10.0.0.1"],
        "user": "ceph-csi",
        "provisioner_replicas": 3,
        "enable_host_network": "false",
        "fsname": None,
    }
    harness.charm.stored.config_hash = 0
    with reconcile_this(harness, lambda _: harness.charm.install_manifests(1)):
        harness.set_leader(True)

    assert harness.charm.unit.status.name == "active"
    assert harness.charm.stored.config_hash == 1

    lk_client.apply.side_effect = ManifestClientError("API Server Unavailable")
    with reconcile_this(harness, lambda _: harness.charm.install_manifests(2)):
        harness.set_leader(True)

    assert harness.charm.stored.config_hash == 1
    assert harness.charm.unit.status.name == "waiting"
    assert harness.charm.unit.status.message == "API Server Unavailable"


@mock.patch("interface_ceph_client.ceph_client.CephClientRequires.request_ceph_permissions")
@mock.patch("interface_ceph_client.ceph_client.CephClientRequires.create_replicated_pool")
def test_ceph_client_broker_available(create_replicated_pool, request_ceph_permissions, harness):
    """Test that when the ceph client broker is available, a pool is
    requested and Ceph capabilities are requested
    """
    # Setup charm
    harness.begin()

    # add ceph-client relation
    reconciled = mock.MagicMock()
    with reconcile_this(harness, lambda _: reconciled):
        relation_id = harness.add_relation(literals.CEPH_CLIENT_RELATION, "ceph-mon")
        harness.add_relation_unit(relation_id, "ceph-mon/0")

    assert create_replicated_pool.call_count == 2
    create_replicated_pool.assert_any_call(name="ext4-pool")
    create_replicated_pool.assert_any_call(name="xfs-pool")
    request_ceph_permissions.assert_called_once_with(
        "ceph-csi",
        [
            "mon",
            "profile rbd, allow r",
            "mds",
            "allow rw",
            "mgr",
            "allow rw",
            "osd",
            "profile rbd, allow rw tag cephfs metadata=*",
        ],
    )
    reconciled.assert_called_once


@mock.patch("charm.CephCsiCharm._purge_manifest")
def test_cleanup(_purge_manifest, harness, caplog):
    harness.set_leader(True)
    harness.begin()
    harness.charm.on.stop.emit()

    assert harness.charm.stored.config_hash == 0
    _purge_manifest.assert_called()


def test_action_list_versions(harness):
    harness.begin()

    mock_event = mock.MagicMock(spec=ops.ActionEvent)
    assert harness.charm._list_versions(mock_event) is None
    expected_results = {
        "cephfs-versions": "v3.13.0\nv3.12.3\nv3.11.0\nv3.10.2\nv3.10.1\nv3.10.0\nv3.9.0\nv3.8.1\nv3.8.0\nv3.7.2",
        "config-versions": "",
        "rbd-versions": "v3.13.0\nv3.12.3\nv3.11.0\nv3.10.2\nv3.10.1\nv3.10.0\nv3.9.0\nv3.8.1\nv3.8.0\nv3.7.2",
    }
    mock_event.set_results.assert_called_once_with(expected_results)


@mock.patch("utils.fsid", mock.MagicMock(return_value="12345"))
@mock.patch("utils.ls_ceph_fs", mock.MagicMock(return_value=[]))
def test_action_manifest_resources(harness, lk_client, api_error_klass):
    harness.begin_with_initial_hooks()
    not_found = api_error_klass()
    not_found.status.code = 404
    not_found.status.message = "Not Found"
    lk_client.get.side_effect = not_found

    expected_results = {
        "config-missing": "ConfigMap/default/ceph-csi-encryption-kms-config",
        "rbd-missing": "\n".join(
            [
                "CSIDriver/rbd.csi.ceph.com",
                "ClusterRole/rbd-csi-nodeplugin",
                "ClusterRole/rbd-external-provisioner-runner",
                "ClusterRoleBinding/rbd-csi-nodeplugin",
                "ClusterRoleBinding/rbd-csi-provisioner-role",
                "DaemonSet/default/csi-rbdplugin",
                "Deployment/default/csi-rbdplugin-provisioner",
                "Role/default/rbd-external-provisioner-cfg",
                "RoleBinding/default/rbd-csi-provisioner-role-cfg",
                "Service/default/csi-metrics-rbdplugin",
                "Service/default/csi-rbdplugin-provisioner",
                "ServiceAccount/default/rbd-csi-nodeplugin",
                "ServiceAccount/default/rbd-csi-provisioner",
                "StorageClass/ceph-ext4",
                "StorageClass/ceph-xfs",
            ]
        ),
    }
    action = harness.run_action("list-resources", {})
    assert action.results == expected_results

    action = harness.run_action("scrub-resources", {})
    assert action.results == expected_results

    action = harness.run_action("sync-resources", {})
    assert action.results == expected_results


@mock.patch("utils.fsid", mock.MagicMock(return_value="12345"))
@mock.patch("utils.ls_ceph_fs", mock.MagicMock(return_value=[]))
def test_action_sync_resources_install_failure(harness, lk_client, api_error_klass):
    harness.begin_with_initial_hooks()
    not_found = api_error_klass()
    not_found.status.code = 404
    not_found.status.message = "Not Found"
    lk_client.get.side_effect = not_found

    lk_client.apply.side_effect = ManifestClientError("API Server Unavailable")
    action = harness.run_action("sync-resources", {})

    lk_client.delete.assert_not_called()
    assert action.results["result"] == "Failed to sync missing resources: API Server Unavailable"


def test_action_delete_storage_class_unknown(harness, lk_client):
    harness.begin_with_initial_hooks()

    lk_client.delete.side_effect = ManifestClientError("API Server Unavailable")
    with pytest.raises(ops.testing.ActionFailed) as exc:
        harness.run_action("delete-storage-class", {"name": ""})

    lk_client.delete.assert_not_called()
    assert (
        str(exc.value) == "Invalid storage class name. Must be one of: cephfs, ceph-xfs, ceph-ext4"
    )


def test_action_delete_storage_class_api_error(harness, lk_client):
    harness.begin_with_initial_hooks()

    lk_client.delete.side_effect = ManifestClientError("API Server Unavailable")
    with pytest.raises(ops.testing.ActionFailed) as exc:
        harness.run_action("delete-storage-class", {"name": "cephfs"})
    lk_client.delete.assert_called_once_with(StorageClass, name="cephfs")
    assert str(exc.value) == "Failed to delete storage class: API Server Unavailable"


def test_action_delete_storage_class(harness, lk_client):
    harness.begin_with_initial_hooks()

    action = harness.run_action("delete-storage-class", {"name": "cephfs"})
    assert {"result": "Successfully deleted StorageClass/cephfs"} == action.results
    lk_client.delete.assert_called_once_with(StorageClass, name="cephfs")


def test_kubelet_dir(harness):
    harness.begin()
    data = {"kubelet-root-dir": "/var/snap/k8s/common/var/lib/kubelet"}
    with reconcile_this(harness, lambda _: None):
        rel_id = harness.add_relation("kubernetes-info", "k8s")
        harness.add_relation_unit(rel_id, "k8s/0")
        harness.update_relation_data(rel_id, "k8s", data)

    assert harness.charm.kubelet_dir == "/var/snap/k8s/common/var/lib/kubelet"


@mock.patch("charm.CephCsiCharm._purge_manifest_by_name")
@pytest.mark.parametrize("manifest", ["cephfs", "rbd"])
def test_enforce_provider_enabled(mock_purge, harness, manifest):
    harness.begin()

    if manifest == "cephfs":
        enabler = harness.charm._cephfs_enabled
        config = "cephfs-enable"
    else:
        enabler = harness.charm._ceph_rbd_enabled
        config = "ceph-rbd-enable"

    with reconcile_this(harness, lambda _: enabler()):
        # disabled on leader, purge
        harness.disable_hooks()
        harness.set_leader(True)
        harness.enable_hooks()
        harness.update_config({config: False})
        assert harness.charm.unit.status.name == "active"
        mock_purge.assert_called_once_with(manifest)
        mock_purge.reset_mock()

        # enabled on leader, no purge
        harness.update_config({config: True})
        mock_purge.assert_not_called()

        # disabled on follower, no purge
        harness.disable_hooks()
        harness.set_leader(False)
        harness.enable_hooks()
        harness.update_config({config: False})
        mock_purge.assert_not_called()
        assert harness.charm.unit.status.name == "active"


@mock.patch("charm.CephCsiCharm.ceph_context", new_callable=mock.PropertyMock)
def test_prevent_collisions(ceph_context, harness, caplog):
    ceph_context.return_value = {}
    harness.begin()

    config_manifest = harness.charm.collector.manifests["config"]
    encrypt_config = EncryptConfig(config_manifest)()
    ManifestLabel(config_manifest)(encrypt_config)
    encrypt_config.metadata.labels["juju.io/application"] = "ceph-csi-alternate"
    conflict = HashableResource(encrypt_config)
    caplog.clear()

    expected_results = [
        ResourceAnalysis("config", {conflict}, set(), set(), set()),
        ResourceAnalysis("rbd", set(), set(), set(), set()),
        ResourceAnalysis("cephfs", set(), set(), set(), set()),
    ]
    with mock.patch.object(harness.charm.collector, "analyze_resources") as mock_list_resources:
        mock_list_resources.return_value = expected_results

        with reconcile_this(harness, lambda e: harness.charm.prevent_collisions(e)):
            harness.set_leader(True)
            assert harness.charm.unit.status.name == "blocked"
            assert (
                harness.charm.unit.status.message
                == "1 Kubernetes resource collision (action: list-resources)"
            )
    assert "1 Kubernetes resource collision (action: list-resources)" in caplog.messages
    assert " Collision count in 'config' is 1" in caplog.messages
    assert "   ConfigMap/ceph-csi-encryption-kms-config" in caplog.messages


@pytest.fixture()
@mock.patch("charm.CephCsiCharm.ceph_context", new_callable=mock.PropertyMock)
def update_status_charm(ceph_context, harness):
    harness.set_leader(True)
    harness.begin()
    harness.charm.reconciler.stored.reconciled = True
    harness.charm.stored.namespace = "non-default"
    ceph_context.return_value = {
        "auth": "cephx",
        "fsid": "12345",
        "kubernetes_key": "123",
        "mon_hosts": ["10.0.0.1"],
        "user": "ceph-csi",
        "provisioner_replicas": 3,
        "enable_host_network": "false",
        "fsname": None,
    }
    return harness.charm


def test_update_status_no_provider(harness, update_status_charm):
    """Test that update_status emits blocked status if no provider is enabled."""
    harness.disable_hooks()
    harness.update_config({"cephfs-enable": False, "ceph-rbd-enable": False})
    harness.enable_hooks()
    update_status_charm.on.update_status.emit()
    assert update_status_charm.unit.status.name == "blocked"
    assert update_status_charm.unit.status.message == "Neither ceph-rbd nor cephfs is enabled."


def test_update_status_unready(update_status_charm):
    with mock.patch.object(update_status_charm, "collector") as mock_collector:
        mock_collector.unready = ["not-ready"]
        update_status_charm.on.update_status.emit()
    assert update_status_charm.unit.status.name == "waiting"
    assert update_status_charm.unit.status.message == "not-ready"


def test_update_status_changed_namespace(update_status_charm):
    with mock.patch.object(update_status_charm, "collector") as mock_collector:
        mock_collector.unready = []
        update_status_charm.on.update_status.emit()
    assert update_status_charm.unit.status.name == "blocked"
    assert (
        update_status_charm.unit.status.message == "Namespace cannot be changed after deployment"
    )


def test_update_status_ready(update_status_charm):
    update_status_charm.stored.namespace = "default"
    with mock.patch.object(update_status_charm, "collector") as mock_collector:
        mock_collector.unready = []
        mock_collector.short_version = "short-version"
        mock_collector.long_version = "long-version"
        update_status_charm.on.update_status.emit()
    assert update_status_charm.unit.status.name == "active"
    assert update_status_charm.app._backend._workload_version == "short-version"
    assert update_status_charm.app.status.message == "long-version"
