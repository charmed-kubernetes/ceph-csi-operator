# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing
"""Collection of tests related to src/charm.py"""

import contextlib
import unittest.mock as mock

import charms.contextual_status as status
import charms.operator_libs_linux.v0.apt as apt
import ops
import pytest
from lightkube.core.exceptions import ApiError
from lightkube.models.meta_v1 import ObjectMeta
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
        "rbd_pool": None,
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
def mocked_handlers(charm):
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
    ]

    handlers = {name: mock.patch(f"charm.CephCsiCharm.{name}") for name in handler_names}
    handlers["update_status_run"] = mock.patch.object(charm.update_status, "run")
    yield {h: patch.start() for h, patch in handlers.items()}
    for handler in handlers.values():
        handler.stop()


def test_set_leader(harness):
    """Test emitting the set_leader hook while not reconciled.

    Args:
        harness: the harness under test
    """
    harness.begin()
    harness.charm.reconciler.stored.reconciled = False  # Pretended to not be reconciled
    with mocked_handlers(harness.charm) as handlers:
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


def test_check_namespace_creation_allowed(harness, lk_charm_client):
    harness.set_leader(True)
    harness.update_config({"create-namespace": True})
    harness.begin()

    not_found = ApiError(response=mock.MagicMock())
    duplicate = ApiError(response=mock.MagicMock())
    error = ApiError(response=mock.MagicMock())

    not_found.status.code = 404
    not_found.status.message = "not found"

    duplicate.status.code = 409
    duplicate.status.message = "duplicate found"

    error.status.code = 503
    error.status.message = "service unavailable"

    # first return a 404 to force creation of the namespace
    lk_charm_client.get.side_effect = not_found

    # then simulate a successful creation
    lk_charm_client.create.side_effect = None
    with reconcile_this(harness, lambda _: harness.charm.check_namespace()):
        harness.charm.on.install.emit()
    assert harness.charm.unit.status.name == "active"

    # now a duplicate to simulate the namespace already existing
    lk_charm_client.create.side_effect = duplicate
    with reconcile_this(harness, lambda _: harness.charm.check_namespace()):
        harness.charm.on.install.emit()
    assert harness.charm.unit.status.name == "active"

    # now an error to simulate the namespace creation failing
    lk_charm_client.create.side_effect = error
    with reconcile_this(harness, lambda _: harness.charm.check_namespace()):
        harness.charm.on.install.emit()
    assert harness.charm.unit.status.name == "waiting"


@pytest.mark.parametrize("leadership", [True, False])
def test_check_namespace(harness, leadership, lk_charm_client):
    harness.set_leader(leadership)
    harness.begin()

    api_error = ApiError(response=mock.MagicMock())

    # should be blocked if the ns doesn't exist
    api_error.status.code = 404
    api_error.status.message = "not found"
    lk_charm_client.get.side_effect = api_error

    with reconcile_this(harness, lambda _: harness.charm.check_namespace()):
        harness.charm.on.install.emit()
    if leadership:
        assert harness.charm.unit.status.name == "blocked"
        assert harness.charm.unit.status.message == "Missing namespace 'default'"
    else:
        # should be waiting if not a leader
        assert harness.charm.unit.status.name == "waiting"
        assert harness.charm.unit.status.message == "Waiting for namespace 'default'"

    # should be in waiting if the k8s api isn't ready
    api_error.status.code = 500
    api_error.status.message = "Internal Server Error"
    lk_charm_client.get.side_effect = api_error
    with reconcile_this(harness, lambda _: harness.charm.check_namespace()):
        harness.charm.on.install.emit()
    assert harness.charm.unit.status.name == "waiting"
    assert harness.charm.unit.status.message == "Waiting for Kubernetes API"

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


@mock.patch("charm.SafeManifest.storage_classes")
@mock.patch("charm.CephCsiCharm.ceph_context", new_callable=mock.PropertyMock)
def test_block_multiple_default_storage_classes(ceph_context, storage_classes, harness):
    """Test that multiple default storage classes are blocked."""
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
    ext4_sc = mock.MagicMock()
    ext4_sc.name = "ceph-ext4"
    ext4_sc.resource.metadata.annotations = literals.DEFAULT_SC_ANNOTATION

    xfs_sc = mock.MagicMock()
    xfs_sc.name = "ceph-xfs"
    xfs_sc.resource.metadata.annotations = literals.DEFAULT_SC_ANNOTATION

    missing_meta = mock.MagicMock()
    missing_meta.name = "invalid"
    missing_meta.resource.metadata = None
    storage_classes.return_value = {ext4_sc, xfs_sc, missing_meta}
    with pytest.raises(status.ReconcilerError) as ei:
        harness.charm.evaluate_manifests()
    assert str(ei.value) == "Multiple StorageClasses are marked as default: ceph-ext4, ceph-xfs"


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
    ceph_versions = ["v1.0.0", "v1.1.0", "v2.0.0"]

    mock_event = mock.MagicMock(spec=ops.ActionEvent)
    with mock.patch.object(harness.charm.collector, "manifests") as mock_manifests:
        mock_manifests.items.return_value = {
            ("cephfs", mock.MagicMock(releases=ceph_versions)),
            ("config", mock.MagicMock(releases=[])),
            ("rbd", mock.MagicMock(releases=ceph_versions)),
        }
        assert harness.charm._list_versions(mock_event) is None

    expected_results = {
        "cephfs-versions": "\n".join(ceph_versions),
        "config-versions": "",
        "rbd-versions": "\n".join(ceph_versions),
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
    harness.update_config({"default-storage": ""})
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


def test_update_status_ready_with_cluster_warning(update_status_charm):
    update_status_charm.stored.namespace = "default"
    with mock.patch.object(update_status_charm, "collector") as mock_collector:
        mock_manifest = mock.MagicMock()
        default_annotated_meta = mock.Mock(spec_set=ObjectMeta)
        default_annotated_meta.annotations = literals.DEFAULT_SC_ANNOTATION
        mock_manifest.client.list.return_value = [
            mock.Mock(spec_set=StorageClass, name="ceph-ext4", metadata=default_annotated_meta),
            mock.Mock(spec_set=StorageClass, name="ceph-xfs", metadata=default_annotated_meta),
        ]
        mock_collector.manifests = {"manifest": mock_manifest}
        mock_collector.unready = []
        mock_collector.short_version = "short-version"
        mock_collector.long_version = "long-version"
        update_status_charm.on.update_status.emit()
    mock_manifest.client.list.assert_called_once_with(StorageClass)
    assert (
        update_status_charm.unit.status.message
        == "Ready (Cluster contains multiple default StorageClasses)"
    )
    assert update_status_charm.unit.status.name == "active"
    assert update_status_charm.app._backend._workload_version == "short-version"
    assert update_status_charm.app.status.message == "long-version"


def test_update_status_ready_with_missing_default_warning(update_status_charm, harness):
    harness.disable_hooks()
    harness.update_config({"default-storage": "cephfs"})
    harness.enable_hooks()
    update_status_charm.stored.namespace = "default"
    with mock.patch.object(update_status_charm, "collector") as mock_collector:
        mock_manifest = mock.MagicMock()

        # Create 3 mock StorageClasses
        # 1. matches this charm's name but is not annotated as default
        my_app_non_default = mock.Mock(name="ceph-ext4", spec_set=StorageClass)
        my_app_non_default.metadata.labels = {"juju.io/application": harness.model.app.name}
        my_app_non_default.metadata.annotations = {}

        # 2. matches another charm's name but is not annotated as default
        other_app_non_default = mock.Mock(name="ceph-alt-ext4", spec_set=StorageClass)
        other_app_non_default.metadata.labels = {"juju.io/application": "ceph-csi-alternate"}

        # 3. matches another charm's name and is annotated as default
        other_app_default = mock.Mock(name="ceph-alt-xfs", spec_set=StorageClass)
        other_app_default.metadata.labels = {"juju.io/application": "ceph-csi-alternate"}
        other_app_default.metadata.annotations = literals.DEFAULT_SC_ANNOTATION

        mock_manifest.client.list.return_value = [
            other_app_non_default,
            other_app_default,
            my_app_non_default,
        ]
        mock_collector.manifests = {"manifest": mock_manifest}
        mock_collector.unready = []
        mock_collector.short_version = "short-version"
        mock_collector.long_version = "long-version"
        update_status_charm.on.update_status.emit()
    mock_manifest.client.list.assert_called_once_with(StorageClass)
    assert (
        update_status_charm.unit.status.message
        == "Ready ('cephfs' doesn't match any charm managed StorageClasses)"
    )
    assert update_status_charm.unit.status.name == "active"
    assert update_status_charm.app._backend._workload_version == "short-version"
    assert update_status_charm.app.status.message == "long-version"


def test_update_status_ready_with_default(update_status_charm, harness):
    harness.disable_hooks()
    harness.update_config({"default-storage": "cephfs"})
    harness.enable_hooks()
    update_status_charm.stored.namespace = "default"
    with mock.patch.object(update_status_charm, "collector") as mock_collector:
        mock_manifest = mock.MagicMock()
        default_metadata = mock.Mock(spec_set=ObjectMeta)
        default_metadata.labels = {"juju.io/application": harness.model.app.name}
        default_metadata.annotations = literals.DEFAULT_SC_ANNOTATION
        mock_manifest.client.list.return_value = [
            mock.Mock(spec_set=StorageClass, name="cephfs", metadata=default_metadata),
        ]
        mock_collector.manifests = {"manifest": mock_manifest}
        mock_collector.unready = []
        mock_collector.short_version = "short-version"
        mock_collector.long_version = "long-version"
        update_status_charm.on.update_status.emit()
    mock_manifest.client.list.assert_called_once_with(StorageClass)
    assert update_status_charm.unit.status.message == "Ready"
    assert update_status_charm.unit.status.name == "active"
    assert update_status_charm.app._backend._workload_version == "short-version"
    assert update_status_charm.app.status.message == "long-version"


def test_update_status_ready_no_default(update_status_charm):
    update_status_charm.stored.namespace = "default"
    with mock.patch.object(update_status_charm, "collector") as mock_collector:
        mock_manifest = mock.MagicMock()
        mock_collector.manifests = {"manifest": mock_manifest}
        mock_collector.unready = []
        mock_collector.short_version = "short-version"
        mock_collector.long_version = "long-version"
        update_status_charm.on.update_status.emit()
    mock_manifest.client.list.assert_called_once_with(StorageClass)
    assert update_status_charm.unit.status.message == "Ready"
    assert update_status_charm.unit.status.name == "active"
    assert update_status_charm.app._backend._workload_version == "short-version"
    assert update_status_charm.app.status.message == "long-version"


def test_check_ceph_client_mutual_exclusivity(harness):
    """Test that having both ceph-csi and ceph-client relations blocks the charm."""
    harness.begin()

    # Add both relations
    with reconcile_this(harness, lambda _: None):
        harness.add_relation(literals.CEPH_CLIENT_RELATION, "ceph-mon")
        harness.add_relation(literals.CEPH_CSI_RELATION, "microceph")

    with reconcile_this(harness, lambda _: harness.charm.check_ceph_client()):
        harness.charm.on.install.emit()

    assert harness.charm.unit.status.name == "blocked"
    assert (
        harness.charm.unit.status.message
        == "Both ceph-csi and ceph-client relations are active. Only one is allowed."
    )


def test_ceph_user_returns_user_id_from_ceph_csi(harness):
    """Test that ceph_user returns user_id from ceph-csi relation when available."""
    harness.begin()

    # Mock the ceph_csi.get_relation_data to return user_id
    with mock.patch.object(
        harness.charm.ceph_csi, "get_relation_data", return_value={"user_id": "microceph-user"}
    ):
        assert harness.charm.ceph_user == "microceph-user"


def test_ceph_user_returns_app_name_without_ceph_csi(harness):
    """Test that ceph_user returns app.name when no ceph-csi relation data."""
    harness.begin()

    # No ceph-csi relation data
    with mock.patch.object(harness.charm.ceph_csi, "get_relation_data", return_value=None):
        assert harness.charm.ceph_user == "ceph-csi"

    # ceph-csi relation data without user_id
    with mock.patch.object(harness.charm.ceph_csi, "get_relation_data", return_value={}):
        assert harness.charm.ceph_user == "ceph-csi"


def test_cephfs_from_relation(harness):
    """Test that _cephfs_from_relation constructs valid CephFilesystem objects."""
    harness.begin()

    # Test with valid cephfs_fs_name
    csi_data = {"cephfs_fs_name": "myfs"}
    result = harness.charm._cephfs_from_relation(csi_data)

    assert len(result) == 1
    assert result[0].name == "myfs"
    assert result[0].metadata_pool == "cephfs.myfs.meta"
    assert result[0].data_pools == ["cephfs.myfs.data"]
    assert result[0].metadata_pool_id == 0
    assert result[0].data_pool_ids == [0]

    # Test without cephfs_fs_name
    csi_data_empty = {}
    result_empty = harness.charm._cephfs_from_relation(csi_data_empty)
    assert result_empty == []

    # Test with None cephfs_fs_name
    csi_data_none = {"cephfs_fs_name": None}
    result_none = harness.charm._cephfs_from_relation(csi_data_none)
    assert result_none == []


@mock.patch("utils.ls_ceph_fs")
@mock.patch("charm.CephCsiCharm.ceph_data", new_callable=mock.PropertyMock)
def test_ceph_context_uses_relation_fs_when_cli_empty(ceph_data, mock_ls_ceph_fs, harness):
    """Test that ceph_context falls back to relation data when CLI returns empty fs list."""
    harness.begin()

    # Mock ceph_data for basic auth
    ceph_data.return_value = {
        "auth": "cephx",
        "key": "secret_key",
        "mon_hosts": ["10.0.0.1"],
    }

    # Mock CLI to return empty fs list
    mock_ls_ceph_fs.return_value = []

    # Mock ceph_csi relation data with cephfs_fs_name
    with mock.patch.object(
        harness.charm.ceph_csi,
        "get_relation_data",
        return_value={
            "fsid": "csi-fsid",
            "user_id": "csi-user",
            "user_key": "csi-key",
            "cephfs_fs_name": "cephfs",
        },
    ):
        context = harness.charm.ceph_context

    # Verify the fs_list was populated from relation data
    assert len(context["fs_list"]) == 1
    assert context["fs_list"][0].name == "cephfs"
    assert context["fs_list"][0].data_pools == ["cephfs.cephfs.data"]


@mock.patch("utils.ls_ceph_fs")
@mock.patch("charm.CephCsiCharm.ceph_data", new_callable=mock.PropertyMock)
def test_ceph_context_uses_rbd_pool_from_ceph_csi_relation(ceph_data, mock_ls_ceph_fs, harness):
    """Test that ceph_context includes rbd_pool from ceph-csi relation data."""
    harness.begin()

    # Mock ceph_data for basic auth
    ceph_data.return_value = {
        "auth": "cephx",
        "key": "secret_key",
        "mon_hosts": ["10.0.0.1"],
    }

    # Mock CLI to return empty fs list
    mock_ls_ceph_fs.return_value = []

    # Mock ceph_csi relation data with rbd_pool
    with mock.patch.object(
        harness.charm.ceph_csi,
        "get_relation_data",
        return_value={
            "fsid": "csi-fsid",
            "user_id": "csi-user",
            "user_key": "csi-key",
            "rbd_pool": "rbd.my-custom-pool",
        },
    ):
        context = harness.charm.ceph_context

    # Verify rbd_pool is included in the context
    assert context["rbd_pool"] == "rbd.my-custom-pool"
    assert context["fsid"] == "csi-fsid"
    assert context["user"] == "csi-user"
    assert context["kubernetes_key"] == "csi-key"


def test_on_ceph_csi_available_requests_workloads(harness):
    """Test _on_ceph_csi_available handler requests workloads."""
    harness.begin()
    harness.set_leader(True)

    # Add ceph-csi relation
    relation_id = harness.add_relation(literals.CEPH_CSI_RELATION, "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    # Mock request_workloads to track calls
    with mock.patch.object(harness.charm.ceph_csi, "request_workloads") as mock_request:
        # Trigger ceph_csi_available event
        harness.update_relation_data(relation_id, "microceph", {"fsid": "test"})

        # Verify workloads were requested
        mock_request.assert_called_once()
        called_workloads = mock_request.call_args[0][0]
        assert "rbd" in called_workloads or "cephfs" in called_workloads


def test_on_ceph_csi_available_requests_rbd_workload(harness):
    """Test _on_ceph_csi_available requests rbd workload when enabled."""
    harness.begin()
    harness.set_leader(True)
    harness.update_config({"ceph-rbd-enable": True, "cephfs-enable": False})

    relation_id = harness.add_relation(literals.CEPH_CSI_RELATION, "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    with mock.patch.object(harness.charm.ceph_csi, "request_workloads") as mock_request:
        harness.update_relation_data(relation_id, "microceph", {"fsid": "test"})

        mock_request.assert_called_once()
        called_workloads = mock_request.call_args[0][0]
        assert "rbd" in called_workloads
        assert "cephfs" not in called_workloads


def test_on_ceph_csi_available_requests_cephfs_workload(harness):
    """Test _on_ceph_csi_available requests cephfs workload when enabled."""
    harness.begin()
    harness.set_leader(True)
    harness.update_config({"ceph-rbd-enable": False, "cephfs-enable": True})

    relation_id = harness.add_relation(literals.CEPH_CSI_RELATION, "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    with mock.patch.object(harness.charm.ceph_csi, "request_workloads") as mock_request:
        harness.update_relation_data(relation_id, "microceph", {"fsid": "test"})

        mock_request.assert_called_once()
        called_workloads = mock_request.call_args[0][0]
        assert "cephfs" in called_workloads
        assert "rbd" not in called_workloads


def test_on_ceph_csi_available_requests_both_workloads(harness):
    """Test _on_ceph_csi_available requests both workloads when both enabled."""
    harness.begin()
    harness.set_leader(True)
    harness.update_config({"ceph-rbd-enable": True, "cephfs-enable": True})

    relation_id = harness.add_relation(literals.CEPH_CSI_RELATION, "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    with mock.patch.object(harness.charm.ceph_csi, "request_workloads") as mock_request:
        harness.update_relation_data(relation_id, "microceph", {"fsid": "test"})

        mock_request.assert_called_once()
        called_workloads = mock_request.call_args[0][0]
        assert "rbd" in called_workloads
        assert "cephfs" in called_workloads


def test_request_ceph_csi_workloads_with_no_providers_enabled(harness):
    """Test _request_ceph_csi_workloads when no providers are enabled."""
    harness.begin()
    harness.set_leader(True)
    harness.update_config({"ceph-rbd-enable": False, "cephfs-enable": False})

    relation_id = harness.add_relation(literals.CEPH_CSI_RELATION, "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    with mock.patch.object(harness.charm.ceph_csi, "request_workloads") as mock_request:
        harness.charm._request_ceph_csi_workloads()

        # When no providers are enabled, request_workloads should not be called
        # or should be called with empty list
        # Looking at the code, it checks if workloads list is truthy before calling
        mock_request.assert_not_called()


@mock.patch("charm.CephCsiCharm.ceph_data", new_callable=mock.PropertyMock)
def test_check_ceph_client_with_incomplete_ceph_csi_data(ceph_data, harness):
    """Test check_ceph_client raises error when ceph-csi relation data is incomplete."""
    ceph_data.return_value = None
    harness.begin()
    harness.disable_hooks()

    # Add ceph-csi relation
    relation_id = harness.add_relation(literals.CEPH_CSI_RELATION, "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    # Mock incomplete ceph-csi data (missing user_key)
    with mock.patch.object(
        harness.charm.ceph_csi,
        "get_relation_data",
        return_value={
            "fsid": "test-fsid",
            "mon_hosts": ["10.0.0.1"],
            "user_id": "ceph-csi",
            "user_key": None,  # Missing this required field
        },
    ):
        # This should raise a ReconcilerError
        with pytest.raises(status.ReconcilerError, match="ceph-csi relation is missing data"):
            harness.charm.check_ceph_client()
