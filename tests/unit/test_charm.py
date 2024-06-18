# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing
"""Collection of tests related to src/charm.py"""

import contextlib
import json
import unittest.mock as mock
from subprocess import SubprocessError

import charms.operator_libs_linux.v0.apt as apt
import pytest
from lightkube.core.exceptions import ApiError
from lightkube.resources.storage_v1 import StorageClass
from ops.manifests import ManifestClientError
from ops.testing import Harness

from charm import CephCsiCharm


@pytest.fixture
def harness():
    harness = Harness(CephCsiCharm)
    try:
        yield harness
    finally:
        harness.cleanup()


@pytest.fixture(autouse=True)
def ceph_conf_directory(tmp_path):
    with mock.patch("charm.ceph_config_dir") as mock_path:
        mock_path.return_value = tmp_path / "ceph-conf"
        yield mock_path


@pytest.fixture(autouse=True)
def mock_apt():
    with mock.patch(
        "charms.operator_libs_linux.v0.apt.DebianPackage.from_system", autospec=True
    ) as mock_apt:
        yield mock_apt


@mock.patch("charm.CephCsiCharm.ceph_data", new_callable=mock.PropertyMock)
@mock.patch("charm.ceph_config_file")
def test_write_ceph_cli_config(ceph_config_file, ceph_data, harness, ceph_conf_directory):
    """Test writing of Ceph CLI config"""
    harness.begin()
    ceph_data.return_value = {"auth": "cephx", "mon_hosts": ["10.0.0.1", "10.0.0.2"]}
    harness.charm.write_ceph_cli_config()
    path = ceph_config_file.return_value
    path.open.assert_called_once_with("w")
    unit_name = harness.charm.unit.name.replace("/", "-")
    with path.open() as fp:
        lines = [
            "[global]",
            "auth cluster required = cephx",
            "auth service required = cephx",
            "auth client required = cephx",
            f"keyring = {ceph_conf_directory.return_value}/$cluster.$name.keyring",
            "mon host = 10.0.0.1 10.0.0.2",
            "log to syslog = true",
            "err to syslog = true",
            "clog to syslog = true",
            "mon cluster log to syslog = true",
            "debug mon = 1/5",
            "debug osd = 1/5",
            "",
            "[client]",
            f"log file = /var/log/ceph/{unit_name}.log",
            "",
        ]
        fp.write.assert_has_calls([mock.call(_l + "\n") for _l in lines])


@mock.patch("charm.CephCsiCharm.ceph_data", new_callable=mock.PropertyMock)
@mock.patch("charm.ceph_keyring_file")
def test_write_ceph_cli_keyring(ceph_keyring_file, ceph_data, harness):
    """Test writing of Ceph CLI keyring file"""
    harness.begin()
    ceph_data.return_value = {"key": "12345"}
    harness.charm.write_ceph_cli_keyring()
    ceph_keyring_file.return_value.open.assert_called_once_with("w")
    with ceph_keyring_file.return_value.open() as fp:
        lines = ["[client.ceph-csi]", "key = 12345", ""]
        fp.write.assert_has_calls([mock.call(_l + "\n") for _l in lines])


@mock.patch("charm.CephCsiCharm.write_ceph_cli_config")
@mock.patch("charm.CephCsiCharm.write_ceph_cli_keyring")
@mock.patch("charm.ceph_config_dir")
def test_configure_ceph_cli(ceph_config_dir, keyring, config, harness):
    """Test configuration of Ceph CLI"""
    harness.begin()
    harness.charm.configure_ceph_cli()
    ceph_config_dir.return_value.mkdir.assert_called_once_with(
        mode=0o700, parents=True, exist_ok=True
    )
    config.assert_called_once()
    keyring.assert_called_once()


@mock.patch("subprocess.check_output")
@mock.patch("charm.CephCsiCharm.ceph_data", new_callable=mock.PropertyMock)
def test_ceph_context_getter(ceph_data, check_output, harness, ceph_conf_directory):
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
        "fsname": None,
    }

    assert harness.charm.ceph_context == expected_context
    conf = (ceph_conf_directory() / "ceph.conf").absolute().as_posix()
    check_output.assert_any_call(
        ["/usr/bin/ceph", "--conf", conf, "--user", "ceph-csi", "fsid"], timeout=60
    )
    check_output.assert_any_call(
        ["/usr/bin/ceph", "--conf", conf, "--user", "ceph-csi", "fs", "ls", "-f", "json"],
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
        "install_ceph_common",
        "check_kube_config",
        "check_namespace",
        "check_ceph_client",
        "configure_ceph_cli",
        "check_cephfs",
        "evaluate_manifests",
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
        handlers["check_cephfs"].return_value = False
        harness.set_leader(True)
    assert harness.charm.unit.status.name == "active"
    assert harness.charm.unit.status.message == "Ready"
    assert harness.charm.reconciler.stored.reconciled
    not_called = {name: h for name, h in handlers.items() if not h.called}
    assert not_called == {}


def test_install(harness, mock_apt):
    """Test that on.install hook will call expected methods"""
    harness.begin()
    with reconcile_this(harness, harness.charm.install_ceph_common):
        harness.charm.on.install.emit()

    mock_apt.assert_called_once_with("ceph-common")
    mock_apt.return_value.ensure.assert_called_once_with(apt.PackageState.Latest)


def test_install_pkg_missing(harness, mock_apt):
    """Test that on.install hook will call expected methods"""
    mock_apt.side_effect = apt.PackageNotFoundError

    harness.begin()
    with reconcile_this(harness, harness.charm.install_ceph_common):
        harness.charm.on.install.emit()

    mock_apt.assert_called_once_with("ceph-common")
    assert harness.charm.unit.status.name == "blocked"
    assert harness.charm.unit.status.message == "Failed to install ceph-common apt package."


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


@mock.patch("charm.CephCsiCharm.configure_ceph_cli", mock.MagicMock())
def test_check_ceph_client(harness):
    """Test that check_ceph_client sets expected unit states."""

    # Add ceph-mon relation, indicating existing relations
    harness.begin()

    with reconcile_this(harness, lambda _: harness.charm.check_ceph_client()):
        harness.charm.on.install.emit()

    assert harness.charm.unit.status.name == "blocked"
    assert harness.charm.unit.status.message == "Missing relation: ceph-client"

    with reconcile_this(harness, lambda _: harness.charm.check_ceph_client()):
        rel_id = harness.add_relation(CephCsiCharm.CEPH_CLIENT_RELATION, "ceph-mon")

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
        relation_id = harness.add_relation(CephCsiCharm.CEPH_CLIENT_RELATION, "ceph-mon")
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


@mock.patch("charm.CephCsiCharm.configure_ceph_cli", mock.MagicMock())
def test_action_list_versions(harness):
    harness.begin()

    mock_event = mock.MagicMock()
    assert harness.charm._list_versions(mock_event) is None
    expected_results = {
        "cephfs-versions": "v3.11.0\nv3.10.2\nv3.10.1\nv3.10.0\nv3.9.0\nv3.8.1\nv3.8.0\nv3.7.2",
        "config-versions": "",
        "rbd-versions": "v3.11.0\nv3.10.2\nv3.10.1\nv3.10.0\nv3.9.0\nv3.8.1\nv3.8.0\nv3.7.2",
    }
    mock_event.set_results.assert_called_once_with(expected_results)


@mock.patch("charm.CephCsiCharm.configure_ceph_cli", mock.MagicMock())
@mock.patch("charm.CephCsiCharm.get_ceph_fsid", mock.MagicMock(return_value="12345"))
@mock.patch("charm.CephCsiCharm.get_ceph_fsname", mock.MagicMock(return_value=None))
def test_action_manifest_resources(harness):
    harness.begin()

    mock_event = mock.MagicMock()
    assert harness.charm._list_resources(mock_event) is None
    expected_results = {
        "config-missing": "ConfigMap/default/ceph-csi-encryption-kms-config",
        "rbd-missing": "\n".join(
            [
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
    mock_event.set_results.assert_called_once_with(expected_results)

    mock_event.set_results.reset_mock()
    assert harness.charm._scrub_resources(mock_event) is None
    mock_event.set_results.assert_called_with(expected_results)

    mock_event.set_results.reset_mock()
    assert harness.charm._sync_resources(mock_event) is None
    mock_event.set_results.assert_called_with(expected_results)


@mock.patch("charm.CephCsiCharm.configure_ceph_cli", mock.MagicMock())
@mock.patch("charm.CephCsiCharm.get_ceph_fsid", mock.MagicMock(return_value="12345"))
@mock.patch("charm.CephCsiCharm.get_ceph_fsname", mock.MagicMock(return_value=None))
def test_action_sync_resources_install_failure(harness, lk_client):
    harness.begin()

    mock_event = mock.MagicMock()
    expected_results = {"result": "Failed to sync missing resources: API Server Unavailable"}
    lk_client.apply.side_effect = ManifestClientError("API Server Unavailable")
    assert harness.charm._sync_resources(mock_event) is None
    mock_event.set_results.assert_called_with(expected_results)


def test_action_delete_storage_class_unknown(harness, lk_client):
    harness.begin_with_initial_hooks()

    mock_event = mock.MagicMock()
    expected_results = "Invalid storage class name. Must be one of: cephfs, ceph-xfs, ceph-ext4"
    lk_client.delete.side_effect = ManifestClientError("API Server Unavailable")
    assert harness.charm._delete_storage_class(mock_event) is None
    mock_event.fail.assert_called_with(expected_results)
    lk_client.delete.assert_not_called()


def test_action_delete_storage_class_api_error(harness, lk_client):
    harness.begin_with_initial_hooks()

    mock_event = mock.MagicMock()
    mock_event.params = {"name": "cephfs"}
    lk_client.delete.side_effect = ManifestClientError("API Server Unavailable")
    assert harness.charm._delete_storage_class(mock_event) is None
    lk_client.delete.assert_called_once_with(StorageClass, name="cephfs")
    mock_event.fail.assert_called_with("Failed to delete storage class: API Server Unavailable")


def test_action_delete_storage_class(harness, lk_client):
    harness.begin_with_initial_hooks()

    mock_event = mock.MagicMock()
    mock_event.params = {"name": "cephfs"}
    assert harness.charm._delete_storage_class(mock_event) is None
    lk_client.delete.assert_called_once_with(StorageClass, name="cephfs")
    mock_event.set_results.assert_called_with(
        {"result": "Successfully deleted StorageClass/cephfs"}
    )


@mock.patch("charm.CephCsiCharm.ceph_cli", mock.MagicMock(side_effect=SubprocessError))
def test_failed_cli_fsid(harness):
    harness.begin()
    assert harness.charm.get_ceph_fsid() == ""


@pytest.mark.parametrize("failure", [SubprocessError, lambda *_: "invalid"])
def test_failed_cli_fsname(harness, failure):
    harness.begin()
    with mock.patch("charm.CephCsiCharm.ceph_cli", side_effect=failure):
        assert harness.charm.get_ceph_fsname() is None


def test_cli_fsname(request, harness):
    harness.begin()
    pools = json.dumps([{"data_pools": ["ceph-fs_data"], "name": request.node.name}])
    with mock.patch("charm.CephCsiCharm.ceph_cli", return_value=pools):
        assert harness.charm.get_ceph_fsname() == request.node.name


def test_kubelet_dir(harness):
    harness.begin()
    data = {"kubelet-root-dir": "/var/snap/k8s/common/var/lib/kubelet"}
    with reconcile_this(harness, lambda _: None):
        rel_id = harness.add_relation("kubernetes-info", "k8s")
        harness.add_relation_unit(rel_id, "k8s/0")
        harness.update_relation_data(rel_id, "k8s", data)

    assert harness.charm.kubelet_dir == "/var/snap/k8s/common/var/lib/kubelet"


def test_check_cephfs(harness):
    harness.begin()

    # no enable, no fsname -> no problem
    with mock.patch("charm.CephCsiCharm.ceph_context", new_callable=mock.PropertyMock) as mock_ctx:
        mock_ctx.return_value = {"fsname": None}
        with reconcile_this(harness, lambda _: harness.charm.check_cephfs()):
            harness.set_leader(True)
            harness.update_config({"cephfs-enable": False})
    assert harness.charm.unit.status.name == "active"

    # enable, fsname -> no problem
    with mock.patch("charm.CephCsiCharm.ceph_context", new_callable=mock.PropertyMock) as mock_ctx:
        mock_ctx.return_value = {"fsname": "abcd"}
        with reconcile_this(harness, lambda _: harness.charm.check_cephfs()):
            harness.update_config({"cephfs-enable": True})
    assert harness.charm.unit.status.name == "active"

    # enable, no fsname -> problem
    with mock.patch("charm.CephCsiCharm.ceph_context", new_callable=mock.PropertyMock) as mock_ctx:
        mock_ctx.return_value = {"fsname": None}
        with reconcile_this(harness, lambda _: harness.charm.check_cephfs()):
            harness.update_config({"cephfs-enable": True})
    assert harness.charm.unit.status.name == "blocked"
    assert harness.charm.unit.status.message == "CephFS is not usable; set 'cephfs-enable=False'"


@mock.patch("charm.CephCsiCharm.ceph_context", new_callable=mock.PropertyMock)
def test_update_status_waiting(ceph_context, harness):
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
    with mock.patch.object(harness.charm, "collector") as mock_collector:
        mock_collector.unready = ["not-ready"]
        harness.charm.on.update_status.emit()
    assert harness.charm.unit.status.name == "waiting"
    assert harness.charm.unit.status.message == "not-ready"

    with mock.patch.object(harness.charm, "collector") as mock_collector:
        mock_collector.unready = []
        harness.charm.on.update_status.emit()
    assert harness.charm.unit.status.name == "blocked"
    assert harness.charm.unit.status.message == "Namespace cannot be changed after deployment"

    harness.charm.stored.namespace = "default"
    with mock.patch.object(harness.charm, "collector") as mock_collector:
        mock_collector.unready = []
        mock_collector.short_version = "short-version"
        mock_collector.long_version = "long-version"
        harness.charm.on.update_status.emit()
    assert harness.charm.unit.status.name == "active"
    assert harness.charm.app._backend._workload_version == "short-version"
    assert harness.charm.app.status.message == "long-version"
