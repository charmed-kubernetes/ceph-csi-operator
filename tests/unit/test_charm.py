# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing
"""Collection of tests related to src/charm.py"""

import json
import logging
import unittest.mock as mock
from subprocess import SubprocessError

import charms.operator_libs_linux.v0.apt as apt
import pytest
from lightkube.core.exceptions import ConfigError
from ops.manifests import ManifestClientError
from ops.manifests.manipulations import AnyCondition
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
def mock_apt():
    with mock.patch("charms.operator_libs_linux.v0.apt.add_package", autospec=True) as mock_apt:
        yield mock_apt


def test_write_ceph_cli_config(harness):
    """Test writing of Ceph CLI config"""
    harness.begin()
    harness.charm.stored.ceph_data["auth"] = "cephx"
    harness.charm.stored.ceph_data["mon_hosts"] = ["10.0.0.1", "10.0.0.2"]
    with mock.patch("charm.Path") as mock_path:
        harness.charm.write_ceph_cli_config()
        mock_path.assert_called_once_with("/etc/ceph/ceph.conf")
        path = mock_path.return_value
        path.open.assert_called_once_with("w")
        with path.open() as fp:
            lines = [
                "[global]",
                "auth cluster required = cephx",
                "auth service required = cephx",
                "auth client required = cephx",
                "keyring = /etc/ceph/$cluster.$name.keyring",
                "mon host = 10.0.0.1 10.0.0.2",
                "log to syslog = true",
                "err to syslog = true",
                "clog to syslog = true",
                "mon cluster log to syslog = true",
                "debug mon = 1/5",
                "debug osd = 1/5",
                "",
                "[client]",
                "log file = /var/log/ceph.log",
                "",
            ]
            fp.write.assert_has_calls([mock.call(_l + "\n") for _l in lines])


def test_write_ceph_cli_keyring(harness):
    """Test writing of Ceph CLI keyring file"""
    harness.begin()
    harness.charm.stored.ceph_data["key"] = "12345"
    with mock.patch("charm.Path") as mock_path:
        harness.charm.write_ceph_cli_keyring()
        mock_path.assert_called_once_with("/etc/ceph/ceph.client.ceph-csi.keyring")
        path = mock_path.return_value
        path.open.assert_called_once_with("w")
        with path.open() as fp:
            lines = ["[client.ceph-csi]", "key = 12345", ""]
            fp.write.assert_has_calls([mock.call(_l + "\n") for _l in lines])


@mock.patch("charm.CephCsiCharm.write_ceph_cli_config")
@mock.patch("charm.CephCsiCharm.write_ceph_cli_keyring")
def test_configure_ceph_cli(keyring, config, harness):
    """Test configuration of Ceph CLI"""
    harness.begin()
    with mock.patch("charm.Path") as mock_path:
        harness.charm.configure_ceph_cli()
        mock_path.assert_called_once_with("/etc/ceph")
        path = mock_path.return_value
        path.mkdir.assert_called_once_with(parents=True, exist_ok=True)
        config.assert_called_once()
        keyring.assert_called_once()


@mock.patch("subprocess.check_output")
def test_ceph_context_getter(check_output, harness):
    """Test that ceph_context property returns properly formatted data."""
    fsid = "12345"
    key = "secret_key"
    monitors = ["10.0.0.1", "10.0.0.2"]

    # stored data from ceph-mon:client relation
    harness.begin()
    harness.charm.stored.ceph_data["key"] = key
    harness.charm.stored.ceph_data["mon_hosts"] = monitors

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
    check_output.assert_any_call(["ceph", "--user", "ceph-csi", "fsid"], timeout=60)
    check_output.assert_any_call(
        ["ceph", "--user", "ceph-csi", "fs", "ls", "-f", "json"], timeout=60
    )


@mock.patch("charm.CephCsiCharm._install_manifests")
@mock.patch("charm.CephCsiCharm._check_required_relations", return_value=False)
def test_install(relation_check, install_manifests, harness, mock_apt):
    """Test that on.install hook will call expected methods"""
    harness.begin()
    harness.charm.on.install.emit()

    mock_apt.assert_called_once_with(["ceph-common"], update_cache=True)
    relation_check.assert_called_once_with()
    install_manifests.assert_not_called()


def test_install_pkg_missing(harness, mock_apt):
    """Test that on.install hook will call expected methods"""
    mock_apt.side_effect = apt.PackageNotFoundError

    harness.begin()
    harness.charm.on.install.emit()

    mock_apt.assert_called_once_with(["ceph-common"], update_cache=True)
    assert harness.charm.unit.status.name == "blocked"
    assert harness.charm.unit.status.message == "Apt packages not found."


def test_install_pkg_error(harness, mock_apt):
    """Test that on.install hook will call expected methods"""
    mock_apt.side_effect = apt.PackageError

    harness.begin()
    harness.charm.on.install.emit()

    mock_apt.assert_called_once_with(["ceph-common"], update_cache=True)
    assert harness.charm.unit.status.name == "blocked"
    assert harness.charm.unit.status.message == "Could not apt install packages."


@mock.patch("charm.CephCsiCharm.configure_ceph_cli", mock.MagicMock())
@mock.patch("charm.CephCsiCharm._check_kube_config", return_value=False)
def test_required_relation_check(check_kube_config, harness):
    """Test that check_required_relations sets expected unit states."""

    # Add ceph-mon relation, indicating existing relations
    harness.begin()
    assert not harness.charm._check_required_relations()

    assert harness.charm.unit.status.name == "blocked"
    assert harness.charm.unit.status.message == "Missing relations: ceph-client"

    rel_id = harness.add_relation(CephCsiCharm.CEPH_CLIENT_RELATION, "ceph-mon")
    assert not harness.charm._check_required_relations()

    assert harness.charm.unit.status.name == "waiting"
    assert harness.charm.unit.status.message == "Ceph relation is missing data."

    data = {"auth": "some-auth", "key": "1234", "mon_hosts": '["10.0.0.1", "10.0.0.2"]'}
    harness.add_relation_unit(rel_id, "ceph-mon/0")
    harness.update_relation_data(rel_id, "ceph-mon/0", data)
    check_kube_config.assert_called_once()


@mock.patch("interface_ceph_client.ceph_client.CephClientRequires.request_ceph_permissions")
@mock.patch("interface_ceph_client.ceph_client.CephClientRequires.create_replicated_pool")
def test_ceph_client_broker_available(create_replicated_pool, request_ceph_permissions, harness):
    """Test that when the ceph client broker is available, a pool is
    requested and Ceph capabilities are requested
    """
    # Setup charm
    harness.begin()

    # add ceph-client relation
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


@pytest.mark.parametrize("leadership", (False, True))
@mock.patch("charm.CephCsiCharm.get_ceph_fsid", mock.MagicMock(return_value="12345"))
@mock.patch("charm.CephCsiCharm.get_ceph_fsname", mock.MagicMock(return_value=None))
@mock.patch("charm.CephCsiCharm.configure_ceph_cli")
@mock.patch("charm.CephCsiCharm._check_kube_config", return_value=True)
def test_ceph_client_relation_changed_leader(
    check_kube_config,
    configure_ceph_cli,
    leadership,
    harness,
    lk_client,
):
    """Test ceph-client-relation-changed hook on a leader unit"""
    harness.begin()
    rel_id = harness.add_relation(CephCsiCharm.CEPH_CLIENT_RELATION, "ceph-mon")
    harness.set_leader(leadership)
    data = {
        "auth": "cephx",
        "key": "12345",
        "mon_hosts": '["10.0.0.1", "10.0.0.2"]',
        "ceph-public-address": "10.0.0.1",
    }

    harness.add_relation_unit(rel_id, "ceph-mon/0")
    configure_ceph_cli.assert_not_called()
    check_kube_config.assert_not_called()
    assert harness.charm.stored.ceph_data == {}
    assert not harness.charm.stored.deployed
    assert harness.charm.stored.config_hash == 0
    lk_client.apply.assert_not_called()

    harness.charm.on.update_status.emit()
    assert harness.charm.unit.status.name == "waiting"
    assert harness.charm.unit.status.message == "Ceph relation is missing data."

    harness.update_relation_data(rel_id, "ceph-mon/0", data)
    configure_ceph_cli.assert_called_once_with()
    check_kube_config.assert_called_once()
    assert harness.charm.stored.ceph_data == {
        "auth": "cephx",
        "key": "12345",
        "mon_hosts": ["10.0.0.1"],
    }
    assert harness.charm.stored.deployed
    assert harness.charm.stored.config_hash != 0
    if leadership:
        lk_client.apply.assert_called()
    else:
        lk_client.apply.assert_not_called()

    # Fake a installed resource waiting to start
    def mock_condition(*args, **kwargs):
        obj = mock.MagicMock(spec=args[0])
        obj.kind = args[0].__name__
        obj.metadata.name = args[1]
        obj.metadata.namespace = kwargs["namespace"]
        if hasattr(obj, "status"):
            obj.status.conditions = [AnyCondition(status="False", type="Ready")]
        return obj

    lk_client.get.side_effect = mock_condition
    harness.charm.on.update_status.emit()
    assert harness.charm.unit.status.name == "waiting"
    assert harness.charm.unit.status.message == ", ".join(
        [
            "rbd: DaemonSet/default/csi-rbdplugin is not Ready",
            "rbd: Deployment/default/csi-rbdplugin-provisioner is not Ready",
            "rbd: Service/default/csi-metrics-rbdplugin is not Ready",
            "rbd: Service/default/csi-rbdplugin-provisioner is not Ready",
        ]
    )

    lk_client.get.side_effect = None
    harness.update_config({"namespace": "different"})
    harness.charm.on.update_status.emit()
    assert harness.charm.unit.status.name == "blocked"
    assert (
        harness.charm.unit.status.message
        == "Namespace 'default' cannot be configured to 'different'"
    )


@pytest.mark.parametrize("leadership", (False, True))
@mock.patch("charm.CephCsiCharm.get_ceph_fsid", mock.MagicMock(return_value="12345"))
@mock.patch("charm.CephCsiCharm.get_ceph_fsname", mock.MagicMock(return_value=None))
@mock.patch("charm.CephCsiCharm.configure_ceph_cli", mock.MagicMock())
@mock.patch("charm.CephCsiCharm._check_kube_config", mock.MagicMock(return_value=True))
def test_ceph_client_relation_departed(harness, caplog, leadership):
    """Test that warning is logged about ceph-pools not being cleaned up after rel. removal."""

    # Operator testing harness does not provide a helper to "remove relation" so for now we'll
    # invoke method manually
    harness.begin()
    rel_id = harness.add_relation(CephCsiCharm.CEPH_CLIENT_RELATION, "ceph-mon")
    harness.set_leader(leadership)
    data = {
        "auth": "cephx",
        "key": "12345",
        "mon_hosts": '["10.0.0.1", "10.0.0.2"]',
        "ceph-public-address": "10.0.0.1",
    }
    harness.add_relation_unit(rel_id, "ceph-mon/0")
    harness.update_relation_data(rel_id, "ceph-mon/0", data)
    caplog.clear()

    pools = ", ".join(CephCsiCharm.REQUIRED_CEPH_POOLS)
    caplog.set_level(logging.INFO)
    harness.charm._on_ceph_client_removed(mock.MagicMock())
    if leadership:
        expected_msg = f"Ceph pools {pools} wont be removed."
    else:
        expected_msg = "Execution of function '_purge_all_manifests' skipped"
    assert expected_msg in caplog.text


@mock.patch("charm.CephCsiCharm._purge_all_manifests")
def test_cleanup(purge_all_manifests, harness, caplog):
    harness.begin()
    harness.add_relation(CephCsiCharm.CEPH_CLIENT_RELATION, "ceph-mon")
    harness.charm.stored.deployed = True
    caplog.set_level(logging.INFO)
    event = mock.MagicMock()
    harness.charm._cleanup(event)
    purge_all_manifests.assert_called_once_with(event)


@mock.patch("charm.ceph_client.CephClientRequires.get_relation_data")
@mock.patch("charm.CephCsiCharm.configure_ceph_cli", mock.MagicMock())
def test_safe_load_ceph_client_data(get_relation_data, harness):
    """Test that `safe_load_ceph_admin_data` method loads data properly.

    Data are expected to be stored in the StoredState only if all the required keys are present
    in the relation data.
    """
    auth = "cephx"
    key = "bar"
    ceph_mons = ["10.0.0.1", "10.0.0.2"]
    relation_data = {"auth": auth, "key": key, "mon_hosts": ceph_mons}
    get_relation_data.return_value = relation_data

    harness.begin_with_initial_hooks()

    # All data should be loaded
    assert harness.charm.safe_load_ceph_client_data()

    assert harness.charm.stored.ceph_data["auth"] == auth
    assert harness.charm.stored.ceph_data["key"] == key
    assert harness.charm.stored.ceph_data["mon_hosts"] == ceph_mons

    # reset
    harness.charm.stored.ceph_data = {}

    # don't load anything if relation data is missing
    get_relation_data.return_value = {"auth": auth, "key": key}

    assert not harness.charm.safe_load_ceph_client_data()
    assert "auth" not in harness.charm.stored.ceph_data
    assert "key" not in harness.charm.stored.ceph_data
    assert "mon_hosts" not in harness.charm.stored.ceph_data


@mock.patch("charm.CephCsiCharm.configure_ceph_cli", mock.MagicMock())
def test_action_list_versions(harness):
    harness.begin_with_initial_hooks()

    mock_event = mock.MagicMock()
    assert harness.charm._list_versions(mock_event) is None
    expected_results = {
        "cephfs-versions": "v3.7.2",
        "config-versions": "",
        "rbd-versions": "v3.7.2",
    }
    mock_event.set_results.assert_called_once_with(expected_results)


@mock.patch("charm.CephCsiCharm.configure_ceph_cli", mock.MagicMock())
@mock.patch("charm.CephCsiCharm.get_ceph_fsid", mock.MagicMock(return_value="12345"))
@mock.patch("charm.CephCsiCharm.get_ceph_fsname", mock.MagicMock(return_value=None))
def test_action_manifest_resources(harness):
    harness.begin_with_initial_hooks()

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
    harness.begin_with_initial_hooks()

    mock_event = mock.MagicMock()
    expected_results = {"result": "Failed to apply missing resources. API Server unavailable."}
    lk_client.apply.side_effect = ManifestClientError()
    assert harness.charm._sync_resources(mock_event) is None
    mock_event.set_results.assert_called_with(expected_results)


def test_signal_unit_waiting_status(harness):
    mock_event = mock.MagicMock()
    harness.begin_with_initial_hooks()
    harness.charm._ops_wait_for(mock_event, "time to wait")
    assert harness.charm.unit.status.name == "waiting"
    assert harness.charm.unit.status.message == "time to wait"
    mock_event.defer.assert_called_once()


def test_signal_unit_blocked_status(harness, caplog):
    harness.begin_with_initial_hooks()
    caplog.set_level(logging.ERROR)
    harness.charm._ops_blocked_by("time to block", exc_info=True)
    assert harness.charm.unit.status.name == "blocked"
    assert harness.charm.unit.status.message == "time to block"
    caplog.clear()


@mock.patch("charm.CephCsiCharm.ceph_cli", mock.MagicMock(side_effect=SubprocessError))
def test_failed_cli_fsid(harness):
    harness.begin_with_initial_hooks()
    assert harness.charm.get_ceph_fsid() == ""


@pytest.mark.parametrize("failure", [SubprocessError, lambda *_: "invalid"])
def test_failed_cli_fsname(harness, failure):
    harness.begin_with_initial_hooks()
    with mock.patch("charm.CephCsiCharm.ceph_cli", side_effect=failure):
        assert harness.charm.get_ceph_fsname() is None


def test_cli_fsname(request, harness):
    harness.begin_with_initial_hooks()
    pools = json.dumps([{"data_pools": ["ceph-fs_data"], "name": request.node.name}])
    with mock.patch("charm.CephCsiCharm.ceph_cli", return_value=pools):
        assert harness.charm.get_ceph_fsname() == request.node.name


def test_check_kube_config(harness):
    mock_event = mock.MagicMock()
    harness.begin_with_initial_hooks()
    with mock.patch("charm.KubeConfig.from_env", side_effect=ConfigError):
        assert not harness.charm._check_kube_config(mock_event)
    assert harness.charm.unit.status.name == "waiting"
    assert harness.charm.unit.status.message == "Waiting for kubeconfig"

    with mock.patch("charm.KubeConfig.from_env"):
        assert harness.charm._check_kube_config(mock_event)
    assert harness.charm.unit.status.name == "maintenance"
    assert harness.charm.unit.status.message == "Evaluating kubernetes authentication"
