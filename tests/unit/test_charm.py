# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing
"""Collection of tests related to src/charm.py"""

import logging
import unittest.mock as mock

import pytest
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


@mock.patch("charm.CephCsiCharm.configure_ceph_cli", mock.MagicMock())
@mock.patch("charm.CephCsiCharm._check_kube_config", return_value=False)
def test_required_relation_check(check_kube_config, harness):
    """Test that check_required_relations sets expected unit states."""

    # Add ceph-mon relation, indicating existing relations
    harness.begin()
    assert not harness.charm._check_required_relations()

    harness.charm.unit.status.name == "blocked"
    harness.charm.unit.status.message == "Missing relations: ceph-client"

    rel_id = harness.add_relation(CephCsiCharm.CEPH_CLIENT_RELATION, "ceph-mon")
    assert not harness.charm._check_required_relations()

    harness.charm.unit.status.name == "waiting"
    harness.charm.unit.status.message == "Ceph relation is missing data."

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


@mock.patch("charm.CephCsiCharm.get_ceph_fsid", mock.MagicMock(return_value="12345"))
@mock.patch("charm.CephCsiCharm.get_ceph_fsname", mock.MagicMock(return_value=None))
@mock.patch("charm.CephCsiCharm.configure_ceph_cli", mock.MagicMock())
@mock.patch("charm.CephCsiCharm._check_kube_config", mock.MagicMock(return_value=True))
def test_ceph_client_relation_departed(harness, caplog):
    """Test that warning is logged about ceph-pools not being cleaned up after rel. removal."""

    # Operator testing harness does not provide a helper to "remove relation" so for now we'll
    # invoke method manually
    harness.begin()
    rel_id = harness.add_relation(CephCsiCharm.CEPH_CLIENT_RELATION, "ceph-mon")
    harness.set_leader(True)
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
    caplog.set_level(logging.WARNING)
    expected_msg = (
        f"Ceph pools {pools} wont be removed. If you want to clean up pools manually, "
        "use juju action 'delete-pool' on 'ceph-mon' units"
    )
    harness.charm._on_ceph_client_removed(mock.MagicMock())
    assert expected_msg in caplog.text


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
