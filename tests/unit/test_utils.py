import json
import subprocess
import unittest.mock as mock

import utils
from manifests_cephfs import CephFilesystem


def test_write_ceph_cli_config(ceph_conf_directory):
    """Test writing of Ceph CLI config"""
    charm = mock.MagicMock()
    charm.auth = "cephx"
    charm.mon_hosts = ["10.0.0.1", "10.0.0.2"]
    charm.key = "12345"
    charm.app.name = "ceph-csi"
    charm.ceph_user = "ceph-csi"
    charm.unit.name = "ceph-csi/0"

    cli = utils.CephCLI(charm)
    cli.configure()

    unit_name = charm.unit.name.replace("/", "-")
    lines = [
        "[global]",
        "auth cluster required = cephx",
        "auth service required = cephx",
        "auth client required = cephx",
        f"keyring = {ceph_conf_directory}/$cluster.$name.keyring",
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
    assert utils.CONFIG_PATH.read_text() == "\n".join(lines) + "\n"

    path = utils._keyring_path(charm.ceph_user)
    lines = ["[client.ceph-csi]", "key = 12345", ""]
    assert path.read_text() == "\n".join(lines) + "\n"


def test_failed_cli_fsid():
    charm = mock.MagicMock()
    cli = utils.CephCLI(charm)

    with mock.patch("utils.CephCLI.command", side_effect=subprocess.SubprocessError) as cmd:
        utils.fsid.cache_clear()
        assert utils.fsid(cli) == ""
    cmd.assert_called_once_with("fsid")

    with mock.patch("utils.CephCLI.command", mock.MagicMock(return_value="invalid")) as cmd:
        utils.fsid.cache_clear()
        assert utils.fsid(cli) == "invalid"
    cmd.assert_called_once_with("fsid")


def test_failed_cli_fs_list():
    charm = mock.MagicMock()
    cli = utils.CephCLI(charm)

    with mock.patch("utils.CephCLI.command", side_effect=subprocess.SubprocessError) as cmd:
        utils.ls_ceph_fs.cache_clear()
        assert utils.ls_ceph_fs(cli) == []
    cmd.assert_called_once_with("--format", "json", "fs", "ls", timeout=60)

    with mock.patch("utils.CephCLI.command", mock.MagicMock(return_value="invalid")) as cmd:
        utils.ls_ceph_fs.cache_clear()
        assert utils.ls_ceph_fs(cli) == []
    cmd.assert_called_once_with("--format", "json", "fs", "ls", timeout=60)


def test_cli_fsdata(request):
    charm = mock.MagicMock()
    cli = utils.CephCLI(charm)

    fs_name = request.node.name
    fs_data = [
        {
            "name": f"{fs_name}-alt",
            "metadata_pool": f"{fs_name}-alt_metadata",
            "metadata_pool_id": 5,
            "data_pool_ids": [4],
            "data_pools": [f"{fs_name}-alt_data"],
        }
    ]
    pools = json.dumps(fs_data)
    with mock.patch("utils.CephCLI.command", return_value=pools):
        assert utils.ls_ceph_fs(cli) == [CephFilesystem(**fs) for fs in fs_data]


def test_cli_ensure_subvolumegroups():
    charm = mock.MagicMock()
    cli = utils.CephCLI(charm)

    with mock.patch("utils.CephCLI.command_json", return_value=[]) as json:
        with mock.patch("utils.CephCLI.command", return_value=[]) as cmd:
            utils.ensure_subvolumegroups(cli, "ceph-fs", {"group1", "group2"})

    json.assert_called_once_with("fs", "subvolumegroup", "ls", "ceph-fs")
    cmd.assert_any_call("fs", "subvolumegroup", "create", "ceph-fs", "group1")
    cmd.assert_any_call("fs", "subvolumegroup", "create", "ceph-fs", "group2")


def test_failed_cli_ensure_subvolumegroups():
    charm = mock.MagicMock()
    cli = utils.CephCLI(charm)

    with mock.patch("utils.CephCLI.command", return_value=[]) as cmd:
        with mock.patch(
            "utils.CephCLI.command_json", side_effect=subprocess.SubprocessError
        ) as json:
            utils.ensure_subvolumegroups(cli, "ceph-fs", {"group1", "group2"})

        json.assert_called_once_with("fs", "subvolumegroup", "ls", "ceph-fs")
        cmd.assert_not_called()
        cmd.reset_mock()

        with mock.patch("utils.CephCLI.command_json", side_effect=ValueError) as json:
            utils.ensure_subvolumegroups(cli, "ceph-fs", {"group1", "group2"})

        json.assert_called_once_with("fs", "subvolumegroup", "ls", "ceph-fs")
        cmd.assert_not_called()
        cmd.reset_mock()
