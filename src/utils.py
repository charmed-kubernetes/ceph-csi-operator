# Copyright 2025 Canonical
# See LICENSE file for licensing details.

import configparser
import json
import logging
import subprocess
from functools import lru_cache
from pathlib import Path
from typing import Any, Protocol

from manifests_cephfs import CephFilesystem

CONFIG_DIR = Path("ceph-conf").resolve()
CONFIG_PATH = CONFIG_DIR / "ceph.conf"
log = logging.getLogger(__name__)


class CharmLike(Protocol):
    """Protocol for a charm-like object"""

    @property
    def app(self) -> Any: ...

    @property
    def auth(self) -> str | None: ...

    @property
    def key(self) -> str | None: ...

    @property
    def unit(self) -> Any: ...

    @property
    def mon_hosts(self) -> list[str]: ...


def _keyring_path(user: str) -> Path:
    """Return the path to the keyring file for a given user"""
    return CONFIG_DIR / f"ceph.client.{user}.keyring"


class CephCLI:
    """Ceph CLI wrapper for configuration and command execution"""

    def __init__(self, charm: CharmLike) -> None:
        """Initialize the CephCLI with the charm instance"""
        self._charm = charm

    def configure(self) -> None:
        """Create the ceph.conf and keyring files"""
        CONFIG_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._write_config()
        self._write_keyring()

    def command(self, *args: str, timeout: int = 60) -> str:
        """Run a command and return the output"""
        user = self._charm.app.name
        cmd = ["/usr/bin/ceph", "--conf", CONFIG_PATH.as_posix(), "--user", user, *args]
        return subprocess.check_output(cmd, timeout=timeout).decode("UTF-8")

    def command_json(self, *args: str, timeout: int = 60) -> Any:
        """Run a command and return the JSON output"""
        result = self.command("--format", "json", *args, timeout=timeout)
        return json.loads(result)

    def _write_config(self) -> None:
        """Write Ceph CLI .conf file"""
        config = configparser.ConfigParser()
        unit_name = self._charm.unit.name.replace("/", "-")
        config["global"] = {
            "auth cluster required": self._charm.auth or "",
            "auth service required": self._charm.auth or "",
            "auth client required": self._charm.auth or "",
            "keyring": f"{CONFIG_DIR}/$cluster.$name.keyring",
            "mon host": " ".join(self._charm.mon_hosts),
            "log to syslog": "true",
            "err to syslog": "true",
            "clog to syslog": "true",
            "mon cluster log to syslog": "true",
            "debug mon": "1/5",
            "debug osd": "1/5",
        }
        config["client"] = {"log file": f"/var/log/ceph/{unit_name}.log"}

        with CONFIG_PATH.open("w") as fp:
            config.write(fp)

    def _write_keyring(self) -> None:
        """Write Ceph CLI keyring file"""
        config = configparser.ConfigParser()
        user = self._charm.app.name
        config[f"client.{user}"] = {"key": self._charm.key or ""}
        with _keyring_path(user).open("w") as fp:
            config.write(fp)


@lru_cache(maxsize=None)
def fsid(cli: CephCLI) -> str:
    """Get the Ceph FSID (cluster ID)"""
    try:
        return cli.command("fsid").strip()
    except subprocess.SubprocessError:
        log.error("get_ceph_fsid: Failed to get CephFS ID, reporting as empty string")
        return ""


@lru_cache(maxsize=None)
def ls_ceph_fs(cli: CephCLI) -> list[CephFilesystem]:
    """Get a list of CephFS names and list of associated pools."""
    try:
        data = cli.command_json("fs", "ls")
    except (subprocess.SubprocessError, ValueError) as e:
        log.error("ls_ceph_fs: Failed to find CephFS name, reporting as None, error: %s", e)
        data = []

    return [CephFilesystem(**fs) for fs in data]


def ensure_subvolumegroups(cli: CephCLI, volume: str, groups: set[str]) -> None:
    """Get a list of CephFS subvolumegroups."""
    try:
        data = cli.command_json("fs", "subvolumegroup", "ls", volume)
        current = set(group["name"] for group in data)
        missing = groups - current
        for group in missing:
            cli.command("fs", "subvolumegroup", "create", volume, group)
    except (subprocess.SubprocessError, ValueError) as e:
        log.error(
            "ensure_subvolumegroups (%s): Failed to ensure CephFS subvolumegroups, error: %s",
            volume,
            e,
        )
