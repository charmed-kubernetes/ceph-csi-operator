# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Unit tests for src/ceph_csi.py"""

import json

import pytest
from ops.charm import CharmBase
from ops.testing import Harness

from ceph_csi import CephCSIRequires


class MockCharm(CharmBase):
    """Mock charm for testing CephCSIRequires."""

    def __init__(self, *args):
        super().__init__(*args)
        self.ceph_csi = CephCSIRequires(self, "ceph")
        self.events = []

        self.framework.observe(
            self.ceph_csi.on.ceph_csi_available,
            self._on_ceph_csi_available,
        )
        self.framework.observe(
            self.ceph_csi.on.ceph_csi_connected,
            self._on_ceph_csi_connected,
        )
        self.framework.observe(
            self.ceph_csi.on.ceph_csi_departed,
            self._on_ceph_csi_departed,
        )

    def _on_ceph_csi_available(self, event):
        self.events.append("available")

    def _on_ceph_csi_connected(self, event):
        self.events.append("connected")

    def _on_ceph_csi_departed(self, event):
        self.events.append("departed")


@pytest.fixture
def harness():
    harness = Harness(
        MockCharm,
        meta="""
        name: test-charm
        requires:
          ceph:
            interface: ceph-csi
    """,
    )
    try:
        yield harness
    finally:
        harness.cleanup()


def test_ceph_csi_requires_init(harness):
    """Test CephCSIRequires initialization."""
    harness.begin()
    assert harness.charm.ceph_csi.charm == harness.charm
    assert harness.charm.ceph_csi.relation_name == "ceph"


def test_ceph_csi_available_event(harness):
    """Test ceph_csi_available event is emitted when relation is added."""
    harness.begin()
    relation_id = harness.add_relation("ceph", "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    # Without required data, should emit available event
    # Need to update with at least some data to trigger the event
    harness.update_relation_data(relation_id, "microceph", {"fsid": "test"})
    assert "available" in harness.charm.events


def test_ceph_csi_connected_event(harness):
    """Test ceph_csi_connected event is emitted when all required data is present."""
    harness.begin()
    relation_id = harness.add_relation("ceph", "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    # With all required data, should emit connected event
    harness.update_relation_data(
        relation_id,
        "microceph",
        {
            "fsid": "test-fsid-123",
            "mon_hosts": '["10.0.0.1:6789", "10.0.0.2:6789"]',
            "user_id": "ceph-csi",
            "user_key": "AQDtest123==",
        },
    )
    assert "connected" in harness.charm.events


def test_ceph_csi_departed_event(harness):
    """Test ceph_csi_departed event is emitted when relation is broken."""
    harness.begin()
    relation_id = harness.add_relation("ceph", "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    harness.remove_relation(relation_id)
    assert "departed" in harness.charm.events


def test_is_data_ready_with_all_required_keys(harness):
    """Test _is_data_ready returns True when all required keys are present."""
    harness.begin()
    relation_id = harness.add_relation("ceph", "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    harness.update_relation_data(
        relation_id,
        "microceph",
        {
            "fsid": "test-fsid",
            "mon_hosts": '["10.0.0.1"]',
            "user_id": "ceph-csi",
            "user_key": "secret",
        },
    )

    relation = harness.charm.model.get_relation("ceph")
    assert harness.charm.ceph_csi._is_data_ready(relation) is True


def test_is_data_ready_with_missing_keys(harness):
    """Test _is_data_ready returns False when required keys are missing."""
    harness.begin()
    relation_id = harness.add_relation("ceph", "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    # Missing user_key
    harness.update_relation_data(
        relation_id,
        "microceph",
        {
            "fsid": "test-fsid",
            "mon_hosts": '["10.0.0.1"]',
            "user_id": "ceph-csi",
        },
    )

    relation = harness.charm.model.get_relation("ceph")
    assert harness.charm.ceph_csi._is_data_ready(relation) is False


def test_is_data_ready_with_empty_relation_data(harness):
    """Test _is_data_ready returns False when relation data is empty."""
    harness.begin()
    relation_id = harness.add_relation("ceph", "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    relation = harness.charm.model.get_relation("ceph")
    assert harness.charm.ceph_csi._is_data_ready(relation) is False


def test_request_workloads_as_leader(harness):
    """Test request_workloads writes workload data when unit is leader."""
    harness.set_leader(True)
    harness.begin()
    relation_id = harness.add_relation("ceph", "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    harness.charm.ceph_csi.request_workloads(["rbd", "cephfs"])

    relation = harness.charm.model.get_relation("ceph")
    unit_data = relation.data[harness.charm.unit]
    assert "workloads" in unit_data
    assert json.loads(unit_data["workloads"]) == ["rbd", "cephfs"]


def test_request_workloads_as_non_leader(harness):
    """Test request_workloads does nothing when unit is not leader."""
    harness.set_leader(False)
    harness.begin()
    relation_id = harness.add_relation("ceph", "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    harness.charm.ceph_csi.request_workloads(["rbd", "cephfs"])

    relation = harness.charm.model.get_relation("ceph")
    unit_data = relation.data[harness.charm.unit]
    assert "workloads" not in unit_data


def test_request_workloads_without_relation(harness):
    """Test request_workloads does nothing when no relation exists."""
    harness.set_leader(True)
    harness.begin()

    # This should not raise an error
    harness.charm.ceph_csi.request_workloads(["rbd", "cephfs"])


def test_get_relation_data_with_full_data(harness):
    """Test get_relation_data returns complete data when all fields are present."""
    harness.begin()
    relation_id = harness.add_relation("ceph", "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    harness.update_relation_data(
        relation_id,
        "microceph",
        {
            "fsid": "test-fsid-456",
            "mon_hosts": '["10.0.0.1:6789", "10.0.0.2:6789"]',
            "rbd_pool": "kubernetes",
            "cephfs_fs_name": "cephfs",
            "user_id": "ceph-csi",
            "user_key": "AQDtest456==",
        },
    )

    data = harness.charm.ceph_csi.get_relation_data()
    assert data["fsid"] == "test-fsid-456"
    assert data["mon_hosts"] == ["10.0.0.1:6789", "10.0.0.2:6789"]
    assert data["rbd_pool"] == "kubernetes"
    assert data["cephfs_fs_name"] == "cephfs"
    assert data["user_id"] == "ceph-csi"
    assert data["user_key"] == "AQDtest456=="


def test_get_relation_data_with_partial_data(harness):
    """Test get_relation_data returns partial data with None for missing fields."""
    harness.begin()
    relation_id = harness.add_relation("ceph", "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    harness.update_relation_data(
        relation_id,
        "microceph",
        {
            "fsid": "test-fsid",
            "mon_hosts": '["10.0.0.1"]',
            "user_id": "ceph-csi",
            "user_key": "secret",
        },
    )

    data = harness.charm.ceph_csi.get_relation_data()
    assert data["fsid"] == "test-fsid"
    assert data["mon_hosts"] == ["10.0.0.1"]
    assert data["rbd_pool"] is None
    assert data["cephfs_fs_name"] is None
    assert data["user_id"] == "ceph-csi"
    assert data["user_key"] == "secret"


def test_get_relation_data_without_relation(harness):
    """Test get_relation_data returns empty dict when no relation exists."""
    harness.begin()

    data = harness.charm.ceph_csi.get_relation_data()
    assert data == {}


def test_get_relation_data_with_empty_relation_data(harness):
    """Test get_relation_data returns empty dict when relation data is empty."""
    harness.begin()
    relation_id = harness.add_relation("ceph", "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    data = harness.charm.ceph_csi.get_relation_data()
    assert data == {}


def test_relation_property_with_relation(harness):
    """Test _relation property returns relation when it exists."""
    harness.begin()
    relation_id = harness.add_relation("ceph", "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    relation = harness.charm.ceph_csi._relation
    assert relation is not None
    assert relation.name == "ceph"


def test_relation_property_without_relation(harness):
    """Test _relation property returns None when no relation exists."""
    harness.begin()

    relation = harness.charm.ceph_csi._relation
    assert relation is None


def test_request_workloads_with_different_workload_combinations(harness):
    """Test request_workloads with various workload combinations."""
    harness.set_leader(True)
    harness.begin()
    relation_id = harness.add_relation("ceph", "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    # Test with single workload
    harness.charm.ceph_csi.request_workloads(["rbd"])
    relation = harness.charm.model.get_relation("ceph")
    unit_data = relation.data[harness.charm.unit]
    assert json.loads(unit_data["workloads"]) == ["rbd"]

    # Test with multiple workloads
    harness.charm.ceph_csi.request_workloads(["rbd", "cephfs"])
    unit_data = relation.data[harness.charm.unit]
    assert json.loads(unit_data["workloads"]) == ["rbd", "cephfs"]

    # Test with empty list
    harness.charm.ceph_csi.request_workloads([])
    unit_data = relation.data[harness.charm.unit]
    assert json.loads(unit_data["workloads"]) == []


def test_get_relation_data_handles_invalid_json(harness):
    """Test get_relation_data handles invalid JSON gracefully."""
    harness.begin()
    relation_id = harness.add_relation("ceph", "microceph")
    harness.add_relation_unit(relation_id, "microceph/0")

    # Update with invalid JSON for mon_hosts
    harness.update_relation_data(
        relation_id,
        "microceph",
        {
            "fsid": "test-fsid",
            "mon_hosts": "not-valid-json",
            "user_id": "ceph-csi",
            "user_key": "secret",
        },
    )

    # Should raise an error or return empty list, depending on implementation
    # Since the implementation doesn't handle JSON errors, this will raise
    with pytest.raises(json.JSONDecodeError):
        harness.charm.ceph_csi.get_relation_data()
