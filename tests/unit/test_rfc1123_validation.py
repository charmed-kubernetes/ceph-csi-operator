# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Unit tests for RFC1123 validation in manifests."""

from unittest.mock import MagicMock

import pytest
from lightkube.resources.storage_v1 import StorageClass

from manifests_base import ValidateResourceNames


class TestValidateResourceNamesPatc:
    """Test the ValidateResourceNames patch class."""

    @pytest.fixture
    def mock_manifests(self):
        """Create a mock manifests object."""
        return MagicMock()

    @pytest.fixture
    def validator(self, mock_manifests):
        """Create a ValidateResourceNames instance."""
        return ValidateResourceNames(mock_manifests)

    def test_valid_storage_class_name(self, validator):
        """Test that valid StorageClass names pass validation."""
        obj = StorageClass.from_dict(
            {
                "metadata": {"name": "cephfs-pool"},
                "provisioner": "cephfs.csi.ceph.com",
            }
        )

        # Should not raise
        validator(obj)

    def test_invalid_storage_class_name_with_underscore(self, validator):
        """Test that StorageClass names with underscores fail validation."""
        obj = StorageClass.from_dict(
            {
                "metadata": {"name": "cephfs-fs_data"},
                "provisioner": "cephfs.csi.ceph.com",
            }
        )

        with pytest.raises(ValueError, match="Invalid Kubernetes resource name"):
            validator(obj)

    def test_invalid_storage_class_name_with_uppercase(self, validator):
        """Test that StorageClass names with uppercase fail validation."""
        obj = StorageClass.from_dict(
            {
                "metadata": {"name": "CephFS-pool"},
                "provisioner": "cephfs.csi.ceph.com",
            }
        )

        with pytest.raises(ValueError, match="Invalid Kubernetes resource name"):
            validator(obj)

    def test_invalid_storage_class_name_ending_with_hyphen(self, validator):
        """Test that StorageClass names ending with hyphen fail validation."""
        obj = StorageClass.from_dict(
            {
                "metadata": {"name": "cephfs-pool-"},
                "provisioner": "cephfs.csi.ceph.com",
            }
        )

        with pytest.raises(ValueError, match="Invalid Kubernetes resource name"):
            validator(obj)

    def test_object_without_metadata(self, validator):
        """Test that objects without metadata are skipped."""
        obj = MagicMock()
        obj.metadata = None

        # Should not raise
        validator(obj)

    def test_object_without_name(self, validator):
        """Test that objects without name are skipped."""
        obj = MagicMock()
        obj.metadata = MagicMock()
        obj.metadata.name = None

        # Should not raise
        validator(obj)

    def test_specific_issue_scenario(self, validator):
        """Test the specific scenario from the GitHub issue."""
        # This is the exact problematic name from the issue:
        # "cephfs-ceph-fs-ceph-fs_data"
        obj = StorageClass.from_dict(
            {
                "metadata": {"name": "cephfs-ceph-fs-ceph-fs_data"},
                "provisioner": "cephfs.csi.ceph.com",
            }
        )

        with pytest.raises(ValueError, match="underscores"):
            validator(obj)
