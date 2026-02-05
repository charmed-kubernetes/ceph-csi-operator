# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Unit tests for Kubernetes resource name validation."""

import pytest

from k8s_name_validator import NameValidationError, get_validation_error, validate_resource_name


class TestValidateResourceName:
    """Test the validate_resource_name function."""

    def test_valid_simple_name(self):
        """Test that simple valid names pass validation."""
        validate_resource_name("my-app", "StorageClass")
        validate_resource_name("app123", "StorageClass")
        validate_resource_name("123app", "StorageClass")
        validate_resource_name("a", "StorageClass")
        validate_resource_name("0", "StorageClass")

    def test_valid_name_with_dots(self):
        """Test that names with dots pass validation."""
        validate_resource_name("my.app", "StorageClass")
        validate_resource_name("app.example.com", "StorageClass")
        validate_resource_name("storage.v1.class", "StorageClass")

    def test_valid_name_with_hyphens_and_dots(self):
        """Test that names with hyphens and dots pass validation."""
        validate_resource_name("my-app.example", "StorageClass")
        validate_resource_name("cephfs-ceph-fs.data", "StorageClass")
        validate_resource_name("a-b-c.d-e-f", "StorageClass")

    def test_valid_max_length_name(self):
        """Test that 253 character names pass validation."""
        long_name = "a" * 253
        validate_resource_name(long_name, "StorageClass")

    def test_invalid_empty_name(self):
        """Test that empty names fail validation."""
        with pytest.raises(NameValidationError, match="cannot be empty"):
            validate_resource_name("", "StorageClass")

    def test_invalid_none_name(self):
        """Test that None names fail validation."""
        with pytest.raises(NameValidationError, match="cannot be empty"):
            validate_resource_name(None, "StorageClass")

    def test_invalid_too_long_name(self):
        """Test that names over 253 characters fail validation."""
        long_name = "a" * 254
        with pytest.raises(NameValidationError, match="too long"):
            validate_resource_name(long_name, "StorageClass")

    def test_invalid_underscore_in_name(self):
        """Test that names with underscores fail validation."""
        with pytest.raises(NameValidationError, match="underscores"):
            validate_resource_name("my_app", "StorageClass")

        with pytest.raises(NameValidationError, match="underscores"):
            validate_resource_name("cephfs-fs_data", "StorageClass")

    def test_invalid_uppercase_in_name(self):
        """Test that names with uppercase letters fail validation."""
        with pytest.raises(NameValidationError, match="must begin"):
            validate_resource_name("MyApp", "StorageClass")

        with pytest.raises(NameValidationError, match="uppercase"):
            validate_resource_name("my-App", "StorageClass")

    def test_invalid_space_in_name(self):
        """Test that names with spaces fail validation."""
        with pytest.raises(NameValidationError, match="spaces"):
            validate_resource_name("my app", "StorageClass")

    def test_invalid_start_with_hyphen(self):
        """Test that names starting with hyphen fail validation."""
        with pytest.raises(NameValidationError, match="must begin"):
            validate_resource_name("-myapp", "StorageClass")

    def test_invalid_start_with_dot(self):
        """Test that names starting with dot fail validation."""
        with pytest.raises(NameValidationError, match="must begin"):
            validate_resource_name(".myapp", "StorageClass")

    def test_invalid_end_with_hyphen(self):
        """Test that names ending with hyphen fail validation."""
        with pytest.raises(NameValidationError, match="must end"):
            validate_resource_name("myapp-", "StorageClass")

    def test_invalid_end_with_dot(self):
        """Test that names ending with dot fail validation."""
        with pytest.raises(NameValidationError, match="must end"):
            validate_resource_name("myapp.", "StorageClass")

    def test_invalid_special_characters(self):
        """Test that names with special characters fail validation."""
        invalid_names = [
            "my@app",
            "my#app",
            "my$app",
            "my%app",
            "my&app",
            "my*app",
            "my+app",
            "my=app",
            "my[app",
            "my]app",
        ]
        for name in invalid_names:
            with pytest.raises(NameValidationError, match="invalid characters"):
                validate_resource_name(name, "StorageClass")

    def test_error_message_includes_resource_type(self):
        """Test that error messages include the resource type."""
        with pytest.raises(NameValidationError, match="ClusterRole"):
            validate_resource_name("invalid_name", "ClusterRole")

        with pytest.raises(NameValidationError, match="ConfigMap"):
            validate_resource_name("Invalid", "ConfigMap")


class TestGetValidationError:
    """Test the get_validation_error function."""

    def test_valid_name_returns_none(self):
        """Test that valid names return None."""
        assert get_validation_error("my-app", "StorageClass") is None
        assert get_validation_error("app.example", "StorageClass") is None

    def test_invalid_name_returns_error_string(self):
        """Test that invalid names return error messages."""
        error = get_validation_error("my_app", "StorageClass")
        assert error is not None
        assert "underscores" in error

        error = get_validation_error("my-App", "StorageClass")
        assert error is not None
        assert "uppercase" in error

    def test_empty_name_returns_error(self):
        """Test that empty names return error messages."""
        error = get_validation_error("", "StorageClass")
        assert error is not None
        assert "empty" in error

    def test_error_string_contains_name(self):
        """Test that error messages contain the invalid name."""
        error = get_validation_error("bad_name", "StorageClass")
        assert "bad_name" in error


class TestRealWorldScenarios:
    """Test real-world scenarios from the issue."""

    def test_cephfs_pool_with_underscore(self):
        """Test the specific scenario from the issue: fs_data pool name."""
        # This is the problematic name from the issue
        invalid_name = "cephfs-ceph-fs-ceph-fs_data"

        with pytest.raises(NameValidationError, match="underscores"):
            validate_resource_name(invalid_name, "StorageClass")

        error = get_validation_error(invalid_name, "StorageClass")
        assert error is not None
        assert "fs_data" in invalid_name  # The pool name causes the issue
        assert "underscores" in error

    def test_valid_alternative_without_underscore(self):
        """Test that the corrected name (with hyphen) is valid."""
        # Corrected version with hyphen instead of underscore
        valid_name = "cephfs-ceph-fs-ceph-fs-data"
        validate_resource_name(valid_name, "StorageClass")
        assert get_validation_error(valid_name, "StorageClass") is None

    def test_various_pool_name_patterns(self):
        """Test various pool naming patterns that might appear."""
        # Valid patterns
        valid_patterns = [
            "cephfs-pool1",
            "cephfs-my-pool",
            "rbd-xfs-pool",
            "rbd-ext4-pool",
        ]
        for pattern in valid_patterns:
            validate_resource_name(pattern, "StorageClass")

        # Invalid patterns
        invalid_patterns = [
            "cephfs_pool1",  # underscore
            "CephFS-pool",  # uppercase
            "cephfs-pool_1",  # underscore
        ]
        for pattern in invalid_patterns:
            with pytest.raises(NameValidationError):
                validate_resource_name(pattern, "StorageClass")
