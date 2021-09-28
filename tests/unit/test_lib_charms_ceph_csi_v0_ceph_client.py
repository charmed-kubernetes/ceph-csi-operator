# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
#
# Learn more at: https://juju.is/docs/sdk
"""Tests for library lib.charms.ceph_csi.v0.ceph_client."""

import json
import unittest
from unittest.mock import MagicMock
from uuid import uuid4

from charms.ceph_csi.v0.ceph_client import (
    CephRequest,
    CommonPoolConfig,
    CreatePoolConfig,
    RequestError,
)


class TestCommonPoolConfig(unittest.TestCase):
    """Tests for lib.charms.ceph_csi.v0.ceph_client.CommonPoolConfig class."""

    EXPECTED_ATTRIBUTES = {  # mapping of expected attributes to their json representation
        "app_name": "app-name",
        "group": "group",
        "max_bytes": "max-bytes",
        "max_objects": "max-objects",
        "group_namespace": "group-namespace",
        "rbd_mirroring_mode": "rbd-mirroring-mode",
        "weight": "weight",
        "compression_algorithm": "compression-algorithm",
        "compression_mode": "compression-mode",
        "compression_required_ratio": "compression-required-ratio",
        "compression_min_blob_size": "compression-min-blob-size",
        "compression_min_blob_size_hdd": "compression-min-blob-size-hdd",
        "compression_min_blob_size_ssd": "compression-min-blob-size-ssd",
        "compression_max_blob_size": "compression-max-blob-size",
        "compression_max_blob_size_hdd": "compression-max-blob-size-hdd",
        "compression_max_blob_size_ssd": "compression-max-blob-size-ssd",
    }

    def test_expected_attributes(self):
        """Test that CommonPoolConfig instance has expected attributes."""
        config = CommonPoolConfig()
        instance_attributes = vars(config).keys()

        self.assertEqual(instance_attributes, self.EXPECTED_ATTRIBUTES.keys())

    def test_attribute_assignment(self):
        """Test that initialization of every expected CommonPoolConfig attribute works."""
        args = {attr: uuid4() for attr in self.EXPECTED_ATTRIBUTES}

        config = CommonPoolConfig(**args)

        for key, value in args.items():
            self.assertEqual(config.__getattribute__(key), value)

    def test_to_json_serialization(self):
        """Test that to_json() serialization returns expected dictionary.

        Keys in the returned dictionary should use hyphenated variable names as expected by ceph
        broker.
        """
        expected_output = {"op": CommonPoolConfig.OP}  # Operation name is explicitly inserted
        init_arguments = {}

        for class_attribute, json_key in self.EXPECTED_ATTRIBUTES.items():
            value = uuid4()
            expected_output[json_key] = value
            init_arguments[class_attribute] = value

        config = CommonPoolConfig(**init_arguments)

        self.assertEqual(config.to_json(), expected_output)


class TestCreatePoolConfig(unittest.TestCase):
    """Tests for lib.charms.ceph_csi.v0.ceph_client.CreatePoolConfig class."""

    COMMON_ATTRIBUTES = TestCommonPoolConfig.EXPECTED_ATTRIBUTES.copy()
    SPECIFIC_ATTRIBUTES = {
        "name": "name",
        "replicas": "replicas",
        "pg_num": "pg_num",  # This is not a typo, pg_num is expected with underscore in json too.
    }

    @property
    def expected_attributes(self) -> dict:
        """Return all expected attributes of CreateCephPool instance,"""
        return {**self.COMMON_ATTRIBUTES, **self.SPECIFIC_ATTRIBUTES}

    def test_expected_attributes(self):
        """Test that CreateCephPool instance has expected attributes."""
        pool = CreatePoolConfig("foo")
        instance_attributes = vars(pool).keys()

        self.assertEqual(instance_attributes, self.expected_attributes.keys())

    def test_json_serialization(self):
        """Test that to_json() serialization returns expected dictionary.

        Keys in the returned dictionary should use hyphenated variable names as expected by ceph
        broker. Only exception is "pg_num" key which is expected with underscore.
        """
        expected_output = {"op": CreatePoolConfig.OP}
        init_arguments = {}

        for class_attribute, json_key in self.expected_attributes.items():
            value = uuid4()
            expected_output[json_key] = value
            init_arguments[class_attribute] = value

        pool = CreatePoolConfig(**init_arguments)
        self.assertEqual(pool.to_json(), expected_output)


class TestCephRequest(unittest.TestCase):
    """Tests for lib.charms.ceph_csi.v0.ceph_client.CephRequest class."""

    def test_id_generation(self):
        """Test that if not supplied, new UUIDv4 is generated as request ID."""
        request_1 = CephRequest(MagicMock(), MagicMock())
        request_2 = CephRequest(MagicMock(), MagicMock())
        expected_uuid_len = len(str(uuid4()))

        self.assertNotEqual(request_1.uuid, request_2.uuid)
        self.assertIsInstance(request_1.uuid, str)
        self.assertEqual(len(request_1.uuid), expected_uuid_len)

    def test_add_operation(self):
        """Test adding operation to the request."""
        operation = {"name": "foo", "data": "bar"}
        request = CephRequest(MagicMock(), MagicMock())

        request.add_op(operation)

        self.assertEqual(len(request.ops), 1)
        self.assertEqual(request.ops[0], operation)

        # Adding same operation second time should not have any effect
        request.add_op(operation)
        self.assertEqual(len(request.ops), 1)

        # New operation can be added
        new_op = {"name": "baz", "data": None}
        request.add_op(new_op)
        self.assertEqual(len(request.ops), 2)
        self.assertIn(operation, request.ops)
        self.assertIn(new_op, request.ops)

    def test_serialization(self):
        """Test that `request` attribute of CephRequest returns expected dictionary."""
        api_version = 1
        id_ = str(uuid4())
        operation = {"name": "foo", "data": "bar"}

        expected_output = {"api-version": api_version, "request-id": id_, "ops": [operation]}

        request = CephRequest(MagicMock(), MagicMock(), id_, api_version)
        request.add_op(operation)

        self.assertEqual(request.request, expected_output)

    def test_execute(self):
        """Test that execute() method stores expected data into the relation with ceph-mon."""
        relation = MagicMock()
        unit = "cph-csi/0"
        relation.data = {unit: {}}
        api_version = 1
        id_ = str(uuid4())
        operation = {"name:": "bar", "data": "foo"}

        expected_output = json.dumps(
            {"api-version": api_version, "request-id": id_, "ops": [operation]}
        )
        request = CephRequest(unit, relation, id_, api_version)
        request.add_op(operation)

        request.execute()

        self.assertEqual(relation.data[unit]["broker_req"], expected_output)

    def test_execute_raises_on_empty_ops(self):
        """Test that executing request without operations raises exception."""
        request = CephRequest(MagicMock(), MagicMock())

        with self.assertRaises(RequestError):
            request.execute()

    def test_add_replicated_pool(self):
        """Test that add_replicated_pool() method stores expected data in the `ops` list."""
        pool = CreatePoolConfig("test-pool")
        request = CephRequest(MagicMock(), MagicMock())

        request.add_replicated_pool(pool)

        self.assertEqual(len(request.ops), 1)
        self.assertEqual(request.ops[0], pool.to_json())
