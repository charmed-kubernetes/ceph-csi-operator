# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
#
# Learn more at: https://juju.is/docs/sdk

"""Ceph client library enables to send requests to ceph broker via CephRequest class. Example of
using CephRequest to create ceph pools from within CharmBase instance:

Example:
    request = CephRequest(self.unit, ceph_client_relation)
    pool = CreatePoolConfig("xfs-pool", replicas=3)
    request.add_replicated_pool(pool)
    request.execute()
"""
import json
from typing import Any, Dict, List, Optional
from uuid import uuid4

from ops.model import Relation, Unit

# The unique Charmhub library identifier, never change it
LIBID = "b0d6eb8649ab49acb0af134a237bd612"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1


class RequestError(Exception):
    """Exception that occur when sending requests to Ceph broker."""


class CommonPoolConfig:  # pylint: disable=R0902,R0903
    """Class that encapsulate config options that are common to requests related to ceph pools.

    More specific configs, like CreatePoolConfig can inherit from this class so they can avoid
    enumerating long list of common attributes.
    """

    OP: str = ""  # Operation name must be overriden in child classes.

    def __init__(self, **kwargs: Any) -> None:
        """Initialize Common config pool.

        This method takes following attributes as parameters:

        :param app_name: Tag pool with application name. Note that there is certain protocols
                         emerging upstream with regard to meaningful application names to use.
                         Examples are 'rbd' and 'rgw'.
        :type app_name: Optional[str]
        :param compression_algorithm: Compressor to use, one of: ('lz4', 'snappy', 'zlib', 'zstd')
        :type compression_algorithm: Optional[str]
        :param compression_mode: When to compress data, one of: ('none', 'passive', 'aggressive',
                                 'force')
        :type compression_mode: Optional[str]
        :param compression_required_ratio: Minimum compression ratio for data chunk, if the
                                           requested ratio is not achieved the compressed version
                                           will be thrown away and the original stored.
        :type compression_required_ratio: Optional[float]
        :param compression_min_blob_size: Chunks smaller than this are never compressed
                                          (unit: bytes).
        :type compression_min_blob_size: Optional[int]
        :param compression_min_blob_size_hdd: Chunks smaller than this are not compressed when
                                              destined to rotational media (unit: bytes).
        :type compression_min_blob_size_hdd: Optional[int]
        :param compression_min_blob_size_ssd: Chunks smaller than this are not compressed when
                                              destined to flash media (unit: bytes).
        :type compression_min_blob_size_ssd: Optional[int]
        :param compression_max_blob_size: Chunks larger than this are broken into
                                          N * compression_max_blob_size chunks before being
                                          compressed (unit: bytes).
        :type compression_max_blob_size: Optional[int]
        :param compression_max_blob_size_hdd: Chunks larger than this are broken into
                                              N * compression_max_blob_size_hdd chunks before
                                              being compressed when destined for rotational media
                                              (unit: bytes)
        :type compression_max_blob_size_hdd: Optional[int]
        :param compression_max_blob_size_ssd: Chunks larger than this are broken into
                                              N * compression_max_blob_size_ssd chunks before
                                              being compressed when destined for flash media
                                              (unit: bytes).
        :type compression_max_blob_size_ssd: Optional[int]
        :param group: Group to add pool to
        :type group: Optional[str]
        :param max_bytes: Maximum bytes quota to apply
        :type max_bytes: Optional[int]
        :param max_objects: Maximum objects quota to apply
        :type max_objects: Optional[int]
        :param group_namespace: Group namespace
        :type group_namespace: Optional[str]
        :param rbd_mirroring_mode: Pool mirroring mode used when Ceph RBD mirroring is enabled.
        :type rbd_mirroring_mode: Optional[str]
        :param weight: The percentage of data that is expected to be contained in the pool from
                       the total available space on the OSDs. Used to calculate number of
                       Placement Groups to create for pool.
        :type weight: Optional[float]

        """
        self.app_name: Optional[str] = None
        self.group: Optional[str] = None
        self.max_bytes: Optional[int] = None
        self.max_objects: Optional[int] = None
        self.group_namespace: Optional[str] = None
        self.rbd_mirroring_mode: str = "pool"
        self.weight: Optional[float] = None
        self.compression_algorithm: Optional[str] = None
        self.compression_mode: Optional[str] = None
        self.compression_required_ratio: Optional[float] = None
        self.compression_min_blob_size: Optional[int] = None
        self.compression_min_blob_size_hdd: Optional[int] = None
        self.compression_min_blob_size_ssd: Optional[int] = None
        self.compression_max_blob_size: Optional[int] = None
        self.compression_max_blob_size_hdd: Optional[int] = None
        self.compression_max_blob_size_ssd: Optional[int] = None

        for key, value in kwargs.items():
            _ = self.__getattribute__(key)  # Ensure that key is valid attribute
            self.__setattr__(key, value)

    def to_json(self) -> Dict[str, str]:
        """Serialize config data into json (dict)."""
        output = {"op": self.OP}
        for attribute, value in vars(self).items():
            json_key = attribute.replace("_", "-")
            output[json_key] = value

        return output


class CreatePoolConfig(CommonPoolConfig):  # pylint: disable=R0903
    """This class encapsulates config data required by ceph broker to create Ceph pool."""

    OP = "create-pool"

    def __init__(
        self, name: str, replicas: int = 3, pg_num: Optional[int] = None, **kwargs: Any
    ) -> None:
        """Initialize config to create ceph pool.

        :param name: Name of the pool.
            :param replicas: How many times should be objects in this pool replicated.
        :param pg_num: Request specific number of Placement Groups to create for the pool.
        :param kwargs: Additional config options. See attributes of CommonPoolConfig for more info
                       about available options.
        """
        self.name = name
        self.replicas = replicas
        self.pg_num = pg_num
        super().__init__(**kwargs)

    def to_json(self) -> Dict[str, str]:
        output = super().to_json()
        # parameter pg_num needs to be renamed since, unlike every other, it's expected with
        # underscore, not dash
        output["pg_num"] = output.pop("pg-num")
        return output


class CephRequest:
    """This class represents single request for ceph-broker.

    Request is executed by setting appropriate data in the relation with ceph-mon application.

    Note: More than one operation (ops) can be requested at once. For example, one request can
    create multiple ceph pools.
    """

    def __init__(self, unit: Unit, client_relation: Relation, id_: str = "", api_version: int = 1):
        """
        Initialize CephRequest.

        :param unit: Juju unit from which the request originates.
        :param client_relation: Instance of relation with ceph-mon application.
        :param id_: (Optional) Request ID. If not supplied, new random uuid will be generated.
        :param api_version: Version of ceph-broker API. default=1.
        """
        self.unit = unit
        self.relation = client_relation
        self.api_version = api_version
        self.uuid = id_ or str(uuid4())
        self._ops: List[Dict] = []

    @property
    def ops(self) -> List[Dict]:
        """List of operations contained in this request."""
        return self._ops

    @property
    def request(self) -> Dict:
        """Return whole request serialized as a dict."""
        return {"api-version": self.api_version, "request-id": self.uuid, "ops": self.ops}

    def add_op(self, op_data: Dict) -> None:
        """Add new operation to the request.

        Operations are de-duplicated and no action is done if same operation is already part this
        request.

        :param op_data: Operation that will be requested from the ceph broker, serialized as dict.
        """
        if op_data not in self.ops:
            self.ops.append(op_data)

    def execute(self) -> None:
        """Send request to ceph-broker.

        Communication with ceph-broker is done via juju relation with ceph-mon application. Request
        is sent by storing it in the relation data.
        """
        if not self.ops:
            raise RequestError(
                "Can not execute request without specifying operations ({})".format(self.uuid)
            )
        self.relation.data[self.unit]["broker_req"] = json.dumps(self.request)

    def add_replicated_pool(self, config: CreatePoolConfig) -> None:
        """Add operation that creates new replicated ceph pool to this request.

        :param config: Specification of the ceph pool parameters.
        :return: None
        """
        self.add_op(config.to_json())
