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
import logging
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


logger = logging.getLogger(__name__)


class RequestError(Exception):
    """Exception that occur when sending requests to Ceph broker."""


class CommonPoolConfig:  # pylint: disable=R0902,R0903,R0913,R0914
    """Class that encapsulate config options that are common to requests related to ceph pools.

    More specific configs, like CreatePoolConfig can inherit from this class so they can avoid
    enumerating long list of common attributes.
    """

    OP = ""  # Operation name must be overriden in child classes.

    def __init__(
        self,
        app_name: Optional[str] = None,
        group: Optional[str] = None,
        max_bytes: Optional[int] = None,
        max_objects: Optional[int] = None,
        group_namespace: Optional[str] = None,
        rbd_mirroring_mode: str = "pool",
        weight: Optional[float] = None,
        compression_algorithm: Optional[str] = None,
        compression_mode: Optional[str] = None,
        compression_required_ratio: Optional[float] = None,
        compression_min_blob_size: Optional[int] = None,
        compression_min_blob_size_hdd: Optional[int] = None,
        compression_min_blob_size_ssd: Optional[int] = None,
        compression_max_blob_size: Optional[int] = None,
        compression_max_blob_size_hdd: Optional[int] = None,
        compression_max_blob_size_ssd: Optional[int] = None,
    ) -> None:
        """Initialize Common config pool.

        This method takes following attributes as parameters:

        :param app_name: Tag pool with application name. Note that there is certain protocols
                         emerging upstream with regard to meaningful application names to use.
                         Examples are 'rbd' and 'rgw'.
        :param compression_algorithm: Compressor to use, one of: ('lz4', 'snappy', 'zlib', 'zstd')
        :param compression_mode: When to compress data, one of: ('none', 'passive', 'aggressive',
                                 'force')
        :param compression_required_ratio: Minimum compression ratio for data chunk, if the
                                           requested ratio is not achieved the compressed version
                                           will be thrown away and the original stored.
        :param compression_min_blob_size: Chunks smaller than this are never compressed
                                          (unit: bytes).
        :param compression_min_blob_size_hdd: Chunks smaller than this are not compressed when
                                              destined to rotational media (unit: bytes).
        :param compression_min_blob_size_ssd: Chunks smaller than this are not compressed when
                                              destined to flash media (unit: bytes).
        :param compression_max_blob_size: Chunks larger than this are broken into
                                          N * compression_max_blob_size chunks before being
                                          compressed (unit: bytes).
        :param compression_max_blob_size_hdd: Chunks larger than this are broken into
                                              N * compression_max_blob_size_hdd chunks before
                                              being compressed when destined for rotational media
                                              (unit: bytes)
        :param compression_max_blob_size_ssd: Chunks larger than this are broken into
                                              N * compression_max_blob_size_ssd chunks before
                                              being compressed when destined for flash media
                                              (unit: bytes).
        :param group: Group to add pool to
        :param max_bytes: Maximum bytes quota to apply
        :param max_objects: Maximum objects quota to apply
        :param group_namespace: Group namespace
        :param rbd_mirroring_mode: Pool mirroring mode used when Ceph RBD mirroring is enabled.
        :param weight: The percentage of data that is expected to be contained in the pool from
                       the total available space on the OSDs. Used to calculate number of
                       Placement Groups to create for pool.
        """
        self.app_name = app_name
        self.group = group
        self.max_bytes = max_bytes
        self.max_objects = max_objects
        self.group_namespace = group_namespace
        self.rbd_mirroring_mode = rbd_mirroring_mode
        self.weight = weight
        self.compression_algorithm = compression_algorithm
        self.compression_mode = compression_mode
        self.compression_required_ratio = compression_required_ratio
        self.compression_min_blob_size = compression_min_blob_size
        self.compression_min_blob_size_hdd = compression_min_blob_size_hdd
        self.compression_min_blob_size_ssd = compression_min_blob_size_ssd
        self.compression_max_blob_size = compression_max_blob_size
        self.compression_max_blob_size_hdd = compression_max_blob_size_hdd
        self.compression_max_blob_size_ssd = compression_max_blob_size_ssd

    def to_json(self) -> Dict[str, Any]:
        """Serialize config data into json (dict)."""
        output = {attribute.replace("_", "-"): value for attribute, value in vars(self).items()}
        output["op"] = self.OP

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

    def to_json(self) -> Dict[str, Any]:
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
        self._ops = []

    @property
    def ops(self) -> List[Dict[str, Any]]:
        """List of operations contained in this request."""
        return self._ops

    @property
    def request(self) -> Dict[str, Any]:
        """Return whole request serialized as a dict."""
        return {"api-version": self.api_version, "request-id": self.uuid, "ops": self.ops}

    def add_op(self, op_data: Dict[str, Any]) -> None:
        """Add new operation to the request.

        Operations are de-duplicated and no action is done if same operation is already part this
        request.

        :param op_data: Operation that will be requested from the ceph broker, serialized as dict.
        """
        if op_data not in self.ops:
            logger.debug("Ceph broker request (%s): Adding OP: %s", self.uuid, op_data)
            self.ops.append(op_data)
        else:
            logger.debug(
                "Ceph broker request (%s): Similar OP already exists in the request: %s",
                self.uuid,
                op_data,
            )

    def execute(self) -> None:
        """Send request to ceph-broker.

        Communication with ceph-broker is done via juju relation with ceph-mon application. Request
        is sent by storing it in the relation data.
        """
        if not self.ops:
            raise RequestError(
                "Can not execute request without specifying operations ({})".format(self.uuid)
            )

        serialized_request = json.dumps(self.request)
        logger.info("Sending request %s to Ceph broker.", self.uuid)
        logger.debug("Request %s details: %s", self.uuid, serialized_request)
        self.relation.data[self.unit]["broker_req"] = serialized_request

    def add_replicated_pool(self, config: CreatePoolConfig) -> None:
        """Add operation that creates new replicated ceph pool to this request.

        :param config: Specification of the ceph pool parameters.
        :return: None
        """
        self.add_op(config.to_json())
