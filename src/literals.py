# Copyright 2025 Canonical
# See LICENSE file for licensing details.

# https://docs.ceph.com/en/reef/cephfs/fs-volumes/#fs-subvolume-groups
CEPHFS_SUBVOLUMEGROUP = "csi"

# Name of the relation from charmcraft.yaml
CEPH_CSI_RELATION = "ceph-csi"
CEPH_CLIENT_RELATION = "ceph-client"

# List of required Ceph packages used by the charm
CEPH_PACKAGES = ["ceph-common"]

# List of required Ceph pools used by the charm
REQUIRED_CEPH_POOLS = ["xfs-pool", "ext4-pool"]

# Default Names where the CephFS CSI driver operates, override with config
DEFAULT_NAMESPACE = "default"

# Default StorageClass name and annotation
CONFIG_DEFAULT_STORAGE = "default-storage"
DEFAULT_SC_ANNOTATION_NAME = "storageclass.kubernetes.io/is-default-class"
DEFAULT_SC_ANNOTATION = {DEFAULT_SC_ANNOTATION_NAME: "true"}

CONFIG_CEPH_RBD_ENABLE = "ceph-rbd-enable"
CONFIG_CEPHFS_ENABLE = "cephfs-enable"
