# Copyright 2025 Canonical
# See LICENSE file for licensing details.

# https://docs.ceph.com/en/reef/cephfs/fs-volumes/#fs-subvolume-groups
CEPHFS_SUBVOLUMEGROUP = "csi"

# Name of the relation from charmcraft.yaml
CEPH_CLIENT_RELATION = "ceph-client"

# List of required Ceph packages used by the charm
CEPH_PACKAGES = ["ceph-common"]

# List of required Ceph pools used by the charm
REQUIRED_CEPH_POOLS = ["xfs-pool", "ext4-pool"]

# Default Names where the CephFS CSI driver operates, override with config
DEFAULT_NAMESPACE = "default"
