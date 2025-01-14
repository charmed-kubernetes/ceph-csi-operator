# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

resource "juju_application" "ceph_csi" {
  name  = var.app_name
  model = var.model

  charm {
    name     = "ceph-csi"
    channel  = var.channel
    revision = var.revision
    base     = var.base
  }

  config      = var.config
  constraints = var.constraints
}
