# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

output "app_name" {
  description = "Name of the deployed application."
  value       = juju_application.ceph_csi.name
}

output "requires" {
  value = {
    ceph_client     = "ceph-client"
    kubernetes      = "kubernetes"
    kubernetes_info = "kubernetes-info"
  }
}
