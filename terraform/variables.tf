# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

variable "app_name" {
  description = "Name of the application in the Juju model."
  type        = string
  default     = "ceph-csi"
}

variable "base" {
  description = "Ubuntu bases to deploy the charm onto"
  type        = string
  default     = "ubuntu@22.04"
  nullable = false

  validation {
    condition     = var.base == null || contains(["ubuntu@20.04", "ubuntu@22.04"], var.base)
    error_message = "Base must be one of ubuntu@20.04, ubuntu@22.04"
  }
}

variable "channel" {
  description = "The channel to use when deploying a charm."
  type        = string
  default     = "1.31/stable"
}

variable "config" {
  description = "Application config. Details about available options can be found at https://charmhub.io/ceph-csi/configurations."
  type        = map(string)
  default     = {}
}

variable "constraints" {
  description = "Juju constraints to apply for this application."
  type        = string
  default     = "arch=amd64"
}

variable "model" {
  description = "Reference to a `juju_model`."
  type        = string
}

variable "revision" {
  description = "Revision number of the charm"
  type        = number
  default     = null
}
