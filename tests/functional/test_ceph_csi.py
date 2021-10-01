# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing
"""Functional tests for ceph-csi charm."""

import logging
from os import environ
from pathlib import Path
from uuid import uuid4

import pytest
from kubernetes import client, config, utils
from pytest_operator.plugin import OpsTest
from utils import render_j2_template, wait_for_pod

logger = logging.getLogger(__name__)

TEMPLATE_DIR = "./tests/functional/templates/"

STORAGE_TEMPLATE = "persistent_volume.yaml.j2"
READING_POD_TEMPLATE = "reading_pod.yaml.j2"
WRITING_POD_TEMPLATE = "writing_pod.yaml.j2"

SUCCESS_POD_STATE = "Succeeded"


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test):
    """Build ceph-csi charm and deploy testing model."""
    logger.info("Building ceph-csi charm.")
    ceph_csi_charm = await ops_test.build_charm(".")

    bundle_vars = {"master_charm": ceph_csi_charm}
    proxy_settings = environ.get("TEST_HTTPS_PROXY")
    if proxy_settings:
        bundle_vars["https_proxy"] = proxy_settings

    logger.debug("Deploying ceph-csi functional test bundle.")
    await ops_test.model.deploy(
        ops_test.render_bundle("tests/functional/bundle.yaml", **bundle_vars)
    )
    await ops_test.model.wait_for_idle(
        wait_for_active=True, timeout=60 * 60, check_freq=5, raise_on_error=False
    )


async def test_active_status(ops_test: OpsTest):
    """Test that ceph-csi charm reached the active state."""
    for unit in ops_test.model.applications["ceph-csi"].units:
        assert unit.workload_status == "active"
        assert unit.workload_status_message == "Unit is ready"


@pytest.mark.parametrize("storage_class", ["ceph-xfs", "ceph-ext4"])
async def test_storage_class(
    kube_config: Path, storage_class: str, namespace: str, cleanup_k8s: None, ops_test
):
    """Test that ceph can be used to create persistent volume.

    This test has following flow:
      * Create PersistentVolumeClaim using one of the supported StorageClasses
      * Create "writing_pod" that uses PersistentVolumeClaim to create file and write data to it.
      * Create "reading_pod" that reads expected data from file in the PersistentVolumeClaim.
    """
    test_payload = "func-test-write-{}-{}".format(storage_class, str(uuid4()))

    config.load_kube_config(str(kube_config))
    k8s_api_client = client.ApiClient()
    core_api = client.CoreV1Api()

    storage = render_j2_template(TEMPLATE_DIR, STORAGE_TEMPLATE, storage_class=storage_class)
    reading_pod = render_j2_template(
        TEMPLATE_DIR, READING_POD_TEMPLATE, storage_class=storage_class
    )
    reading_pod_name = reading_pod["metadata"]["name"]
    writing_pod = render_j2_template(
        TEMPLATE_DIR, WRITING_POD_TEMPLATE, storage_class=storage_class, data=test_payload
    )
    writing_pod_name = writing_pod["metadata"]["name"]

    logger.info("Creating PersistentVolumeClaim %s", storage["metadata"]["name"])
    utils.create_from_dict(k8s_api_client, storage)

    logger.info("Creating Pod %s", writing_pod_name)
    utils.create_from_dict(k8s_api_client, writing_pod)
    wait_for_pod(core_api, writing_pod_name, namespace, target_state=SUCCESS_POD_STATE)

    logger.info("Creating Pod %s", reading_pod_name)
    utils.create_from_dict(k8s_api_client, reading_pod)
    wait_for_pod(core_api, reading_pod_name, namespace, target_state=SUCCESS_POD_STATE)

    pod_log = core_api.read_namespaced_pod_log(reading_pod_name, namespace)
    assert test_payload in pod_log, "Pod {} failed to read data written by pod {}".format(
        reading_pod_name, writing_pod_name
    )
