# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Cross-model integration tests for ceph-csi charm with microceph as the provider."""

import json
import logging
from os import environ
from pathlib import Path
from typing import AsyncGenerator
from uuid import uuid4

import juju.constraints
import pytest
import pytest_asyncio
from juju.model import Model
from kubernetes import client, config, utils
from pytest_operator.plugin import OpsTest

from utils import render_j2_template, wait_for_pod

logger = logging.getLogger(__name__)

LATEST_RELEASE = ""
TEST_PATH = Path(__file__).parent
TEMPLATE_DIR = TEST_PATH / "templates"
TEST_OVERLAY = TEST_PATH / "microceph.yaml"

STORAGE_TEMPLATE = "persistent_volume.yaml.j2"
READING_POD_TEMPLATE = "reading_pod.yaml.j2"
WRITING_POD_TEMPLATE = "writing_pod.yaml.j2"

RUNNING_POD_STATE = "Running"
SUCCESS_POD_STATE = "Succeeded"

MICROCEPH_MODEL_ALIAS = "microceph-model"
MICROCEPH_APP = "microceph"
MICROCEPH_CONSTRAINTS = "cores=2 mem=4G root-disk=4G"
CEPH_CSI_APP = "ceph-csi"
K8S_APP = "k8s"
LOOP_OSD_SPEC = "1G,3"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def run_test_storage_class(kube_config: Path, storage_class: str):
    """Test that ceph can be used to create persistent volume.

    Creates a PVC, writes data via a pod, reads it back via another pod.
    """
    test_payload = "integ-test-write-{}-{}".format(storage_class, str(uuid4()))

    config.load_kube_config(str(kube_config))
    k8s_api_client = client.ApiClient()
    core_api = client.CoreV1Api()

    storage = render_j2_template(TEMPLATE_DIR, STORAGE_TEMPLATE, storage_class=storage_class)
    reading_pod = render_j2_template(
        TEMPLATE_DIR, READING_POD_TEMPLATE, storage_class=storage_class
    )
    namespace = reading_pod["metadata"]["namespace"]
    reading_pod_name = reading_pod["metadata"]["name"]
    writing_pod = render_j2_template(
        TEMPLATE_DIR,
        WRITING_POD_TEMPLATE,
        storage_class=storage_class,
        data=test_payload,
    )
    writing_pod_name = writing_pod["metadata"]["name"]

    try:
        logger.info("Creating PersistentVolumeClaim %s", storage["metadata"]["name"])
        utils.create_from_dict(k8s_api_client, storage)

        logger.info("Creating Writer Pod %s", writing_pod_name)
        utils.create_from_dict(k8s_api_client, writing_pod)
        wait_for_pod(core_api, writing_pod_name, namespace, target_state=SUCCESS_POD_STATE)

        logger.info("Creating Reader Pod %s", reading_pod_name)
        utils.create_from_dict(k8s_api_client, reading_pod)
        wait_for_pod(core_api, reading_pod_name, namespace, target_state=SUCCESS_POD_STATE)

        pod_log = core_api.read_namespaced_pod_log(reading_pod_name, namespace)
        assert test_payload in pod_log, "Pod {} failed to read data written by pod {}".format(
            reading_pod_name, writing_pod_name
        )
    finally:
        core_api.delete_namespaced_pod(reading_pod_name, namespace)
        core_api.delete_namespaced_pod(writing_pod_name, namespace)
        core_api.delete_namespaced_persistent_volume_claim(storage["metadata"]["name"], namespace)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="module")
async def microceph_model(
    ops_test: OpsTest, microceph_source: dict
) -> AsyncGenerator[Model, None]:
    """Deploy microceph in a second (machine) model and return the model name."""
    await ops_test.track_model(MICROCEPH_MODEL_ALIAS)

    with ops_test.model_context(MICROCEPH_MODEL_ALIAS) as model:
        if charm := microceph_source.get("charm"):
            logger.info("Deploying microceph from local charm in model %s", model.name)
            await model.deploy(
                str(charm),
                MICROCEPH_APP,
                constraints=juju.constraints.parse(MICROCEPH_CONSTRAINTS),
            )
        elif channel := microceph_source.get("channel"):
            logger.info(
                "Deploying microceph from charmhub (%s) in model %s",
                channel,
                model.name,
            )
            # Use CLI to deploy because juju client doesn't handle channels like "latest/edge/csi"
            await ops_test.juju(
                "deploy",
                MICROCEPH_APP,
                "--channel",
                channel,
                "--constraints",
                MICROCEPH_CONSTRAINTS,
                check=True,
            )
        else:
            pytest.fail("No microceph source specified")
    await model.wait_for_idle(apps=[MICROCEPH_APP], status="active", timeout=20 * 60)

    # Add loop OSDs
    microceph_units = model.applications[MICROCEPH_APP].units
    for unit in microceph_units:
        action = await unit.run_action("add-osd", **{"loop-spec": LOOP_OSD_SPEC})
        action = await action.wait()
        assert action.status == "completed", f"add-osd failed on {unit.name}: {action.results}"

    await model.wait_for_idle(apps=[MICROCEPH_APP], status="active", timeout=20 * 60)

    yield model

    if not ops_test.keep_model:
        await ops_test.forget_model(MICROCEPH_MODEL_ALIAS)


@pytest_asyncio.fixture(scope="module")
async def ceph_csi_offer(microceph_model: Model):
    """Create a juju offer for the microceph ceph-csi endpoint."""
    endpoint = f"{MICROCEPH_APP}:ceph-csi"
    offer_url = f"admin/{microceph_model.name}.{MICROCEPH_APP}"
    logger.info("Creating offer for %s", offer_url)

    try:
        await microceph_model.create_offer(endpoint)
        yield offer_url
    finally:
        await microceph_model.remove_offer(endpoint, force=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.abort_on_fail
async def test_deploy_microceph(microceph_model: Model):
    """Deploy microceph in a separate machine model and add storage."""
    assert microceph_model, "microceph model should be available"
    logger.info("Microceph deployed in model: %s", microceph_model.name)


@pytest.mark.abort_on_fail
async def test_build_and_deploy(
    ops_test: OpsTest,
    namespace: str,
    ceph_csi_channel: str | None,
    ceph_csi_charm: Path | str,
    ceph_csi_offer: str,
):
    """Build ceph-csi, deploy k8s + ceph-csi overlay, consume offer, and integrate."""
    charm = ceph_csi_charm
    channel = ceph_csi_channel

    bundle_vars = {
        "charm": charm,
        "channel": channel,
        "namespace": namespace,
        "release": environ.get("TEST_RELEASE", LATEST_RELEASE),
    }
    overlays = [
        ops_test.Bundle("canonical-kubernetes", channel="latest/edge"),
        TEST_OVERLAY,
    ]

    bundle, *overlays = await ops_test.async_render_bundles(*overlays, **bundle_vars)

    logger.info("Deploying ceph-csi integration test bundle.")
    with ops_test.model_context("main") as model:
        await ops_test.juju(
            "deploy", str(bundle), *(f"--overlay={f}" for f in overlays), check=True
        )

        # Integrate ceph-csi with the consumed offer
        logger.info("Integrating ceph-csi:ceph with microceph offer")
        await model.integrate(f"{CEPH_CSI_APP}:ceph", ceph_csi_offer)

        # Wait for ceph-csi to report namespace issue first
        def ceph_csi_needs_namespace():
            ceph_csi = model.applications.get(CEPH_CSI_APP)
            expected = f"Missing namespace '{namespace}'"
            if not ceph_csi or not ceph_csi.units:
                return False
            return any(expected in u.workload_status_message for u in ceph_csi.units)

        await model.block_until(ceph_csi_needs_namespace, timeout=60 * 60, wait_period=5)


@pytest.mark.abort_on_fail
async def test_active_status(kube_config: Path, ops_test: OpsTest):
    """Test that all apps reach active state after namespace creation."""
    config.load_kube_config(str(kube_config))
    ceph_csi_app = ops_test.model.applications[CEPH_CSI_APP]
    await ceph_csi_app.set_config({"create-namespace": "true"})

    async with ops_test.fast_forward("60s"):
        await ops_test.model.wait_for_idle(
            apps=[K8S_APP, CEPH_CSI_APP], wait_for_active=True, timeout=30 * 60
        )
    for unit in ceph_csi_app.units:
        assert unit.workload_status == "active"
        assert unit.workload_status_message == "Ready"


@pytest.mark.usefixtures("cleanup_k8s", "ops_test")
async def test_rbd_storage_class(kube_config: Path):
    """Test RBD storage class with PVC + write/read pod."""
    await run_test_storage_class(kube_config, "ceph-xfs")


@pytest.mark.usefixtures("cleanup_k8s")
async def test_cephfs_storage_class(kube_config: Path, ops_test: OpsTest):
    """Enable cephfs, verify cephfs storage class, then test PVC + pod."""
    ceph_csi_app = ops_test.model.applications[CEPH_CSI_APP]
    await ceph_csi_app.set_config({"cephfs-enable": "true"})
    await ops_test.model.wait_for_idle(
        apps=[K8S_APP, CEPH_CSI_APP], status="active", timeout=10 * 60
    )

    await run_test_storage_class(kube_config, "cephfs")

    # Restore cephfs to disabled
    await ceph_csi_app.set_config({"cephfs-enable": "false"})
    await ops_test.model.wait_for_idle(
        apps=[K8S_APP, CEPH_CSI_APP], status="active", timeout=5 * 60
    )


async def test_ceph_resources(microceph_model: Model):
    """Verify pools, filesystem, and auth on the microceph side."""
    microceph_units = microceph_model.applications[MICROCEPH_APP].units
    leader = microceph_units[0]

    # Verify RBD pool (cross-model uses rbd.remote-<uuid> naming)
    action = await leader.run("sudo microceph.ceph osd pool ls --format json")
    await action.wait()
    pools = json.loads(action.results.get("stdout", "[]"))
    logger.info("Ceph pools: %s", pools)
    rbd_pools = [p for p in pools if p.startswith("rbd.")]
    assert len(rbd_pools) > 0, f"Expected at least one rbd.* pool, got: {pools}"

    # Verify cephx auth (cross-model uses client.csi-remote-<uuid> naming)
    action = await leader.run("sudo microceph.ceph auth ls --format json")
    await action.wait()
    auth_data = json.loads(action.results.get("stdout", "[]"))
    # ceph auth ls --format json may return {"auth_dump": [...]} or a plain list
    if isinstance(auth_data, dict):
        auth_entries = auth_data.get("auth_dump", [])
    else:
        auth_entries = auth_data
    csi_clients = [
        e
        for e in auth_entries
        if isinstance(e, dict) and e.get("entity", "").startswith("client.csi-")
    ]
    assert len(csi_clients) > 0, (
        f"No cephx auth entry starting with 'client.csi-' found " f"among: {auth_entries}"
    )
    logger.info("Found cephx entries: %s", [e.get("entity") for e in csi_clients])
