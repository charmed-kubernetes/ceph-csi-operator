# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Cross-model integration tests for ceph-csi charm with microceph as the provider."""

import json
import logging
import shlex
from os import environ
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
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

MICROCEPH_APP = "microceph"
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
async def microceph_model(ops_test: OpsTest, microceph_source: dict):
    """Deploy microceph in a second (machine) model and return the model name."""
    model_alias = "microceph-model"
    microceph_model = await ops_test.track_model(model_alias)
    model_name = microceph_model.info.name

    if "charm" in microceph_source:
        logger.info("Deploying microceph from local charm in model %s", model_name)
        await microceph_model.deploy(str(microceph_source["charm"]), MICROCEPH_APP)
    else:
        logger.info(
            "Deploying microceph from charmhub (%s) in model %s",
            microceph_source["channel"],
            model_name,
        )
        # Use CLI to deploy because juju client doesn't handle channels like "latest/edge/csi"
        await ops_test.juju(
            "deploy", MICROCEPH_APP, "-m", model_name, "--channel", microceph_source["channel"]
        )
    await microceph_model.wait_for_idle(apps=[MICROCEPH_APP], status="active", timeout=20 * 60)

    # Add loop OSDs
    microceph_units = microceph_model.applications[MICROCEPH_APP].units
    for unit in microceph_units:
        action = await unit.run_action("add-osd", **{"loop-spec": LOOP_OSD_SPEC})
        action = await action.wait()
        assert action.status == "completed", f"add-osd failed on {unit.name}: {action.results}"

    await microceph_model.wait_for_idle(apps=[MICROCEPH_APP], status="active", timeout=20 * 60)

    yield model_name

    if not ops_test.keep_model:
        await ops_test.forget_model(model_alias)


@pytest_asyncio.fixture(scope="module")
async def ceph_csi_offer(ops_test: OpsTest, microceph_model: str):
    """Create a juju offer for the microceph ceph-csi endpoint."""
    offer_url = f"admin/{microceph_model}.{MICROCEPH_APP}"
    logger.info("Creating offer for %s:ceph-csi", MICROCEPH_APP)

    rc, stdout, stderr = await ops_test.run(
        *shlex.split(f"juju offer -m {microceph_model} {MICROCEPH_APP}:ceph-csi")
    )
    assert rc == 0, f"juju offer failed: {(stderr or stdout).strip()}"

    yield offer_url


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.abort_on_fail
async def test_deploy_microceph(microceph_model: str):
    """Deploy microceph in a separate machine model and add storage."""
    assert microceph_model, "microceph model should be available"
    logger.info("Microceph deployed in model: %s", microceph_model)


@pytest.mark.abort_on_fail
async def test_build_and_deploy(
    ops_test: OpsTest, namespace: str, ceph_csi_channel: str | None, ceph_csi_offer: str
):
    """Build ceph-csi, deploy k8s + ceph-csi overlay, consume offer, and integrate."""
    if ceph_csi_channel:
        logger.info(f"Using ceph-csi channel: {ceph_csi_channel}")
        charm = "ceph-csi"
        channel = ceph_csi_channel
    else:
        channel = None
        charm = next(Path(".").glob("ceph-csi*.charm"), None)
        if not charm:
            logger.info("Building ceph-csi charm.")
            charm = await ops_test.build_charm(".")
        charm = charm.resolve() if charm else None

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
    model = ops_test.model_full_name
    cmd = f"juju deploy -m {model} {bundle} " + " ".join(f"--overlay={f}" for f in overlays)
    rc, stdout, stderr = await ops_test.run(*shlex.split(cmd))
    assert rc == 0, f"Bundle deploy failed: {(stderr or stdout).strip()}"
    logger.info(stdout)

    # Consume the microceph offer
    logger.info("Consuming offer %s", ceph_csi_offer)
    rc, stdout, stderr = await ops_test.run(
        *shlex.split(f"juju consume -m {model} {ceph_csi_offer} {MICROCEPH_APP}")
    )
    assert rc == 0, f"juju consume failed: {(stderr or stdout).strip()}"

    # Integrate ceph-csi with the consumed offer
    logger.info("Integrating ceph-csi:ceph with microceph offer")
    await ops_test.model.integrate(f"{CEPH_CSI_APP}:ceph", MICROCEPH_APP)

    # Wait for ceph-csi to report namespace issue first
    def ceph_csi_needs_namespace():
        ceph_csi = ops_test.model.applications.get(CEPH_CSI_APP)
        expected = f"Missing namespace '{namespace}'"
        if not ceph_csi or not ceph_csi.units:
            return False
        return any(expected in u.workload_status_message for u in ceph_csi.units)

    await ops_test.model.block_until(ceph_csi_needs_namespace, timeout=60 * 60, wait_period=5)


@pytest.mark.abort_on_fail
async def test_active_status(kube_config: Path, namespace: str, ops_test: OpsTest):
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


async def test_ceph_resources(ops_test: OpsTest, microceph_model: str):
    """Verify pools, filesystem, and auth on the microceph side."""
    microceph = await ops_test.get_model(microceph_model)
    microceph_units = microceph.applications[MICROCEPH_APP].units
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
