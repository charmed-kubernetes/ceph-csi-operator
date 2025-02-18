# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing
"""Functional tests for ceph-csi charm."""

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

TEST_PATH = Path(__file__).parent
TEMPLATE_DIR = TEST_PATH / "templates"
TEST_OVERLAY = TEST_PATH / "overlay.yaml"

STORAGE_TEMPLATE = "persistent_volume.yaml.j2"
READING_POD_TEMPLATE = "reading_pod.yaml.j2"
WRITING_POD_TEMPLATE = "writing_pod.yaml.j2"

SUCCESS_POD_STATE = "Succeeded"
CEPH_CSI_ALT = "ceph-csi-alt"
CEPHFS_LS = dict(label_selector="juju.io/manifest=cephfs")
RBD_LS = dict(label_selector="juju.io/manifest=rbd")


def ready_apps(ops_test: OpsTest):
    """Return a list of apps in the model excluding ceph-csi-alt."""
    return [a for a in ops_test.model.applications if a != CEPH_CSI_ALT]


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest, namespace: str):
    """Build ceph-csi charm and deploy testing model."""
    charm = next(Path(".").glob("ceph-csi*.charm"), None)
    if not charm:
        logger.info("Building ceph-csi charm.")
        charm = await ops_test.build_charm(".")

    bundle_vars = {"charm": charm.resolve(), "namespace": namespace}
    proxy_settings = environ.get("TEST_HTTPS_PROXY")
    if proxy_settings:
        bundle_vars["https_proxy"] = proxy_settings

    overlays = [ops_test.Bundle("kubernetes-core", channel="stable"), TEST_OVERLAY]

    bundle, *overlays = await ops_test.async_render_bundles(*overlays, **bundle_vars)

    logger.debug("Deploying ceph-csi functional test bundle.")
    model = ops_test.model_full_name
    cmd = f"juju deploy -m {model} {bundle} " + " ".join(f"--overlay={f}" for f in overlays)
    rc, stdout, stderr = await ops_test.run(*shlex.split(cmd))
    assert rc == 0, f"Bundle deploy failed: {(stderr or stdout).strip()}"
    logger.info(stdout)

    def ceph_csi_needs_namespace():
        ceph_csi = ops_test.model.applications.get("ceph-csi")
        expected = f"Missing namespace '{namespace}'"
        if not ceph_csi:
            logger.info("Waiting for ceph-csi app")
        elif not ceph_csi.units:
            logger.info("Waiting for ceph-csi units")
        for unit in ceph_csi.units:
            workload_status = unit.workload_status_message
            if expected in workload_status:
                return True
            logger.info(f"Waiting for ceph-csi units status: {workload_status}")
        return False

    await ops_test.model.block_until(ceph_csi_needs_namespace, timeout=60 * 60, wait_period=5)


async def test_active_status(kube_config: Path, namespace: str, ops_test: OpsTest):
    """Test that ceph-csi charm reached the active state after creating namespace."""
    config.load_kube_config(str(kube_config))
    v1_core = client.CoreV1Api()
    namespaces = [o.metadata.name for o in v1_core.list_namespace().items]
    if namespace not in namespaces:
        v1_namespace = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
        v1_core.create_namespace(v1_namespace)

    async with ops_test.fast_forward("60s"):
        await ops_test.model.wait_for_idle(
            apps=ready_apps(ops_test), wait_for_active=True, timeout=30 * 60
        )
    for unit in ops_test.model.applications["ceph-csi"].units:
        assert unit.workload_status == "active"
        assert unit.workload_status_message == "Ready"


async def test_deployment_replicas(kube_config: Path, namespace: str, ops_test):
    """Test that ceph-csi deployments run the correctly sized replicas."""
    config.load_kube_config(str(kube_config))
    apps_api = client.AppsV1Api()
    (rbdplugin,) = apps_api.list_namespaced_deployment(namespace, **RBD_LS).items
    k8s_workers = ops_test.model.applications["kubernetes-worker"]
    assert rbdplugin.status.replicas == 1  # from the test overlay.yaml
    # Due to anti-affinity rules on the control-plane, the ready replicas
    # are limited to the number of worker nodes, of which there are 1
    assert rbdplugin.status.ready_replicas == len(k8s_workers.units)


@pytest.mark.parametrize("storage_class", ["ceph-xfs", "ceph-ext4"])
@pytest.mark.usefixtures("cleanup_k8s", "ops_test")
async def test_storage_class(kube_config: Path, storage_class: str):
    """Test that ceph can be used to create persistent volume.

    Isolated tests for xfs and ext4, cephfs comes later.
    """
    await run_test_storage_class(kube_config, storage_class)


async def run_test_storage_class(kube_config: Path, storage_class: str):
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
    namespace = reading_pod["metadata"]["namespace"]
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


async def test_update_default_storage_class(kube_config: Path, ops_test: OpsTest):
    """Test that updating "default-storage" configuration takes effect in k8s resources."""

    async def assert_is_default_class(expected_default: str, api: client.StorageV1Api):
        for class_ in api.list_storage_class().items:
            sc = class_.metadata.name
            annotations = class_.metadata.annotations
            is_default = annotations and annotations[default_property]
            if sc == expected_default:
                assert is_default == "true", f"Expected to find {sc} as the default"
            else:
                assert is_default in ("false", None), f"Expected to find {sc} not the default"

    default_property = "storageclass.kubernetes.io/is-default-class"
    expected_classes = ["ceph-xfs", "ceph-ext4"]
    ceph_csi_app = ops_test.model.applications["ceph-csi"]

    config.load_kube_config(str(kube_config))
    storage_api = client.StorageV1Api()

    # Scan available StorageClasses and make sure that all expected classes are present.
    classes_to_test = []
    original_default = None
    logger.debug("Discovering available StorageClasses")
    for storage_class in storage_api.list_storage_class().items:
        name = storage_class.metadata.name
        annotations = storage_class.metadata.annotations
        if annotations:
            is_default = annotations.get(default_property) == "true"
        else:
            is_default = False
        classes_to_test.append(name)
        logger.debug("StorageClass: %s; isDefault: %s", name, is_default)

        if name not in expected_classes:
            pytest.fail("Unexpected storage class in the cluster: {}".format(name))

        if is_default:
            original_default = name

    # move currently active default class to last place so we end up with the same
    # cluster setting after the test
    classes_to_test.remove(original_default)
    classes_to_test.append(original_default)

    # Change 'default-storage' config in charm and make sure it has effect on k8s cluster.
    for storage_class in classes_to_test:
        logger.info("Setting %s StorageClass to be default.", storage_class)
        await ceph_csi_app.set_config({"default-storage": storage_class})
        await ops_test.model.wait_for_idle(apps=["ceph-csi"], timeout=5 * 60)
        await assert_is_default_class(storage_class, storage_api)


async def test_host_networking(kube_config: Path, namespace: str, ops_test):
    """Test that ceph-csi deployments can be run with host networking."""
    config.load_kube_config(str(kube_config))
    apps_api = client.AppsV1Api()
    test_app = ops_test.model.applications["ceph-csi"]

    await test_app.set_config({"enable-host-networking": "true"})
    await ops_test.model.wait_for_idle(apps=ready_apps(ops_test), status="active", timeout=5 * 60)
    (rbdplugin,) = apps_api.list_namespaced_deployment(namespace, **RBD_LS).items
    assert rbdplugin.spec.template.spec.host_network is True

    await test_app.set_config({"enable-host-networking": "false"})
    await ops_test.model.wait_for_idle(apps=ready_apps(ops_test), status="active", timeout=5 * 60)
    (rbdplugin,) = apps_api.list_namespaced_deployment(namespace, **RBD_LS).items
    assert rbdplugin.spec.template.spec.host_network in (None, False)


@pytest_asyncio.fixture()
async def cephfs_enabled(ops_test):
    """Fixture which enables/disable cephfs for a single test."""
    test_app = ops_test.model.applications["ceph-csi"]
    await test_app.set_config({"cephfs-enable": "true"})
    await ops_test.model.wait_for_idle(apps=ready_apps(ops_test), status="active", timeout=5 * 60)
    yield
    await test_app.set_config({"cephfs-enable": "false"})
    await ops_test.model.wait_for_idle(apps=ready_apps(ops_test), status="active", timeout=5 * 60)


@pytest.mark.usefixtures("cleanup_k8s", "cephfs_enabled")
async def test_cephfs(kube_config: Path, namespace: str, ops_test):
    """Test that ceph-csi deployments include cephfs."""
    config.load_kube_config(str(kube_config))
    apps_api = client.AppsV1Api()
    k8s_workers = ops_test.model.applications["kubernetes-worker"]

    (cephfsplugin,) = apps_api.list_namespaced_deployment(namespace, **CEPHFS_LS).items
    assert cephfsplugin.status.ready_replicas == len(k8s_workers.units)

    await run_test_storage_class(kube_config, "cephfs")


async def test_duplicate_ceph_csi(ops_test: OpsTest):
    """Test that deploying ceph-csi twice in the same model fails the second."""
    app = ops_test.model.applications[CEPH_CSI_ALT]
    expected_msg = "resource collisions (action: list-resources)"
    try:
        await app.relate("kubernetes", "kubernetes-control-plane")
        await ops_test.model.wait_for_idle(apps=[CEPH_CSI_ALT], status="blocked", timeout=5 * 60)
        assert expected_msg in app.units[0].workload_status_message
    finally:
        await app.destroy_relation("kubernetes", "kubernetes-control-plane:juju-info")
