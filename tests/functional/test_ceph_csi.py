# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing
"""Functional tests for ceph-csi charm."""

import json
import logging
import re
import shlex
import time
from os import environ
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from kubernetes import client, config, utils
from kubernetes.stream import stream
from pytest_operator.plugin import OpsTest

from utils import (
    render_j2_template,
    set_test_config,
    units_have_status,
    wait_for_pod,
    wait_for_pvc_resize,
)

logger = logging.getLogger(__name__)

LATEST_RELEASE = ""  # ops.manifest will load the latest release when unspecified
TEST_PATH = Path(__file__).parent
TEMPLATE_DIR = TEST_PATH / "templates"
TEST_OVERLAY = TEST_PATH / "overlay.yaml"

DEFAULT_ANNOTATION = "storageclass.kubernetes.io/is-default-class"
STORAGE_TEMPLATE = "persistent_volume.yaml.j2"
READING_POD_TEMPLATE = "reading_pod.yaml.j2"
REPORTER_POD_TEMPLATE = "reporter_pod.yaml.j2"
WRITING_POD_TEMPLATE = "writing_pod.yaml.j2"

RUNNING_POD_STATE = "Running"
SUCCESS_POD_STATE = "Succeeded"
CEPH_CSI_ALT = "ceph-csi-alt"
CEPHFS_LS = dict(label_selector="juju.io/manifest=cephfs")
RBD_LS = dict(label_selector="juju.io/manifest=rbd")


def ready_apps(ops_test: OpsTest):
    """Return a list of apps in the model excluding ceph-csi-alt."""
    return [a for a in ops_test.model.applications if a != CEPH_CSI_ALT]


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest, namespace: str, ceph_csi_channel: str | None):
    """Build ceph-csi charm and deploy testing model."""
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
    ceph_csi_app = ops_test.model.applications["ceph-csi"]
    await ceph_csi_app.set_config({"create-namespace": "true"})

    async with ops_test.fast_forward("60s"):
        await ops_test.model.wait_for_idle(
            apps=ready_apps(ops_test), status="active", timeout=30 * 60
        )
    for unit in ceph_csi_app.units:
        assert unit.workload_status == "active"
        assert unit.workload_status_message == "Ready"


async def test_deployment_replicas(kube_config: Path, namespace: str, ops_test):
    """Test that ceph-csi deployments run the correctly sized replicas."""
    config.load_kube_config(str(kube_config))
    apps_api = client.AppsV1Api()
    (rbdplugin,) = apps_api.list_namespaced_deployment(namespace, **RBD_LS).items
    k8s_workers = ops_test.model.applications["k8s-worker"]
    assert rbdplugin.status.replicas == 1  # from the test overlay.yaml
    # Due to anti-affinity rules on the control-plane, the ready replicas
    # are limited to the number of worker nodes, of which there are 1
    assert rbdplugin.status.ready_replicas == len(k8s_workers.units)


async def test_rbac_name_formatter(kube_config: Path, ops_test):
    """Test that ceph-csi deployments use the correct rbac name formatter."""
    config.load_kube_config(str(kube_config))
    rbac_api = client.RbacAuthorizationV1Api()
    cluster_roles = rbac_api.list_cluster_role(**RBD_LS).items
    # Check that the ceph-csi cluster roles are using the correct name formatter
    assert len(cluster_roles) == 2
    for role in cluster_roles:
        assert role.metadata.name.endswith("-formatter")


@pytest.mark.parametrize("storage_class", ["ceph-xfs", "ceph-ext4"])
@pytest.mark.usefixtures("cleanup_k8s", "ops_test")
async def test_storage_resize(kube_config: Path, storage_class: str):
    """Test that ceph can be used to create persistent volume.

    Isolated tests for xfs and ext4, cephfs comes later.
    """
    await run_resize_storage_class(kube_config, storage_class)


async def run_resize_storage_class(kube_config: Path, storage_class: str):
    """Test that ceph can be used to create and resize persistent volume.

    This test has following flow:
      * Create PersistentVolumeClaim using one of the supported StorageClasses
      * Create "reporter_pod" that begins reading filesystem stats from the PersistentVolumeClaim.
      * Resize PersistentVolumeClaim
      * Confirm that the filesystem has been resized in the "reporter_pod".
    """
    config.load_kube_config(str(kube_config))
    k8s_api_client = client.ApiClient()
    core_api = client.CoreV1Api()

    storage = render_j2_template(TEMPLATE_DIR, STORAGE_TEMPLATE, storage_class=storage_class)
    reporter_pod = render_j2_template(
        TEMPLATE_DIR, REPORTER_POD_TEMPLATE, storage_class=storage_class
    )
    namespace = reporter_pod["metadata"]["namespace"]
    reporter_pod_name = reporter_pod["metadata"]["name"]
    reporter_pod_mount = reporter_pod["spec"]["containers"][0]["volumeMounts"][0]["mountPath"]
    pvc_name = storage["metadata"]["name"]

    def current_size(pod_name: str, ns: str) -> int:
        timeout, initial_stats = 120, []
        while len(initial_stats) < 2 and timeout > 0:
            pod_log = core_api.read_namespaced_pod_log(pod_name, ns)
            initial_stats = pod_log.splitlines()
            time.sleep(1)
            timeout -= 1
        report = json.loads(initial_stats[-1])
        return report["size"] * report["blocks"]

    try:
        logger.info("Creating PersistentVolumeClaim %s", pvc_name)
        utils.create_from_dict(k8s_api_client, storage)

        logger.info("Creating Writer Pod %s", reporter_pod_name)
        utils.create_from_dict(k8s_api_client, reporter_pod)
        wait_for_pod(core_api, reporter_pod_name, namespace, target_state=RUNNING_POD_STATE)

        logger.info("Read initial filesystem stats")
        initial_size = current_size(reporter_pod_name, namespace)
        logger.info("Initial stats: %s", initial_size)

        # Resize the PVC
        storage["spec"]["resources"]["requests"]["storage"] = "2Gi"
        core_api.patch_namespaced_persistent_volume_claim(pvc_name, namespace, storage)

        logger.info("Wait for pvc resize")
        wait_for_pvc_resize(core_api, pvc_name, namespace, at_least="2Gi")

        final_size, timeout = initial_size, 120
        while (
            timeout > 0
            and (final_size := current_size(reporter_pod_name, namespace)) == initial_size
        ):
            logger.info("Waiting for filesystem resize, current size: %s", final_size)
            time.sleep(1)
            timeout -= 1

        logger.info("Final stats: %s", final_size)
        assert final_size > initial_size, "Filesystem resize did not complete"

        # check pvc size from api, same way kubectl gets it
        pvc_final = core_api.read_namespaced_persistent_volume_claim(pvc_name, namespace)
        reported_size = pvc_final.status.capacity.get("storage")

        assert (
            reported_size == "2Gi"
        ), f"PVC expected size should be 2Gi, though K8s says its {reported_size}"

        # check in pod that size also matches what we expect
        resp = stream(
            core_api.connect_get_namespaced_pod_exec,
            reporter_pod_name,
            namespace,
            command=["df", "-B1", reporter_pod_mount],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
        )
        logger.info("file sys size from inside pod:\n%s", resp)
        match = re.search(r"/dev/\S+\s+(\d+)", resp)
        assert match, "failed to find storage size from command"
        reported_bytes = int(match.group(1))
        expected_bytes = 2 * 1024 * 1024 * 1024  # 2 Gi
        tolerance = 0.93
        assert (
            reported_bytes >= expected_bytes * tolerance
        ), "Filesystem size report does not match actual volume size"
    finally:
        core_api.delete_namespaced_pod(reporter_pod_name, namespace)
        core_api.delete_namespaced_persistent_volume_claim(pvc_name, namespace)


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
    k8s_workers = ops_test.model.applications["k8s-worker"]

    (cephfsplugin,) = apps_api.list_namespaced_deployment(namespace, **CEPHFS_LS).items
    assert cephfsplugin.status.ready_replicas == len(k8s_workers.units)

    await run_test_storage_class(kube_config, "cephfs")


@pytest_asyncio.fixture()
async def ceph_rbd_disabled(ops_test):
    """Fixture which disable/enables ceph-rbd for a single test."""
    test_app = ops_test.model.applications["ceph-csi"]
    await test_app.set_config({"ceph-rbd-enable": "false"})
    await ops_test.model.wait_for_idle(apps=ready_apps(ops_test), timeout=5 * 60)
    yield
    await test_app.set_config({"ceph-rbd-enable": "true"})
    await ops_test.model.wait_for_idle(apps=ready_apps(ops_test), status="active", timeout=5 * 60)


@pytest.mark.usefixtures("cleanup_k8s", "ceph_rbd_disabled")
async def test_provisioners_disabled(ops_test):
    """Test that ceph-csi deployments do not include any provisioners."""
    test_app = ops_test.model.applications["ceph-csi"]
    for unit in test_app.units:
        assert unit.workload_status == "blocked"
        assert unit.workload_status_message == "Neither ceph-rbd nor cephfs is enabled."


async def test_conflicting_ceph_csi(ops_test: OpsTest):
    """Test that deploying ceph-csi twice in the same model fails the second."""
    app = ops_test.model.applications[CEPH_CSI_ALT]
    expected_msg = "resource collisions (action: list-resources)"
    try:
        await app.relate("kubernetes-info", "k8s")
        await ops_test.model.wait_for_idle(apps=[CEPH_CSI_ALT], status="blocked", timeout=5 * 60)
        assert any(expected_msg in u.workload_status_message for u in app.units)
    finally:
        await app.destroy_relation("kubernetes-info", "k8s")
        await ops_test.model.wait_for_idle(
            apps=[CEPH_CSI_ALT], wait_for_exact_units=0, timeout=5 * 60
        )


async def test_duplicate_ceph_csi(ops_test: OpsTest, namespace: str, kube_config: Path):
    """Test that deploying ceph-csi twice in the same model succeeds after removing conflicts."""
    app = ops_test.model.applications[CEPH_CSI_ALT]

    # Create a new namespace for the second ceph-csi deployment
    alt_namespace = f"{namespace}-alt"
    config.load_kube_config(str(kube_config))
    v1_core = client.CoreV1Api()
    namespaces = [o.metadata.name for o in v1_core.list_namespace().items]
    if alt_namespace not in namespaces:
        v1_namespace = client.V1Namespace(metadata=client.V1ObjectMeta(name=alt_namespace))
        v1_core.create_namespace(v1_namespace)

    # Prepare to restore config after the test
    current_config = await app.get_config()
    await app.set_config(
        {
            "namespace": alt_namespace,
            "cephfs-enable": "true",
            "ceph-ext4-storage-class-name-formatter": "ceph-ext4-{namespace}",
            "ceph-xfs-storage-class-name-formatter": "ceph-xfs-{namespace}",
            "cephfs-storage-class-name-formatter": "cephfs-{namespace}",
            "ceph-rbac-name-formatter": "{name}-{namespace}",
        }
    )

    try:
        await app.relate("kubernetes-info", "k8s")
        await ops_test.model.wait_for_idle(apps=[CEPH_CSI_ALT], status="active", timeout=5 * 60)

        await run_test_storage_class(kube_config, "ceph-xfs")
        await run_test_storage_class(kube_config, f"ceph-xfs-{alt_namespace}")
        await run_test_storage_class(kube_config, "ceph-ext4")
        await run_test_storage_class(kube_config, f"ceph-ext4-{alt_namespace}")
    finally:
        await app.destroy_relation("kubernetes-info", "k8s")
        await ops_test.model.wait_for_idle(
            apps=[CEPH_CSI_ALT], wait_for_exact_units=0, timeout=5 * 60
        )
        await app.set_config(current_config)


@pytest.fixture(scope="class")
def storage_api(kube_config: Path):
    """Fixture which provides kubernetes storage api client."""
    config.load_kube_config(str(kube_config))
    return client.StorageV1Api()


@pytest.fixture(scope="function")
def mock_storage_class(request, storage_api: client.StorageV1Api):
    camel = request.node.name.lower().replace("_", "")
    body = client.V1StorageClass(
        api_version="storage.k8s.io/v1",
        kind="StorageClass",
        metadata=client.V1ObjectMeta(name=camel, annotations={DEFAULT_ANNOTATION: "true"}),
        provisioner="doesNotExist",
        reclaim_policy="Retain",
        parameters={},
        volume_binding_mode="Immediate",  # Or "WaitForFirstConsumer"
    )
    sc = storage_api.create_storage_class(body=body)

    try:
        yield sc
    finally:
        storage_api.delete_storage_class(name=camel)


class TestStorageClass:
    """Test suite for storage class related tests."""

    async def test_update_default(self, storage_api: client.StorageV1Api, ops_test: OpsTest):
        """Test that updating "default-storage" configuration takes effect in k8s resources."""

        async def assert_is_default_class(expected_default: str, api: client.StorageV1Api):
            for class_ in api.list_storage_class().items:
                sc = class_.metadata.name
                annotations = class_.metadata.annotations
                is_default = annotations and annotations[DEFAULT_ANNOTATION]
                if sc == expected_default:
                    assert is_default == "true", f"Expected to find {sc} as the default"
                else:
                    assert is_default in (
                        "false",
                        None,
                    ), f"Expected to find {sc} not the default"

        expected_classes = ["ceph-xfs", "ceph-ext4"]
        ceph_csi_app = ops_test.model.applications["ceph-csi"]

        # Scan available StorageClasses and make sure that all expected classes are present.
        classes_to_test = []
        original_default = None
        logger.debug("Discovering available StorageClasses")
        for storage_class in storage_api.list_storage_class().items:
            name = storage_class.metadata.name
            anno = storage_class.metadata.annotations
            if is_default := anno and anno.get(DEFAULT_ANNOTATION) == "true":
                original_default = name

            classes_to_test.append(name)
            logger.debug("StorageClass: %s; isDefault: %s", name, is_default)

            if name not in expected_classes:
                pytest.fail("Unexpected storage class in the cluster: {}".format(name))

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

    @pytest.mark.parametrize("storage_class", ["ceph-xfs", "ceph-ext4"])
    @pytest.mark.usefixtures("cleanup_k8s", "ops_test")
    async def test_supported(self, kube_config: Path, storage_class: str):
        """Test that ceph can be used to create persistent volume.

        Isolated tests for xfs and ext4, cephfs comes later.
        """
        await run_test_storage_class(kube_config, storage_class)

    @pytest.mark.usefixtures("mock_storage_class")
    async def test_too_many_defaults(self, ops_test: OpsTest):
        """Test that enabling more than one default storage class warns the user."""
        app = ops_test.model.applications["ceph-csi"]
        async with ops_test.fast_forward("10s"):
            await ops_test.model.block_until(
                units_have_status(
                    app, "active", "Cluster contains multiple default StorageClasses"
                ),
                timeout=60,
            )

    async def test_no_matching_default(self, request, ops_test: OpsTest):
        """Test that setting default-storage to a non-existing storage warns the user."""
        app = ops_test.model.applications["ceph-csi"]
        async with set_test_config(app, {"default-storage": request.node.name}):
            msg = f"'{request.node.name}' doesn't match any charm managed StorageClass"
            await ops_test.model.wait_for_idle(apps=[app.name], timeout=5 * 60)
            await ops_test.model.block_until(
                units_have_status(app, "active", msg),
                timeout=60,
            )
        await ops_test.model.wait_for_idle(apps=[app.name], status="active", timeout=5 * 60)

    # Testing multiple cephfs pools
    #
    # Create a second data pool for cephfs
    #     $ ceph osd pool create ceph-fs_data_ci 2
    #     $ ceph fs add_data_pool ceph-fs ceph-fs_data_ci
    # config
    #     cephfs-enabled=true
    # Verify that the charm cannot create a unique storage class name formatter
    # config
    #     cephfs-storage-class-name-formatter=cephfs-{name}-{pool-id}
    # Verify that the cluster comes up satisfied
    # config
    #     default-storage=cephfs-{name}-{pool-id}
    # Verify that the status message errors about multiple default storage classes
    # clean up the extra pool after test
    #     $ ceph osd pool stats ceph-fs_data_ci
    #     $ ceph fs rm_data_pool ceph-fs ceph-fs_data_ci
    #     $ ceph osd pool stats ceph-fs_data_ci
    #     $ ceph tell mon.* injectargs --mon-allow-pool-delete=true
    #     $ ceph osd pool delete ceph-fs_data_ci ceph-fs_data_ci --yes-i-really-really-mean-it
