# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Pytest fixtures for ceph-csi cross-model integration tests."""

import logging
import os
from pathlib import Path
from typing import AsyncGenerator

import pytest
import pytest_asyncio
import yaml
from kubernetes import client, config
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


MICROCEPH_CHANNEL = "latest/edge/csi"


def pytest_addoption(parser):
    parser.addoption(
        "--microceph-charm",
        action="store",
        default=None,
        help="Path to a pre-built microceph charm artifact (overrides charmhub).",
    )
    parser.addoption(
        "--microceph-channel",
        action="store",
        default=None,
        help=f"Charmhub channel for microceph (default: {MICROCEPH_CHANNEL}).",
    )


@pytest.fixture(scope="session")
def microceph_source(pytestconfig) -> dict:
    """Return microceph deploy source: either a local path or charmhub channel.

    Returns a dict with either {"charm": Path} or {"channel": str}.
    Priority: --microceph-charm > MICROCEPH_CHARM env > --microceph-channel > default channel.
    """
    charm_path = pytestconfig.getoption("microceph_charm") or os.environ.get("MICROCEPH_CHARM")
    if charm_path:
        path = Path(charm_path).resolve()
        if not path.exists():
            pytest.fail(f"microceph charm not found at {path}")
        return {"charm": path}

    channel = (
        pytestconfig.getoption("microceph_channel")
        or os.environ.get("MICROCEPH_CHANNEL")
        or MICROCEPH_CHANNEL
    )
    return {"channel": channel}


@pytest.fixture(scope="module")
def namespace(ops_test: OpsTest) -> str:
    """Return namespace used for ceph-csi installation."""
    return ops_test.model_name


@pytest_asyncio.fixture(scope="module")
async def kube_config(ops_test: OpsTest) -> AsyncGenerator[Path, None]:
    """Return path to the kube config of the tested Kubernetes cluster.

    Config file is fetched from the k8s unit and stored in a temporary file.
    """
    k8s_app = ops_test.model.applications["k8s"]
    (leader,) = [u for u in k8s_app.units if (await u.is_leader_from_status())]
    action = await leader.run_action("get-kubeconfig")
    action = await action.wait()
    success = (
        action.status == "completed"
        and action.results["return-code"] == 0
        and "kubeconfig" in action.results
    )

    if not success:
        logging.error(f"status: {action.status}")
        logging.error(f"results:\n{yaml.safe_dump(action.results, indent=2)}")
        pytest.fail("Failed to copy kubeconfig from k8s")

    kubeconfig_path = ops_test.tmp_path / "kubeconfig"
    with kubeconfig_path.open("w") as f:
        f.write(action.results["kubeconfig"])
    yield kubeconfig_path


@pytest_asyncio.fixture()
async def cleanup_k8s(kube_config):
    """Cleanup kubernetes resources created during test."""
    yield  # act only on teardown
    config.load_kube_config(str(kube_config))

    pod_namespace = "default"
    pod_prefixes = ["read-test-ceph", "write-test-ceph"]
    pvc_prefix = "pvc-test-"
    core_api = client.CoreV1Api()

    for pod in core_api.list_namespaced_pod(pod_namespace).items:
        pod_name = pod.metadata.name
        if any(pod_name.startswith(prefix) for prefix in pod_prefixes):
            try:
                logger.info("Removing Pod %s", pod_name)
                core_api.delete_namespaced_pod(pod_name, pod_namespace)
            except client.ApiException as exc:
                if exc.status != 404:
                    raise exc
                logger.debug("Pod %s is already removed", pod_name)

    for pvc in core_api.list_namespaced_persistent_volume_claim(pod_namespace).items:
        pvc_name = pvc.metadata.name
        if pvc_name.startswith(pvc_prefix):
            try:
                logger.info("Removing PersistentVolumeClaim %s", pvc_name)
                core_api.delete_namespaced_persistent_volume_claim(pvc_name, pod_namespace)
            except client.ApiException as exc:
                if exc.status != 404:
                    raise exc
                logger.debug("PersistentVolumeClaim %s is already removed.", pvc_name)
