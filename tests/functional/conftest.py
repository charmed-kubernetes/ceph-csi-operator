# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
"""Pytest fixtures for functional tests."""
#  pylint: disable=W0621

import logging
from pathlib import Path

import pytest
import yaml
from kubernetes import client, config
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def namespace(ops_test) -> str:
    """Return namespace used for ceph-csi installment."""
    return ops_test.model_name


@pytest.fixture(scope="module")
async def kube_config(ops_test: OpsTest) -> Path:
    """Return path to the kube config of the tested Kubernetes cluster.

    Config file is fetched from kubernetes-control-plane unit and stored in the temporary file.
    """
    k_c_p = ops_test.model.applications["kubernetes-control-plane"]
    (leader,) = [u for u in k_c_p.units if (await u.is_leader_from_status())]
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
        pytest.fail("Failed to copy kubeconfig from kubernetes-control-plane")

    kubeconfig_path = ops_test.tmp_path / "kubeconfig"
    with kubeconfig_path.open("w") as f:
        f.write(action.results["kubeconfig"])
    yield kubeconfig_path


@pytest.fixture()
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
