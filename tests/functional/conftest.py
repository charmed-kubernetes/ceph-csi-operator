# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
"""Pytest fixtures for functional tests."""
#  pylint: disable=W0621

import logging
import tempfile
from pathlib import Path

import pytest
from kubernetes import client, config
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def namespace() -> str:
    """Return namespace used for functional tests."""
    return "default"


@pytest.fixture(scope="module")
async def kube_config(ops_test: OpsTest) -> Path:
    """Return path to the kube config of the tested Kubernetes cluster.

    Config file is fetched from kubernetes-control-plane unit and stored in the temporary file.
    """
    k8s_cp = ops_test.model.applications["kubernetes-control-plane"].units[0]

    with tempfile.TemporaryDirectory() as tmp_dir:
        kube_config_file = Path(tmp_dir).joinpath("kube_config")

        cmd = "juju scp -m {} {}:config {}".format(
            ops_test.model_name, k8s_cp.name, kube_config_file
        ).split()
        return_code, _, std_err = await ops_test.run(*cmd)
        assert return_code == 0, std_err
        yield kube_config_file


@pytest.fixture()
async def cleanup_k8s(kube_config, namespace: str):
    """Cleanup kubernetes resources created during test."""
    yield  # act only on teardown
    config.load_kube_config(str(kube_config))

    pod_prefixes = ["read-test-ceph-", "write-test-ceph"]
    pvc_prefix = "pvc-test-"
    core_api = client.CoreV1Api()

    for pod in core_api.list_namespaced_pod(namespace).items:
        pod_name = pod.metadata.name
        if any(pod_name.startswith(prefix) for prefix in pod_prefixes):
            try:
                logger.info("Removing Pod %s", pod_name)
                core_api.delete_namespaced_pod(pod_name, namespace)
            except client.ApiException as exc:
                if exc.status != 404:
                    raise exc
                logger.debug("Pod %s is already removed", pod_name)

    for pvc in core_api.list_namespaced_persistent_volume_claim(namespace).items:
        pvc_name = pvc.metadata.name
        if pvc_name.startswith(pvc_prefix):
            try:
                logger.info("Removing PersistentVolumeClaim %s", pvc_name)
                core_api.delete_namespaced_persistent_volume_claim(pvc_name, namespace)
            except client.ApiException as exc:
                if exc.status != 404:
                    raise exc
                logger.debug("PersistentVolumeClaim %s is already removed.", pvc_name)
