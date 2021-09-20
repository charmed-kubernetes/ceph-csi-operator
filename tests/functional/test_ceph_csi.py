# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing
import pytest
from pytest_operator.plugin import OpsTest


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test):
    ceph_csi_charm = await ops_test.build_charm(".")
    await ops_test.model.deploy(ops_test.render_bundle(
        "tests/functional/bundle.yaml", master_charm=ceph_csi_charm))
    await ops_test.model.wait_for_idle(wait_for_active=True, timeout=60 * 60, check_freq=5, raise_on_error=False)


async def test_active_status(ops_test: OpsTest):
    for unit in ops_test.model.applications["ceph-csi"].units:
        assert unit.workload_status == "active"
        assert unit.workload_status_message == "Unit is ready"
