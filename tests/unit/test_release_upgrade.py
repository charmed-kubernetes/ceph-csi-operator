# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests of the staged Ceph CSI release upgrade guard in src/charm.py."""

import unittest.mock as mock

import charms.contextual_status as status
import pytest
from lightkube.models.apps_v1 import (
    DaemonSetSpec,
    DaemonSetStatus,
    DeploymentSpec,
    DeploymentStatus,
)
from lightkube.models.core_v1 import Container, PodSpec, PodTemplateSpec
from lightkube.models.meta_v1 import LabelSelector, ObjectMeta
from lightkube.resources.apps_v1 import DaemonSet, Deployment
from ops.testing import Harness

from charm import CephCsiCharm, _is_staged_upgrade, _rollout_pending

CEPH_CONTEXT = {
    "auth": "cephx",
    "fsid": "12345",
    "kubernetes_key": "123",
    "mon_hosts": ["10.0.0.1"],
    "user": "ceph-csi",
    "provisioner_replicas": 3,
    "enable_host_network": "false",
    "fsname": None,
}
IMAGE = "ghcr.io/canonical/cdk/cephcsi/cephcsi:{}"


@pytest.fixture
def harness():
    harness = Harness(CephCsiCharm)
    harness.set_leader(True)
    try:
        yield harness
    finally:
        harness.cleanup()


@pytest.fixture(autouse=True)
def ceph_context():
    with mock.patch(
        "charm.CephCsiCharm.ceph_context", new_callable=mock.PropertyMock
    ) as ceph_context:
        ceph_context.return_value = CEPH_CONTEXT
        yield ceph_context


@pytest.fixture
def not_found(api_error_klass):
    error = api_error_klass()
    error.status.code = 404
    error.status.message = "Not Found"
    return error


def provisioner(component, image, ready=True, generation=1):
    driver = f"csi-{component}plugin"
    replicas = 3
    return Deployment(
        metadata=ObjectMeta(name=f"{driver}-provisioner", generation=generation),
        spec=DeploymentSpec(
            replicas=replicas,
            selector=LabelSelector(),
            template=PodTemplateSpec(
                spec=PodSpec(containers=[Container(name=driver, image=image)])
            ),
        ),
        status=DeploymentStatus(
            observedGeneration=generation,
            updatedReplicas=replicas if ready else 1,
            readyReplicas=replicas if ready else 1,
        ),
    )


def nodeplugin(component, image, ready=True, generation=1):
    driver = f"csi-{component}plugin"
    nodes = 4
    return DaemonSet(
        metadata=ObjectMeta(name=driver, generation=generation),
        spec=DaemonSetSpec(
            selector=LabelSelector(),
            template=PodTemplateSpec(
                spec=PodSpec(containers=[Container(name=driver, image=image)])
            ),
        ),
        status=DaemonSetStatus(
            observedGeneration=generation,
            currentNumberScheduled=nodes,
            desiredNumberScheduled=nodes,
            numberMisscheduled=0,
            numberReady=nodes if ready else 2,
            updatedNumberScheduled=nodes if ready else 2,
        ),
    )


def deploy(lk_client, not_found, *workloads):
    """Make the Kubernetes API serve the given workloads and 404 everything else."""
    served = {(type(w), w.metadata.name): w for w in workloads}

    def get(kind, name, **_):
        if (kind, name) in served:
            return served[(kind, name)]
        raise not_found

    lk_client.get.side_effect = get


def releases(rbd="v3.18.2", cephfs=None, ready=True):
    """Provisioner and nodeplugin pairs at the given releases."""
    workloads = []
    for component, release in (("rbd", rbd), ("cephfs", cephfs)):
        if release:
            image = IMAGE.format(release)
            workloads += [
                provisioner(component, image, ready),
                nodeplugin(component, image, ready),
            ]
    return workloads


def guard(harness):
    """Run the guard, returning the unit status it set."""
    with status.context(harness.charm.unit):
        try:
            harness.charm.check_release_upgrade()
        except status.ReconcilerError as e:
            return str(e)
    return None


def applied_images(lk_client):
    images = set()
    for call in lk_client.apply.call_args_list:
        obj = call.args[0]
        if isinstance(obj, (Deployment, DaemonSet)):
            images |= {
                c.image
                for c in obj.spec.template.spec.containers
                if "/cephcsi/cephcsi:" in c.image
            }
    return images


def test_default_release_is_loadable_and_coherent(harness):
    harness.begin()
    versions = {name: m.current_release for name, m in harness.charm.collector.manifests.items()}
    assert versions["rbd"] == versions["cephfs"] == harness.charm.config["release"]
    for name in ("rbd", "cephfs"):
        manifest = harness.charm.collector.manifests[name]
        assert manifest.current_release in manifest.releases
        assert manifest.resources or not manifest.config["enabled"]


def test_default_only_refresh_rolls_out_new_release(harness, lk_client, not_found):
    """A refresh whose only change is the schema default must apply the new release."""
    harness.update_config({"release": "v3.9.0"})  # the previous charm schema default
    harness.begin()
    previous = harness.charm.evaluate_manifests()
    harness.charm.stored.config_hash = previous

    harness.update_config(unset=["release"])  # the refreshed charm's default applies
    refreshed = harness.charm.evaluate_manifests()
    assert refreshed != previous

    deploy(lk_client, not_found)
    harness.charm.install_manifests(refreshed)
    assert harness.charm.stored.config_hash == refreshed
    assert applied_images(lk_client) == {IMAGE.format(harness.charm.config["release"])}


@pytest.mark.parametrize(
    "current, target, allowed",
    [
        ("v3.17.2", "v3.18.2", True),
        ("v3.18.1", "v3.18.2", True),
        ("v3.18.2", "v3.18.2", True),
        ("v3.16.3", "v3.18.2", False),
        ("v3.9.0", "v3.18.2", False),
        ("v3.18.2", "v3.17.2", False),
        ("v3.18.2", "v3.18.1", False),
        ("v2.99.0", "v3.0.0", False),
        ("latest", "v3.18.2", False),
        ("v3.17.2", "stable", False),
    ],
)
def test_is_staged_upgrade(current, target, allowed):
    assert _is_staged_upgrade(current, target) is allowed


def test_rollout_pending_needs_status():
    for obj in (
        provisioner("rbd", IMAGE.format("v3.18.2")),
        nodeplugin("rbd", IMAGE.format("v3.18.2")),
    ):
        obj.status = None
        assert _rollout_pending(obj)


def test_rollout_pending_observed_generation():
    obj = provisioner("rbd", IMAGE.format("v3.18.2"))
    obj.metadata.generation = 2
    assert _rollout_pending(obj)
    obj.status.observedGeneration = 2
    assert not _rollout_pending(obj)


@pytest.mark.parametrize("builder", [provisioner, nodeplugin])
def test_rollout_pending_replicas(builder):
    assert not _rollout_pending(builder("rbd", IMAGE.format("v3.18.2"), ready=True))
    assert _rollout_pending(builder("rbd", IMAGE.format("v3.18.2"), ready=False))


def test_fresh_install_is_allowed(harness, lk_client, not_found):
    harness.begin()
    deploy(lk_client, not_found)
    assert guard(harness) is None


def test_no_enabled_component_skips_the_guard(harness, lk_client):
    harness.update_config({"ceph-rbd-enable": False, "cephfs-enable": False})
    harness.begin()
    assert guard(harness) is None
    lk_client.get.assert_not_called()


def test_adjacent_minor_upgrade_is_allowed(harness, lk_client, not_found):
    harness.begin()
    deploy(lk_client, not_found, *releases(rbd="v3.17.2"))
    assert guard(harness) is None


def test_old_default_to_new_default_is_blocked_without_applying(harness, lk_client, not_found):
    """A refresh over the previous v3.9.0 default must not jump straight to the new default."""
    harness.begin()
    deploy(lk_client, not_found, *releases(rbd="v3.9.0"))
    harness.charm.stored.config_hash = 1

    with status.context(harness.charm.unit):
        with pytest.raises(status.ReconcilerError, match="skips releases"):
            harness.charm.install_manifests(2)

    lk_client.apply.assert_not_called()
    assert harness.charm.stored.config_hash == 1
    assert harness.charm.unit.status.name == "blocked"
    assert "release=v3.9.0" in harness.charm.unit.status.message


def test_downgrade_is_blocked(harness, lk_client, not_found):
    harness.update_config({"release": "v3.17.2"})
    harness.begin()
    deploy(lk_client, not_found, *releases(rbd="v3.18.2"))
    message = guard(harness)
    assert "downgrade" in message
    assert "release=v3.18.2" in message


def test_partial_known_transition_converges(harness, lk_client, not_found):
    """One workload already moved to the target; the other is an adjacent minor behind."""
    harness.begin()
    image_old, image_new = IMAGE.format("v3.17.2"), IMAGE.format("v3.18.2")
    deploy(
        lk_client,
        not_found,
        provisioner("rbd", image_new),
        nodeplugin("rbd", image_old),
    )
    assert guard(harness) is None


def test_partial_transition_spanning_two_minors_is_blocked(harness, lk_client, not_found):
    harness.begin()
    deploy(
        lk_client,
        not_found,
        provisioner("rbd", IMAGE.format("v3.18.2")),
        nodeplugin("rbd", IMAGE.format("v3.16.3")),
    )
    assert "v3.16.3 -> v3.18.2" in guard(harness)


def test_rollout_in_progress_waits(harness, lk_client, not_found):
    harness.begin()
    deploy(lk_client, not_found, *releases(rbd="v3.17.2", ready=False))
    assert "Waiting for the running" in guard(harness)


def test_target_rollout_in_progress_is_not_blocked(harness, lk_client, not_found):
    """Re-applying the release that is already rolling out is safe."""
    harness.begin()
    deploy(lk_client, not_found, *releases(rbd="v3.18.2", ready=False))
    assert guard(harness) is None


def test_unidentifiable_workload_is_blocked(harness, lk_client, not_found):
    harness.begin()
    digest = "ghcr.io/canonical/cdk/cephcsi/cephcsi@sha256:" + "0" * 64
    deploy(lk_client, not_found, provisioner("rbd", digest))
    assert "Cannot determine" in guard(harness)


def test_workload_without_driver_container_is_blocked(harness, lk_client, not_found):
    harness.begin()
    odd = provisioner("rbd", IMAGE.format("v3.17.2"))
    odd.spec.template.spec.containers[0].name = "something-else"
    deploy(lk_client, not_found, odd)
    assert "Cannot determine" in guard(harness)


def test_deleted_workloads_can_be_recovered(harness, lk_client, not_found):
    """Applying onto an empty namespace is a fresh install, even after earlier deployments."""
    harness.begin()
    harness.charm.stored.config_hash = 1
    deploy(lk_client, not_found)
    harness.charm.install_manifests(2)
    assert harness.charm.stored.config_hash == 2
    assert applied_images(lk_client)


def test_pin_hint_orders_releases_numerically(harness, lk_client, not_found):
    """v3.10.0 is newer than v3.9.0, which a string sort would get backwards."""
    harness.update_config({"cephfs-enable": True})
    harness.begin()
    deploy(lk_client, not_found, *releases(rbd="v3.9.0", cephfs="v3.10.0"))
    message = guard(harness)
    assert "release=v3.10.0" in message
    assert "next: v3.11.x" in message


def test_stale_remembered_release_cannot_deadlock_the_guard(harness, lk_client, not_found):
    """rbd was disabled at v3.9.0 while cephfs stepped on to v3.12.0; re-enabling rbd."""
    harness.update_config({"cephfs-enable": True, "release": "v3.12.0"})
    harness.begin()
    harness.charm.stored.deployed_releases = {"rbd": "v3.9.0"}
    deploy(lk_client, not_found, *releases(rbd=None, cephfs="v3.12.0"))
    assert guard(harness) is None  # the observed release wins over the stale memory

    harness.update_config({"release": "v3.18.2"})
    message = guard(harness)
    assert "release=v3.12.0" in message  # a pinnable release always exists
    harness.update_config({"release": "v3.12.0"})
    assert guard(harness) is None


def test_sync_resources_does_not_record_a_release(harness, lk_client, not_found):
    harness.begin()
    deploy(lk_client, not_found, *releases(rbd="v3.17.2"))
    with mock.patch.object(harness.charm.collector, "apply_missing_resources"):
        harness.run_action("sync-resources", {})
    assert dict(harness.charm.stored.deployed_releases) == {}


def test_disable_then_enable_cannot_bypass_the_guard(harness, lk_client, not_found):
    """Disabling a component deletes its workloads; re-enabling must still be staged."""
    harness.begin()
    deploy(lk_client, not_found, *releases(rbd="v3.9.0"))
    harness.charm.stored.deployed_releases = {"rbd": "v3.9.0"}
    deploy(lk_client, not_found)  # the disable purged every RBD workload
    assert "v3.9.0 -> v3.18.2 skips" in guard(harness)

    harness.update_config({"release": "v3.9.0"})
    assert guard(harness) is None  # redeploying the remembered release is allowed


def test_applied_release_is_remembered(harness, lk_client, not_found):
    harness.update_config({"release": "v3.17.2"})
    harness.begin()
    deploy(lk_client, not_found)
    harness.charm.install_manifests(5)
    assert dict(harness.charm.stored.deployed_releases) == {"rbd": "v3.17.2"}


def test_live_workloads_win_over_the_remembered_release(harness, lk_client, not_found):
    harness.begin()
    harness.charm.stored.deployed_releases = {"rbd": "v3.9.0"}
    deploy(lk_client, not_found, *releases(rbd="v3.17.2"))
    assert guard(harness) is None


@pytest.fixture
def reconciling(harness):
    """Reconcile on update-status through the evaluate/install steps only."""
    harness.begin()
    charm = harness.charm
    charm.reconciler.reconcile_function = lambda _: charm.install_manifests(
        config_hash=charm.evaluate_manifests()
    )
    return charm


def test_update_status_applies_once_the_rollout_finishes(
    harness, reconciling, lk_client, not_found
):
    deploy(lk_client, not_found, *releases(rbd="v3.17.2", ready=False))
    harness.charm.on.update_status.emit()
    assert harness.charm.unit.status.name == "waiting"
    assert not harness.charm.reconciler.stored.reconciled
    assert harness.charm.stored.config_hash == 0
    lk_client.apply.assert_not_called()

    deploy(lk_client, not_found, *releases(rbd="v3.17.2"))  # rollout finished
    harness.charm.on.update_status.emit()
    assert harness.charm.stored.config_hash != 0
    assert applied_images(lk_client) == {IMAGE.format(harness.charm.config["release"])}
    assert harness.charm.unit.status.name != "waiting"


def test_pinning_the_deployed_release_unblocks_without_applying(
    harness, reconciling, lk_client, not_found
):
    harness.update_config({"release": "v3.9.0"})
    previous = reconciling.evaluate_manifests()
    reconciling.stored.config_hash = previous  # the state before the refresh
    harness.update_config(unset=["release"])  # refresh: new default
    deploy(lk_client, not_found, *releases(rbd="v3.9.0"))

    harness.charm.on.update_status.emit()
    assert harness.charm.unit.status.name == "blocked"
    lk_client.apply.assert_not_called()

    harness.update_config({"release": "v3.9.0"})  # operator pins
    harness.charm.on.update_status.emit()
    assert harness.charm.unit.status.name != "blocked"
    assert harness.charm.stored.config_hash == previous
    lk_client.apply.assert_not_called()


def test_api_failure_waits(harness, lk_client, api_error_klass):
    harness.begin()
    error = api_error_klass()
    error.status.code = 500
    lk_client.get.side_effect = error
    assert "Waiting for Kubernetes API" in guard(harness)


def test_both_components_are_checked(harness, lk_client, not_found):
    harness.update_config({"cephfs-enable": True})
    harness.begin()
    deploy(lk_client, not_found, *releases(rbd="v3.17.2", cephfs="v3.15.1"))
    assert "v3.15.1 -> v3.18.2" in guard(harness)
    deploy(lk_client, not_found, *releases(rbd="v3.17.2", cephfs="v3.17.2"))
    assert guard(harness) is None


def test_unsupported_release_is_blocked(harness):
    harness.update_config({"release": "latest"})
    harness.begin()
    assert "Unsupported release 'latest'" in guard(harness)


def test_non_leader_never_reads_or_applies(harness, lk_client):
    harness.set_leader(False)
    harness.begin()
    harness.charm.install_manifests(2)
    lk_client.get.assert_not_called()
    lk_client.apply.assert_not_called()


def test_sync_resources_action_cannot_bypass_the_guard(harness, lk_client, not_found):
    harness.begin()
    deploy(lk_client, not_found, *releases(rbd="v3.9.0"))
    with mock.patch.object(harness.charm.collector, "apply_missing_resources") as apply_missing:
        with pytest.raises(Exception, match="skips releases"):
            harness.run_action("sync-resources", {})
    apply_missing.assert_not_called()
    lk_client.apply.assert_not_called()


def test_sync_resources_action_runs_after_a_permitted_transition(harness, lk_client, not_found):
    harness.begin()
    deploy(lk_client, not_found, *releases(rbd="v3.17.2"))
    with mock.patch.object(harness.charm.collector, "apply_missing_resources") as apply_missing:
        harness.run_action("sync-resources", {})
    apply_missing.assert_called_once()
