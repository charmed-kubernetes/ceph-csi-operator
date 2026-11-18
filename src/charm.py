#!/usr/bin/env python3
# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
"""Dispatch logic for the ceph-csi storage charm."""

import json
import logging
import re
from functools import cached_property
from typing import Any, Dict, List, NamedTuple, NoReturn, Optional, Set, Tuple, Union, cast

import charms.contextual_status as status
import charms.operator_libs_linux.v0.apt as apt
import ops
import ops.manifests.literals as manifest_literals
from charms.reconciler import Reconciler
from interface_ceph_client import ceph_client  # type: ignore
from lightkube import Client, KubeConfig
from lightkube.core.exceptions import ApiError
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.apps_v1 import DaemonSet, Deployment
from lightkube.resources.core_v1 import Namespace
from lightkube.resources.storage_v1 import StorageClass
from ops.manifests import Collector, HashableResource, ManifestClientError, ResourceAnalysis

import literals
import utils
from manifests_base import Manifests, SafeManifest
from manifests_cephfs import CephFSManifests, CephStorageClass
from manifests_config import ConfigManifests
from manifests_rbd import RBDManifests

logger = logging.getLogger(__name__)

# A Ceph CSI release, as used by upstream tags, charm config and manifest directories.
RELEASE = re.compile(r"v(\d+)\.(\d+)\.(\d+)")

# The release pinned on a container image, e.g. ghcr.io/canonical/cdk/cephcsi/cephcsi:v3.18.2
IMAGE_RELEASE = re.compile(rf":({RELEASE.pattern})$")


class WorkloadState(NamedTuple):
    """Observed state of the Ceph CSI workloads of one component."""

    releases: Set[str]
    """Releases pinned on the driver containers of the deployed workloads."""

    indeterminate: bool
    """True when a workload is deployed but its release cannot be established."""

    pending: bool
    """True when a deployed workload has not finished rolling out."""


def _release_tuple(release: str) -> Optional[Tuple[int, int, int]]:
    """Convert a 'vMAJOR.MINOR.PATCH' release into a comparable tuple."""
    match = RELEASE.fullmatch(release)
    return (int(match[1]), int(match[2]), int(match[3])) if match else None


def _image_release(image: Optional[str]) -> Optional[str]:
    """Read the release pinned on a container image."""
    match = IMAGE_RELEASE.search(image or "")
    return match[1] if match else None


def _is_staged_upgrade(current: str, target: str) -> bool:
    """Determine whether target is reachable from current in one upstream step.

    Upstream supports upgrading a single minor release at a time and documents no
    downgrade procedure: "we do not recommend any direct upgrades to 3.18 except
    from 3.17 to 3.18" -- docs/ceph-csi-upgrade.md at v3.18.2.
    """
    old, new = _release_tuple(current), _release_tuple(target)
    if old is None or new is None:
        return False
    return new >= old and new[0] == old[0] and new[1] - old[1] <= 1


def _compatible(a: str, b: str) -> bool:
    """Determine whether one target release can be reached from both a and b."""
    old, new = _release_tuple(a), _release_tuple(b)
    return old is not None and new is not None and old[0] == new[0] and abs(old[1] - new[1]) <= 1


def _rollout_pending(obj: Union[DaemonSet, Deployment]) -> bool:
    """Determine whether a deployed workload is still rolling out."""
    meta = obj.metadata
    if meta is None or obj.status is None:
        return True
    if (obj.status.observedGeneration or 0) < (meta.generation or 0):
        return True
    if isinstance(obj, Deployment):
        state = obj.status
        wanted = (obj.spec.replicas or 0) if obj.spec else 0
        return (state.updatedReplicas or 0) < wanted or (state.readyReplicas or 0) < wanted
    wanted = obj.status.desiredNumberScheduled or 0
    return (obj.status.updatedNumberScheduled or 0) < wanted or (
        obj.status.numberReady or 0
    ) < wanted


def _blocked(msg: str) -> NoReturn:
    """Block the unit and escape the reconcile loop."""
    status.add(ops.BlockedStatus(msg))
    raise status.ReconcilerError(msg)


def _waiting(msg: str) -> NoReturn:
    """Set the unit waiting and escape the reconcile loop."""
    status.add(ops.WaitingStatus(msg))
    raise status.ReconcilerError(msg)


class DynamicActiveStatus(ops.ActiveStatus):
    """An ActiveStatus class that can be updated.

    Attributes:
        message (str): explanation of the unit status
        prefix  (str): Optional prefix to the unit status
        postfix (str): Optional postfix to the unit status
    """

    def __init__(self, msg: str = "Ready") -> None:
        """Initialise the DynamicActiveStatus."""
        super().__init__(msg)
        self.prefix: str = ""
        self.postfix: str = ""

    @property
    def message(self) -> str:
        """Return the message for the status."""
        pre = f"{self.prefix} :" if self.prefix else ""
        post = f" ({self.postfix})" if self.postfix else ""
        return f"{pre}{self._message}{post}"

    @message.setter
    def message(self, message: str) -> None:
        """Set the message for the status.

        Args:
            message (str): explanation of the unit status
        """
        self._message = message


class UpdateStatusHandler(ops.Object):
    """Handler for the update-status event in the ceph-csi operator.

    This class observes the `update_status` event and handles it by checking the
    status of the manifest installer and updating the unit's workload version accordingly.

    Attributes:
        charm (CharmBase): The charm instance that this handler is associated with.
        active_status (DynamicActiveStatus): The active status object used to manage
            the unit's status during the update process.
    """

    def __init__(self, charm: "CephCsiCharm"):
        """Initialize the UpdateStatusEvent.

        Args:
            charm: The charm instance that is instantiating this event.
        """
        super().__init__(charm, "update_status")
        self.charm = charm
        self.active_status = DynamicActiveStatus()
        self.charm.framework.observe(self.charm.on.update_status, self._on_update_status)

    def run(self) -> None:
        if not (
            self.charm.config[literals.CONFIG_CEPH_RBD_ENABLE]
            or self.charm.config[literals.CONFIG_CEPHFS_ENABLE]
        ):
            msg = "Neither ceph-rbd nor cephfs is enabled."
            status.add(ops.BlockedStatus(msg))
            raise status.ReconcilerError(msg)
        elif unready := self.charm.collector.unready:
            status.add(ops.WaitingStatus(", ".join(unready)))
            raise status.ReconcilerError("Waiting for deployment")
        elif str(self.charm.stored.namespace) != self.charm._configured_ns:
            status.add(ops.BlockedStatus("Namespace cannot be changed after deployment"))
        elif str(self.charm.stored.drivername) != self.charm._configured_drivername:
            status.add(
                ops.BlockedStatus("csidriver-name-formatter cannot be changed after deployment")
            )
        else:
            warnings: str = ""
            self.charm.unit.set_workload_version(self.charm.collector.short_version)
            if self.charm.unit.is_leader():
                self.charm.app.status = ops.ActiveStatus(self.charm.collector.long_version)
            try:
                storage_classes = self.charm.list_storage_classes()
                default_scs = self.charm.cluster_default_storage_classes(storage_classes)
                if len(default_scs) > 1:
                    warnings = "Cluster contains multiple default StorageClasses"
                elif self.charm.is_default_storage_class_missing(default_scs):
                    fmt = self.charm.default_storage_fmt
                    warnings = f"'{fmt}' doesn't match any charm managed StorageClasses"
            except ApiError:
                logger.exception("Failed to list StorageClasses for status warnings")

            self.active_status.postfix = warnings

    def _on_update_status(self, _: ops.UpdateStatusEvent) -> None:
        """Handle update-status event."""
        if not self.charm.reconciler.stored.reconciled:
            return

        try:
            with status.context(self.charm.unit, exit_status=self.active_status):
                self.run()
        except status.ReconcilerError:
            logger.exception("Can't update_status")


class CephCsiCharm(ops.CharmBase):
    """Charm the service."""

    stored = ops.StoredState()

    def __init__(self, *args: Any) -> None:
        """Setup even observers and initial storage values."""
        super().__init__(*args)
        self.ceph_client = ceph_client.CephClientRequires(self, literals.CEPH_CLIENT_RELATION)

        self.framework.observe(
            self.ceph_client.on.broker_available, self._on_ceph_client_broker_available
        )
        self.cli = utils.CephCLI(self)
        self.update_status = UpdateStatusHandler(self)
        self.reconciler = Reconciler(
            self, self.reconcile, exit_status=self.update_status.active_status
        )

        self.framework.observe(self.on.list_versions_action, self._list_versions)
        self.framework.observe(self.on.list_resources_action, self._list_resources)
        self.framework.observe(self.on.scrub_resources_action, self._scrub_resources)
        self.framework.observe(self.on.sync_resources_action, self._sync_resources)
        self.framework.observe(self.on.delete_storage_class_action, self._delete_storage_class)

        self.stored.set_default(config_hash=0)  # hashed value of the provider config once valid
        self.stored.set_default(destroying=False)  # True when the charm is being shutdown
        self.stored.set_default(namespace=self._configured_ns)
        self.stored.set_default(drivername=self._configured_drivername)
        # release last applied per component; outlives its workloads (disable, deletion)
        self.stored.set_default(deployed_releases={})

        self.collector = Collector(
            ConfigManifests(self),
            CephFSManifests(self),
            RBDManifests(self),
        )

    @property
    def default_storage_fmt(self) -> str:
        """Get the currently configured default storage class."""
        return cast(str, self.config[literals.CONFIG_DEFAULT_STORAGE])

    def _list_versions(self, event: ops.ActionEvent) -> None:
        self.collector.list_versions(event)

    def _list_resources(self, event: ops.ActionEvent) -> None:
        manifests = event.params.get("manifest", "")
        resources = event.params.get("resources", "")
        self.collector.list_resources(event, manifests, resources)

    def _scrub_resources(self, event: ops.ActionEvent) -> None:
        manifests = event.params.get("manifest", "")
        resources = event.params.get("resources", "")
        self.collector.scrub_resources(event, manifests, resources)

    def _sync_resources(self, event: ops.ActionEvent) -> None:
        manifests = event.params.get("manifest", "")
        resources = event.params.get("resources", "")
        try:
            self.check_release_upgrade()
            self.collector.apply_missing_resources(event, manifests, resources)
        except status.ReconcilerError as e:
            event.fail(str(e))
        except ManifestClientError as e:
            msg = "Failed to sync missing resources: "
            msg += " -> ".join(map(str, e.args))
            event.set_results({"result": msg})
        else:
            self.stored.deployed = True

    def list_storage_classes(self) -> List[StorageClass]:
        """List all StorageClasses in the cluster."""
        manifest, *_ = self.collector.manifests.values()
        return list(manifest.client.list(StorageClass))

    def cluster_default_storage_classes(self, scs: List[StorageClass]) -> List[StorageClass]:
        """Find all default StorageClasses in the cluster."""
        default_scs = [
            sc
            for sc in scs
            if (meta := sc.metadata)
            and (anno := meta.annotations)
            and anno.get(literals.DEFAULT_SC_ANNOTATION_NAME) == "true"
        ]
        return default_scs

    def is_default_storage_class_missing(self, scs: List[StorageClass]) -> bool:
        """Check if this charm has configured the cluster's default StorageClass."""
        if not self.default_storage_fmt:
            # No default StorageClass configured in charm, so no need to check
            return False
        # True if any of the storage classes (scs) were created by this charm
        return not any(
            sc.metadata
            and sc.metadata.labels
            and sc.metadata.labels.get(manifest_literals.APP_LABEL) == self.app.name
            for sc in scs
        )

    def _delete_storage_class(self, event: ops.ActionEvent) -> None:
        storage_class: Optional[str] = event.params.get("name")
        if storage_class not in ["cephfs", "ceph-xfs", "ceph-ext4"]:
            msg = "Invalid storage class name. Must be one of: cephfs, ceph-xfs, ceph-ext4"
            event.fail(msg)
            return

        manifest, *_ = self.collector.manifests.values()
        try:
            manifest.client.delete(StorageClass, name=storage_class)
        except ManifestClientError as e:
            msg = "Failed to delete storage class: "
            msg += " -> ".join(map(str, e.args))
            event.fail(msg)
        else:
            event.set_results({"result": f"Successfully deleted StorageClass/{storage_class}"})

    @property
    def _configured_ns(self) -> str:
        """Currently configured namespace."""
        return str(self.config.get("namespace") or literals.DEFAULT_NAMESPACE)

    @property
    def _configured_drivername(self) -> str:
        """Currently configured csi drivername."""
        return str(self.config.get("csidriver-name-formatter") or "{name}")

    @property
    def ceph_data(self) -> Dict[str, Any]:
        """Return Ceph data from ceph-client relation"""
        r_data = self.ceph_client.get_relation_data()
        return {k: r_data.get(k) for k in ("auth", "key", "mon_hosts")}

    @property
    def auth(self) -> Optional[str]:
        """Return Ceph auth mode from ceph-client relation"""
        return self.ceph_data["auth"]

    @property
    def key(self) -> Optional[str]:
        """Return Ceph key from ceph-client relation"""
        return self.ceph_data["key"]

    @property
    def mon_hosts(self) -> List[str]:
        """Return Ceph monitor hosts from ceph-client relation"""
        return self.ceph_data["mon_hosts"] or []

    @status.on_error(ops.BlockedStatus("Failed to install ceph apt packages."))
    def install_ceph_packages(self, event: ops.EventBase) -> None:
        """Install ceph deb packages"""
        for package in literals.CEPH_PACKAGES:
            self.unit.status = ops.MaintenanceStatus(f"Ensuring {package} package")
            latest = isinstance(event, (ops.InstallEvent, ops.UpgradeCharmEvent))
            state = apt.PackageState.Latest if latest else apt.PackageState.Present
            ceph = apt.DebianPackage.from_system(package)
            ceph.ensure(state)
            logger.info("Installing %s to version: %s", package, ceph.fullversion)

    @property
    def provisioner_replicas(self) -> int:
        """Get the number of csi-*plugin-provisioner replicas."""
        return int(self.config.get("provisioner-replicas") or 3)

    @property
    def enable_host_network(self) -> bool:
        """Get the hostNetwork enabling of csi-*plugin-provisioner deployments."""
        return bool(self.config.get("enable-host-networking"))

    @property
    def kubelet_dir(self) -> str:
        """Get the kubelet directory from the kubernetes-info relation (if available)"""
        relation = self.model.get_relation("kubernetes-info")
        if relation is None or relation.app is None:
            return "/var/lib/kubelet"

        return relation.data[relation.app].get("kubelet-root-dir") or "/var/lib/kubelet"

    @property
    def kubernetes_context(self) -> Dict[str, Any]:
        """Return context that can be used to render ceph resource files in templates/ folder."""
        return {"kubelet_dir": self.kubelet_dir}

    @cached_property
    def _client(self) -> Client:
        """Lightkube Client instance."""
        return Client(field_manager=f"{self.model.app.name}")

    @property
    def ceph_context(self) -> Dict[str, Any]:
        """Return context that can be used to render ceph resource files in templates/ folder."""
        return {
            "auth": self.auth,
            "fsid": utils.fsid(self.cli),
            "kubernetes_key": self.key,
            "mon_hosts": self.mon_hosts,
            "user": self.app.name,
            "provisioner_replicas": self.provisioner_replicas,
            "enable_host_network": json.dumps(self.enable_host_network),
            CephStorageClass.FILESYSTEM_LISTING: utils.ls_ceph_fs(self.cli),
        }

    @status.on_error(ops.WaitingStatus("Waiting for kubeconfig"))
    def check_kube_config(self) -> None:
        self.unit.status = ops.MaintenanceStatus("Evaluating kubernetes authentication")
        KubeConfig.from_env()

    def _create_namespace(self, namespace: str) -> Optional[ops.StatusBase]:
        """Create the namespace if it does not exist.

        Args:
            namespace (str): The name of the namespace to create.

        Returns:
            Optional[ops.StatusBase]: Returns None if the namespace was created or already exists,
            or a status indicating an error if the namespace could not be created.
        """
        if not self.unit.is_leader():
            logger.info("Waiting for namespace creation, not the leader")
            return ops.WaitingStatus(f"Waiting for namespace '{namespace}'")

        if not self.config["create-namespace"]:
            logger.info("Skipping namespace creation, create-namespace is False")
            return ops.BlockedStatus(f"Missing namespace '{namespace}'")

        ns_resource = Namespace(metadata=ObjectMeta(name=namespace))
        logger.info("Creating namespace '%s'", namespace)
        status.add(ops.MaintenanceStatus(f"Creating namespace: '{namespace}'"))
        try:
            self._client.create(ns_resource)
        except ApiError as e:
            if e.status.code == 409:  # Conflict
                # Namespace already exists, do not raise an error
                logger.info("Namespace '%s' already exists", namespace)
            else:
                logger.exception("Failed to create namespace '%s': %s", namespace, e)
                return ops.WaitingStatus(f"Waiting for namespace: {namespace}")
        return None

    def check_namespace(self) -> None:
        namespace = str(self.stored.namespace)
        self.unit.status = ops.MaintenanceStatus(f"Evaluating namespace: '{namespace}'")
        try:
            self._client.get(Namespace, name=namespace)
        except ApiError as e:
            if e.status.code == 404:
                if error := self._create_namespace(namespace):
                    status.add(error)
                    raise status.ReconcilerError(error.message)
            else:
                # surface any other errors besides not found
                status.add(ops.WaitingStatus("Waiting for Kubernetes API"))
                raise status.ReconcilerError("Waiting for Kubernetes API")

    @status.on_error(ops.WaitingStatus("Waiting for kubeconfig"))
    def _ceph_rbd_enabled(self) -> None:
        """Determine if CephRBD should be enabled or disabled."""

        if self.config[literals.CONFIG_CEPH_RBD_ENABLE]:
            self.unit.status = ops.MaintenanceStatus("Enabling CephRBD")
        else:
            self.unit.status = ops.MaintenanceStatus("Disabling CephRBD")
            if self.unit.is_leader():
                self._purge_manifest_by_name("rbd")

    @status.on_error(ops.WaitingStatus("Waiting for kubeconfig"))
    def _cephfs_enabled(self) -> None:
        """Determine if CephFS should be enabled or disabled."""
        if self.config[literals.CONFIG_CEPHFS_ENABLE]:
            self.unit.status = ops.MaintenanceStatus("Enabling CephFS")
            groups = {literals.CEPHFS_SUBVOLUMEGROUP}
            for volume in utils.ls_ceph_fs(self.cli):
                utils.ensure_subvolumegroups(self.cli, volume.name, groups)
        else:
            self.unit.status = ops.MaintenanceStatus("Disabling CephFS")
            if self.unit.is_leader():
                self._purge_manifest_by_name("cephfs")

    def prevent_collisions(self, event: ops.EventBase) -> None:
        """Prevent manifest collisions."""
        if self.unit.is_leader():
            self.unit.status = ops.MaintenanceStatus("Detecting manifest collisions")
            analyses: List[ResourceAnalysis] = self.collector.analyze_resources(event, "", "")
            count = sum(len(a.conflicting) for a in analyses)
            if count > 0:
                msg = f"{count} Kubernetes resource collision{'s'[:count^1]} (action: list-resources)"
                logger.error(msg)
                for analysis in analyses:
                    if analysis.conflicting:
                        logger.error(
                            " Collision count in '%s' is %d",
                            analysis.manifest,
                            len(analysis.conflicting),
                        )
                        for _ in sorted(map(str, analysis.conflicting)):
                            logger.error("   %s", _)
                status.add(ops.BlockedStatus(msg))
                raise status.ReconcilerError(msg)

    def _prevent_multiple_default_storageclasses(
        self, storage_classes: Set[HashableResource]
    ) -> None:
        """Prevent multiple StorageClasses from being marked as default.

        Args:
            storage_classes (Set[HashableResource]): Set of StorageClass resources to evaluate.
        Raises:
            status.ReconcilerError: If multiple StorageClasses are marked as default.
        """

        default_sc = set()
        for sc in storage_classes:
            meta = sc.resource.metadata
            if not meta:
                continue
            annotations = meta.annotations or {}
            if annotations.get(literals.DEFAULT_SC_ANNOTATION_NAME) == "true":
                default_sc.add(sc)

        if len(default_sc) > 1:
            names = ", ".join(sorted(str(sc.name) for sc in default_sc))
            msg = "Multiple StorageClasses are marked as default: " + names
            status.add(ops.BlockedStatus(msg))
            raise status.ReconcilerError(msg)

    def evaluate_manifests(self) -> int:
        """Evaluate all manifests."""
        self.unit.status = ops.MaintenanceStatus("Evaluating CephCSI")
        storage_classes: Set[HashableResource] = set()
        new_hash = 0
        for manifest in self.collector.manifests.values():
            manifest = cast(SafeManifest, manifest)
            if evaluation := manifest.evaluate():
                status.add(ops.BlockedStatus(evaluation))
                raise status.ReconcilerError(evaluation)
            storage_classes |= manifest.storage_classes()
            new_hash += manifest.hash()
        self._prevent_multiple_default_storageclasses(storage_classes)

        return new_hash

    def _workload_state(self, component: str) -> WorkloadState:
        """Inspect the deployed provisioner and nodeplugin of one component."""
        manifest = self.collector.manifests[component]
        namespace = str(self.stored.namespace)
        driver = f"csi-{component}plugin"
        releases: Set[str] = set()
        indeterminate = pending = False

        workloads: List[Tuple[Any, str]] = [
            (Deployment, f"{driver}-provisioner"),
            (DaemonSet, driver),
        ]
        for kind, name in workloads:
            try:
                obj: Union[DaemonSet, Deployment] = manifest.client.get(
                    kind, name, namespace=namespace
                )
            except ApiError as exc:
                if exc.status.code == 404:
                    continue
                logger.exception("Cannot read %s/%s in %s", kind.__name__, name, namespace)
                _waiting("Waiting for Kubernetes API to read deployed Ceph CSI releases")

            pod = obj.spec.template.spec if obj.spec else None
            release = next(
                (
                    found
                    for container in (pod.containers if pod else [])
                    if container.name == driver
                    for found in [_image_release(container.image)]
                    if found
                ),
                None,
            )
            if release is None:
                logger.warning("No release on %s/%s container %s", kind.__name__, name, driver)
                indeterminate = True
                continue
            releases.add(release)
            pending |= _rollout_pending(obj)

        return WorkloadState(releases, indeterminate, pending)

    def _enabled_components(self) -> List[str]:
        """List the Ceph CSI components enabled by the charm config."""
        return [
            component
            for component in ("rbd", "cephfs")
            if self.collector.manifests[component].config["enabled"]
        ]

    def _record_deployed_releases(self) -> None:
        """Remember the release applied for each enabled component."""
        recorded = dict(cast(Dict[str, str], self.stored.deployed_releases))
        for component in self._enabled_components():
            recorded[component] = self.collector.manifests[component].current_release
        self.stored.deployed_releases = recorded

    def check_release_upgrade(self) -> None:
        """Allow only the release transitions upstream documents as supported.

        Ceph CSI supports moving one minor release at a time, so the deployed
        release must be known and must be the target release or its immediate
        predecessor. Applying manifests on top of an unknown or more distant
        release is blocked with recovery instructions instead of attempted.
        """
        enabled = self._enabled_components()
        if not enabled:
            return
        # RBD and CephFS share the single `release` option, so one target applies to both.
        target = self.collector.manifests[enabled[0]].current_release
        if _release_tuple(target) is None:
            _blocked(f"Unsupported release '{target}'; run list-versions for valid releases")

        states = [self._workload_state(component) for component in enabled]
        deployed = {release for state in states for release in state.releases}
        # Without workloads (component disabled, or workloads deleted) the volumes still
        # belong to the last release this charm applied. A remembered release that cannot
        # be reconciled with what is observed (or with a newer remembered release) is stale;
        # honouring it would leave no release that could be pinned.
        remembered = cast(Dict[str, str], self.stored.deployed_releases)
        for component in sorted(
            (
                c
                for c, state in zip(enabled, states)
                if not state.releases and not state.indeterminate
            ),
            key=lambda c: _release_tuple(remembered.get(c, "")) or (0, 0, 0),
            reverse=True,
        ):
            release = remembered.get(component)
            if release and all(_compatible(release, other) for other in deployed):
                deployed.add(release)
        unreachable = sorted(
            (r for r in deployed if not _is_staged_upgrade(r, target)),
            key=lambda r: _release_tuple(r) or (0, 0, 0),
        )

        if any(state.indeterminate for state in states):
            # A workload exists, but the release it runs cannot be read from its image.
            _blocked(
                "Cannot determine the deployed Ceph CSI release from the workload images. "
                "Use a driver image tag of the form vX.Y.Z (for example, fix the image "
                "registry or release config so the tag is visible) and check the workload "
                "images with kubectl. Do not delete the Ceph CSI workloads: removing nodeplugin "
                "pods disrupts mounted volumes (CephFS FUSE mounts in particular, see the "
                "upstream upgrade guide), and once the workloads are gone the charm may treat the "
                "namespace as a new installation and skip the staged-upgrade check "
                "(unless it recorded the release it applied earlier)."
            )
        target_tuple = _release_tuple(target) or (0, 0, 0)
        newer = [r for r in unreachable if (_release_tuple(r) or (0, 0, 0)) > target_tuple]
        if newer:
            _blocked(
                f"Ceph CSI {newer[-1]} -> {target} is a downgrade, which upstream does not "
                f"support. Pin release={max(deployed, key=lambda r: _release_tuple(r) or ())}."
            )
        if unreachable:
            old = unreachable[0]
            newest = max(deployed, key=lambda r: _release_tuple(r) or ())
            major, minor, _ = _release_tuple(newest) or (0, 0, 0)
            _blocked(
                f"Ceph CSI {old} -> {target} skips releases upstream does not support. "
                f"Pin release={newest}, then upgrade one minor release at a time (next: "
                f"v{major}.{minor + 1}.x, see list-versions), waiting for both workloads "
                "to become ready at each step."
            )
        if deployed - {target} and any(state.pending for state in states):
            # Converging on the target is allowed, but only one rollout at a time.
            _waiting("Waiting for the running Ceph CSI rollout to finish")

    def install_manifests(self, config_hash: int) -> None:
        if cast(int, self.stored.config_hash) == config_hash:
            logger.info(f"No config changes detected. config_hash={config_hash}")
            return
        if self.unit.is_leader():
            self.check_release_upgrade()
            self.unit.status = ops.MaintenanceStatus("Deploying CephCSI")
            self.unit.set_workload_version("")
            for manifest in self.collector.manifests.values():
                try:
                    manifest.apply_manifests()
                except ManifestClientError as e:
                    failure_msg = " -> ".join(map(str, e.args))
                    status.add(ops.WaitingStatus(failure_msg))
                    logger.warning("Encountered retriable installation error: %s", e)
                    raise status.ReconcilerError(failure_msg)
            self._record_deployed_releases()

        self.stored.config_hash = config_hash

    def check_ceph_client(self) -> None:
        """Load data from ceph-mon:client relation and store it in StoredState.

        This method expects all the required data (key, mon_hosts) to be present in the
        relation data. If any of the expected keys is missing, none of the data will be loaded.

        :param relation: Instance of ceph-mon:client relation.
        :param remote_unit: Unit instance representing remote ceph-mon unit.
        :return: `True` if all the data successfully loaded, otherwise `False`
        """
        self.unit.status = ops.MaintenanceStatus("Checking Relations")
        try:
            relation = self.model.get_relation(literals.CEPH_CLIENT_RELATION)
        except ops.model.TooManyRelatedAppsError:
            status.add(ops.BlockedStatus("Multiple ceph-client relations"))
            raise status.ReconcilerError("Multiple ceph-client relations")

        if not relation:
            status.add(ops.BlockedStatus("Missing relation: ceph-client"))
            raise status.ReconcilerError("Missing relation: ceph-client")

        relation_data = self.ceph_client.get_relation_data()
        expected_relation_keys = ("auth", "key", "mon_hosts")

        missing_data = [key for key in expected_relation_keys if key not in relation_data]
        if missing_data:
            logger.warning("Ceph relation is missing data: %s", missing_data)
            status.add(ops.WaitingStatus("Ceph relation is missing data."))
            raise status.ReconcilerError("Ceph relation is missing data.")

    def reconcile(self, event: ops.EventBase) -> None:
        """Reconcile the charm state."""
        if self._destroying(event):
            leader = self.unit.is_leader()
            logger.info("purge manifests if leader(%s) event(%s)", leader, event)
            if leader:
                self._purge_all_manifests()
            return

        self.install_ceph_packages(event)
        self.check_kube_config()
        self.check_namespace()
        self.check_ceph_client()
        self.cli.configure()
        self._ceph_rbd_enabled()
        self._cephfs_enabled()
        hash = self.evaluate_manifests()
        self.prevent_collisions(event)
        self.install_manifests(config_hash=hash)
        self.update_status.run()

    def request_ceph_pools(self) -> None:
        """Request creation of Ceph pools from the ceph-client relation"""
        for pool_name in literals.REQUIRED_CEPH_POOLS:
            self.ceph_client.create_replicated_pool(name=pool_name)

    def request_ceph_permissions(self) -> None:
        """Request Ceph permissions from the ceph-client relation"""
        # Permissions needed for Ceph CSI
        # https://github.com/ceph/ceph-csi/blob/v3.6.0/docs/capabilities.md
        permissions = [
            "mon",
            "profile rbd, allow r",
            "mds",
            "allow rw",
            "mgr",
            "allow rw",
            "osd",
            "profile rbd, allow rw tag cephfs metadata=*",
        ]
        self.ceph_client.request_ceph_permissions(self.app.name, permissions)

    def _on_ceph_client_broker_available(self, event: ops.EventBase) -> None:
        """Use ceph-mon:client relation to request creation of ceph-pools and
        ceph user permissions
        """
        self.request_ceph_pools()
        self.request_ceph_permissions()
        self.reconciler.reconcile(event)

    def _purge_all_manifests(self) -> None:
        """Purge resources created by this charm."""
        self.unit.status = ops.MaintenanceStatus("Removing Kubernetes resources")
        for manifest in self.collector.manifests.values():
            self._purge_manifest(manifest)
        ceph_pools = ", ".join(literals.REQUIRED_CEPH_POOLS)
        logger.warning(
            "Ceph pools %s won't be removed. If you want to clean up pools manually,"
            "use `juju run ceph-mon/leader delete-pool`",
            ceph_pools,
        )
        self.stored.config_hash = 0
        self.stored.deployed_releases = {}

    def _purge_manifest_by_name(self, name: str) -> None:
        """Purge resources created by this charm by manifest name."""
        self.unit.status = ops.MaintenanceStatus(f"Removing {name} resources")
        for manifest in self.collector.manifests.values():
            if manifest.name == name:
                self._purge_manifest(manifest)

    @status.on_error(ops.WaitingStatus("Manifest purge failed."))
    def _purge_manifest(self, manifest: Manifests) -> None:
        """Purge resources created by this charm by manifest."""
        manifest = cast(SafeManifest, manifest)
        manifest.purging = True
        manifest.delete_manifests(ignore_unauthorized=True, ignore_not_found=True)
        manifest.purging = False

    def _destroying(self, event: ops.EventBase) -> bool:
        """Check if the charm is being destroyed."""
        if cast(bool, self.stored.destroying):
            return True
        if isinstance(event, (ops.StopEvent, ops.RemoveEvent)):
            self.stored.destroying = True
            return True
        elif isinstance(event, ops.RelationBrokenEvent) and event.relation.name == "ceph-client":
            return True
        return False


if __name__ == "__main__":  # pragma: no cover
    ops.main.main(CephCsiCharm)
