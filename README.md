# ceph-csi

## Description

This is a subordinate charm of [kubernetes-control-plane][1], deployed as part of
[Charmed Kubernetes][2] and enables a Kubernetes cluster to use Ceph as a
storage backend. Currently supported storage options are `ceph-xfs`, `ceph-ext4`,
and `cephfs`.

**__Note:__** This charm creates various Kubernetes resources, including pods.
Therefore it requires `kubernetes-control-plane` to run in privileged mode (config
option `allow-privileged=true`)

## Usage

As this charm has no standalone functionality, we'll need `Kubernetes` and
`Ceph` clusters first.

Deploy **Charmed Kubernetes** configured to allow privileged containers as follows:

```bash
juju deploy charmed-kubernetes
juju config kubernetes-control-plane allow-privileged=true
```

Deploy a Ceph cluster consisting of three monitor and three storage nodes:

```bash
 juju deploy -n 3 ceph-mon
 juju deploy -n 3 ceph-osd --storage osd-devices=32G,2 --storage osd-journals=8G,1
 juju integrate ceph-osd:mon ceph-mon:osd
```

The storage nodes above will have two 32GB devices for storage and 8GB for journaling.
As we have asked for 3 machines, this means a total of 192GB of storage and 24GB of
journal space. The storage comes from whatever the default storage class is for the
cloud (e.g., on AWS this will be EBS volumes).

Once the deployment has settled, We can add `ceph-csi`:

```bash
juju deploy ceph-csi
juju integrate ceph-csi:kubernetes kubernetes-control-plane:juju-info
juju integrate ceph-csi:ceph-client ceph-mon:client
```

If desired, add support for the Ceph distributed filesystem (CephFS) as follows:

```bash
juju deploy ceph-fs
juju integrate ceph-fs:ceph-mds ceph-mon:mds
juju config ceph-csi cephfs-enable=True
```

## Ceph CSI releases and upgrades

The default release is **v3.18.2** for both RBD and CephFS. The charm also bundles
v3.16.3 and v3.17.2, retaining all previously bundled releases. Bundling a release
does not imply support on every Kubernetes version: upstream lists v3.18.2 and
[v3.17.2][4] as tested with Kubernetes 1.34–1.36, whereas v3.16.3 is tested with
1.32–1.34. The [v3.18.2 support matrix][3] requires Ceph Pacific (16.2.0) or newer
for ordinary RBD and CephFS provisioning. Optional features have additional
requirements; for example, CephFS volume group snapshots require Ceph Squid.

**Before refreshing an existing charm, explicitly pin its currently deployed
release** with `juju config ceph-csi release=<current-release>`. The previous
charm schema default was v3.9.0, even though its newest bundle was v3.16.0.
Ceph CSI upstream supports upgrading [one minor release at a time][5]
(for example v3.17 → v3.18, "we do not recommend any direct upgrades to 3.18
except from 3.17"), so the charm reads the release running in the provisioner
Deployments and nodeplugin DaemonSets and refuses to apply a different release
unless it is the same minor or the next one: the unit goes blocked with the
release to pin, and neither the config hook nor the `sync-resources` action
applies anything. While a previous release is still rolling out the unit waits.
Step through the minors with `juju config ceph-csi release=<next-release>`,
waiting for both the provisioner and nodeplugin workloads to become ready and
checking existing PVC I/O each time. For a deployment on v3.16, the final steps
are v3.16.3 → v3.17.2 → v3.18.2. Workloads whose release cannot be read from the
driver image tag block the upgrade until they are removed or restored.

Before using v3.17/v3.18 standard images on amd64, qualify the nodes against the
[upstream CPU requirements][6]: Rocky Linux 10 images no longer run on older
x86-64-v2 CPUs. Upstream offers separately tagged Rocky Linux 9 alternatives,
but this charm bundles the standard release manifests. Arm64 remains experimental
upstream. Image availability and architecture support must be checked in the
configured image registry before upgrading.

The charm retains upstream CSI sidecars, RBAC and snapshot API use; it does not
install the cluster-wide snapshot controller or snapshot CRDs. Administrators
must manage those prerequisites and their compatibility with the bundled
snapshotter (v8.6.0 for v3.18.2). The v3.18.2 CephFS manifest enables
`podInfoOnMount` and adds a controller container. Encrypted CephFS volumes also
change their fscrypt RADOS lock namespace; review the [release notes][7] before
upgrading. CephFS FUSE mounts need the controlled restart procedure in the
[upgrade guide][5] to avoid disrupting application mounts.

## Verify things are working

Check the **Charmed Kubernetes** cluster to verify Ceph cluster resources are
available. Running:

```bash
juju ssh kubernetes-control-plane/leader -- kubectl get sc,po --namespace default
```

... should return output similar to:

```no-highlight
NAME                                             PROVISIONER           RECLAIMPOLICY   VOLUMEBINDINGMODE   ALLOWVOLUMEEXPANSION   AGE
storageclass.storage.k8s.io/ceph-ext4            rbd.csi.ceph.com      Delete          Immediate           true                   142m
storageclass.storage.k8s.io/ceph-xfs (default)   rbd.csi.ceph.com      Delete          Immediate           true                   142m
storageclass.storage.k8s.io/cephfs               cephfs.csi.ceph.com   Delete          Immediate           true                   127m

NAME                                               READY   STATUS    RESTARTS   AGE
pod/csi-cephfsplugin-4gs22                         3/3     Running   0          127m
pod/csi-cephfsplugin-ljfw9                         3/3     Running   0          127m
pod/csi-cephfsplugin-mlbdx                         3/3     Running   0          127m
pod/csi-cephfsplugin-provisioner-9f479bcc4-48v8f   5/5     Running   0          127m
pod/csi-cephfsplugin-provisioner-9f479bcc4-92bt6   5/5     Running   0          127m
pod/csi-cephfsplugin-provisioner-9f479bcc4-hbb82   5/5     Running   0          127m
pod/csi-cephfsplugin-wlp2w                         3/3     Running   0          127m
pod/csi-cephfsplugin-xwdb2                         3/3     Running   0          127m
pod/csi-rbdplugin-b8nk8                            3/3     Running   0          142m
pod/csi-rbdplugin-bvqwn                            3/3     Running   0          142m
pod/csi-rbdplugin-provisioner-85dc49c6c-9rckg      7/7     Running   0          142m
pod/csi-rbdplugin-provisioner-85dc49c6c-f6h6k      7/7     Running   0          142m
pod/csi-rbdplugin-provisioner-85dc49c6c-n47fx      7/7     Running   0          142m
pod/csi-rbdplugin-vm25h                            3/3     Running   0          142m
```
## Warning: Removal

When the `ceph-csi` charm is removed, it will not clean up Ceph pools that were
created when the relation with `ceph-mon:client` was joined. The interface of
`ceph-mon:client` does not seem to offer "removal" functionality. If you wish to
remove ceph pools, use the `delete-pool` action on a `ceph-mon` unit.

## Developing

Create and activate a virtualenv with the development requirements:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements-dev.txt
```

## Testing

The Python operator framework includes a very nice harness for testing
operator behaviour without full deployment. To execute unit tests, run:

```bash
tox -e unit
```

This charm contains functional tests which fully deploy the `ceph-csi` charm
along with `kubernetes` and `ceph` cluster to test its functionality in a real
environment. There are a few ways to run functional tests

```bash
tox -e integration                          # Deploys new juju model and runs tests
tox -e integration -- --model <model_name>  # Runs tests against existing model
tox -e integration -- --keep-models         # Does not tear down model after tests are done (useful for debugging failing tests )
```

**__NOTE:__** If the environment which runs integration tests is behind a http
proxy, you must export `TEST_HTTPS_PROXY` environment variable. Otherwise
Kubernetes might have problem fetching docker images. Example:

```bash
export TEST_HTTPS_PROXY=http://10.0.0.1:3128
tox -e integration
```

[1]: https://charmhub.io/kubernetes-control-plane
[2]: https://charmhub.io/charmed-kubernetes
[3]: https://github.com/ceph/ceph-csi/blob/v3.18.2/README.md#support-matrix
[4]: https://github.com/ceph/ceph-csi/blob/v3.17.2/README.md#known-to-work-co-platforms
[5]: https://github.com/ceph/ceph-csi/blob/v3.18.2/docs/ceph-csi-upgrade.md
[6]: https://github.com/ceph/ceph-csi/blob/v3.18.2/README.md#alternative-images-for-older-cpus
[7]: https://github.com/ceph/ceph-csi/releases/tag/v3.18.2
