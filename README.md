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

The storage nodes above will have two 32GB devices for storage and 8GB for journalling.
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
tox -e func                          # Deploys new juju model and runs tests
tox -e func -- --model <model_name>  # Runs tests against existing model
tox -e func -- --keep-models         # Does not tear down model after tests are done (usefull for debuging failing tests )
```

**__NOTE:__** If the environment which runs functional tests is behind a http
proxy, you must export `TEST_HTTPS_PROXY` environment variable. Otherwise
Kubernetes might have problem fetching docker images. Example:

```bash
export TEST_HTTPS_PROXY=http://10.0.0.1:3128
tox -e func
```

[1]: https://charmhub.io/kubernetes-control-plane
[2]: https://charmhub.io/charmed-kubernetes
