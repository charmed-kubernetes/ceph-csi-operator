# ceph-csi

## Description

This is a subordinate charm of [kubernetes-master][1], deployed as part of the
[Charmed Kubernetes][2] and enables Kubernetes cluster to use Ceph as a
storage. Currently supported storage options are `ceph-xfs` and `ceph-ext4`
with `cephfs` being work in progress.

**__Note:__** This charm creates various Kubernetes resources, including pods.
Therefore it requires `kubernetes-master` to run in privileged mode (config
option `allow-privileged=true`) 

## Usage

Since this charm is not published in charmstore yet, it needs to be built from
source.

    $ charmcraft pack  # This will produce `ceph-csi.charm`

As this charm has no standalone functionality, we'll need `Kubernetes` and
`Ceph` cluster first.

    $ juju deploy charmed-kubernetes
    $ juju config kubernetes-master allow-privileged=true
    $ juju deploy -n 3 ceph-mon
    $ juju deploy -n 3 ceph-osd
    $ juju add-relation ceph-osd ceph-mon
    
Once the deployment settled, We can add `ceph-csi`

    $ juju deploy ./ceph-csi.charm
    $ juju add-relation ceph-csi kubernetes-master
    $ juju add-relation ceph-csi ceph-mon
    



## Developing

Create and activate a virtualenv with the development requirements:

    virtualenv -p python3 venv
    source venv/bin/activate
    pip install -r requirements-dev.txt

## Testing

The Python operator framework includes a very nice harness for testing
operator behaviour without full deployment. Just `unit` tox target:

    $ tox -e unit

[1]: https://charmhub.io/containers-kubernetes-master
[2]: https://charmhub.io/charmed-kubernetes