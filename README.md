# ceph-csi

## Description

This is a subordinate charm of [kubernetes-control-plane][1], deployed as part of
[Charmed Kubernetes][2] and enables a Kubernetes cluster to use Ceph as a
storage backend. Currently supported storage options are `ceph-xfs` and `ceph-ext4`
with `cephfs` being work in progress.

**__Note:__** This charm creates various Kubernetes resources, including pods.
Therefore it requires `kubernetes-control-plane` to run in privileged mode (config
option `allow-privileged=true`)

## Usage

As this charm has no standalone functionality, we'll need `Kubernetes` and
`Ceph` cluster first.

    $ juju deploy charmed-kubernetes
    $ juju config kubernetes-control-plane allow-privileged=true
    $ juju deploy -n 3 ceph-mon
    $ juju deploy -n 3 ceph-osd
    $ juju add-relation ceph-osd ceph-mon

Once the deployment settled, We can add `ceph-csi`

    $ juju deploy ceph-csi
    $ juju integrate ceph-csi:kubernetes kubernetes-control-plane:juju-info
    $ juju integrate ceph-csi:ceph-client ceph-mon:client


## Warning: Removal

When the `ceph-csi` charm is removed, it will not clean up Ceph pools that were
created when the relation with `ceph-mon:client` was joined. The interface of
`ceph-mon:client` does not seem to offer a "removal" functionality. If you
wish to remove ceph pools, use `delete-pool` action of `ceph-mon unit`.

## Developing

Create and activate a virtualenv with the development requirements:

    virtualenv -p python3 venv
    source venv/bin/activate
    pip install -r requirements-dev.txt

## Testing

The Python operator framework includes a very nice harness for testing
operator behaviour without full deployment. To execute unit tests, run:

    $ tox -e unit

This charm contains functional tests which fully deploy the `ceph-csi` charm
along with `kubernetes` and `ceph` cluster to test its functionality in a real
environment. There are a few ways to run functional tests

    $ tox -e func                          # Deploys new juju model and runs tests
    $ tox -e func -- --model <model_name>  # Runs tests against existing model
    $ tox -e func -- --keep-models         # Does not tear down model after tests are done (usefull for debuging failing tests )


**__NOTE:__** If the environment which runs functional tests is behind a http
proxy, you must export `TEST_HTTPS_PROXY` environment variable. Otherwise
Kubernetes might have problem fetching docker images. Example:

    $ export TEST_HTTPS_PROXY=http://10.0.0.1:3128
    $ tox -e func

[1]: https://charmhub.io/kubernetes-control-plane
[2]: https://charmhub.io/charmed-kubernetes
