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
    

## Warning: Removal

When `ceph-csi` charm is removed, it will not clean up Ceph pools that were
created when relation with `ceph-mon:client` was joined. Interface of
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
    
This charm contains also functional tests which fully deploy `ceph-csi` charm 
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

[1]: https://charmhub.io/containers-kubernetes-master
[2]: https://charmhub.io/charmed-kubernetes