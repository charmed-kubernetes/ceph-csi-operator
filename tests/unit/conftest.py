# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import unittest.mock as mock

import pytest


@pytest.fixture(autouse=True)
def lk_client():
    with mock.patch("ops.manifests.manifest.Client", autospec=True) as mock_lightkube:
        yield mock_lightkube.return_value


# Autouse to prevent calling out to the k8s API via lightkube client in charm
@pytest.fixture(autouse=True)
def lk_charm_client():
    with mock.patch("charm.Client", autospec=True) as mock_lightkube:
        yield mock_lightkube.return_value
