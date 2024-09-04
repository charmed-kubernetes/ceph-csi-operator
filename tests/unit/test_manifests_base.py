from unittest.mock import MagicMock

import pytest

from manifests_base import ConfigureLivenessPrometheus


@pytest.fixture
def mock_manifests():
    return MagicMock()


@pytest.fixture
def mock_obj():
    return MagicMock()


def test_liveness_prometheus_on_deployment_swap(mock_manifests, mock_obj, request):
    config = "sample_config"
    mock_manifests.config = {"metrics-port-sample_config": 8080}
    mock_obj.kind = "Deployment"
    mock_obj.metadata.name = name = request.node.name
    mock_obj.spec.template.spec.containers = [MagicMock()]
    mock_obj.spec.template.spec.containers[0].name = "liveness-prometheus"
    mock_obj.spec.template.spec.containers[0].args = ["--metricsport=9090"]
    liveness_prometheus = ConfigureLivenessPrometheus(mock_manifests, "Deployment", name, config)
    liveness_prometheus(mock_obj)
    assert mock_obj.spec.template.spec.containers[0].args == ["--metricsport=8080"]


def test_liveness_prometheus_on_deployment_off(mock_manifests, mock_obj, request):
    config = "sample_config"
    mock_manifests.config = {"metrics-port-sample_config": -1}
    mock_obj.kind = "Deployment"
    mock_obj.metadata.name = name = request.node.name
    mock_obj.spec.template.spec.containers = [MagicMock()]
    mock_obj.spec.template.spec.containers[0].name = "liveness-prometheus"
    mock_obj.spec.template.spec.containers[0].args = ["--metricsport=9090"]
    liveness_prometheus = ConfigureLivenessPrometheus(mock_manifests, "Deployment", name, config)
    liveness_prometheus(mock_obj)
    assert not mock_obj.spec.template.spec.containers


def test_liveness_prometheus_on_service_portmap_swap(mock_manifests, mock_obj, request):
    config = "sample_config"
    mock_manifests.config = {"metrics-port-sample_config": 8080}
    mock_obj.kind = "Service"
    mock_obj.metadata.name = name = request.node.name
    mock_obj.spec.ports = [MagicMock()]
    mock_obj.spec.ports[0].name = "http-metrics"
    mock_obj.spec.ports[0].targetPort = 9090
    liveness_prometheus = ConfigureLivenessPrometheus(mock_manifests, "Service", name, config)
    liveness_prometheus(mock_obj)
    assert mock_obj.spec.ports[0].targetPort == 8080


def test_liveness_prometheus_on_service_portmap_off(mock_manifests, mock_obj, request):
    config = "sample_config"
    mock_manifests.config = {"metrics-port-sample_config": -1}
    mock_obj.kind = "Service"
    mock_obj.metadata.name = name = request.node.name
    mock_obj.spec.ports = [MagicMock()]
    mock_obj.spec.ports[0].name = "http-metrics"
    mock_obj.spec.ports[0].targetPort = 9090
    liveness_prometheus = ConfigureLivenessPrometheus(mock_manifests, "Service", name, config)
    liveness_prometheus(mock_obj)
    assert mock_obj.spec.ports[0].targetPort == 9090
