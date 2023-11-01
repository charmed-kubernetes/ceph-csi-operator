# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
"""Various helper functions used by functional tests."""

import logging
import pprint
from typing import Any

import pytest
import yaml
from jinja2 import Environment, FileSystemLoader
from kubernetes import client, watch
from kubernetes.client.models import EventsV1EventList

logger = logging.getLogger(__name__)


def render_j2_template(template_dir: str, template: str, **context: Any) -> dict:
    """Render jinja2 template with provided context.

    :param template_dir: Directory in which the template file is located.
    :param template: Template file name
    :param context: variables and their values used in the template.
    :return: dict parsed from rendered jinja2 template
    """
    env = Environment(loader=FileSystemLoader(template_dir))
    raw_data = env.get_template(template).render(**context)
    return yaml.safe_load(raw_data)


def wait_for_pod(
    core_api: client.CoreV1Api,
    name: str,
    namespace: str,
    timeout: int = 60,
    target_state: str = "Running",
) -> None:
    """
    Wait for kubernetes pod to reach desired state.

    If the state is not reached within specified timeout, pytest.fail is executed.

    :param core_api: instance of CoreV1 kubernetes api.
    :param name: name of the kubernetes pod.
    :param namespace: namespace in which the pod is running
    :param timeout: Maximum seconds to wait for pod to reach target_state
    :param target_state: Expected pod status. (e.g.: Running or Succeeded)
    :return: None
    """
    k8s_watch = watch.Watch()

    logger.info("Waiting for pod '%s' to reach state '%s'", name, target_state)
    for event in k8s_watch.stream(
        func=core_api.list_namespaced_pod, namespace=namespace, timeout_seconds=timeout
    ):
        resource = event["object"]
        if resource.metadata.name != name:
            continue
        pod_state = resource.status.phase
        logger.debug("%s state: %s", name, pod_state)
        if pod_state == target_state:
            break
    else:
        logger.info(f"Pod failed to start within allotted timeout: '{timeout}s'")
        events: EventsV1EventList = core_api.list_namespaced_event(
            namespace, field_selector=f"involvedObject.name={name}"
        )
        for event in events.items:
            event_interest = ", ".join(
                [event.type, event.reason, pprint.pformat(event.source), event.message]
            )
            logger.info(event_interest)
        pytest.fail(
            "Timeout after {}s while waiting for {} pod to reach {} status".format(
                timeout, name, target_state
            )
        )
