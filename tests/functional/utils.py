# Copyright 2021 Martin Kalcok
# See LICENSE file for licensing details.
"""Various helper functions used by functional tests."""

import logging
import pprint
from contextlib import asynccontextmanager
from typing import Any, Optional

import pytest
import yaml
from jinja2 import Environment, FileSystemLoader
from juju.application import Application
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
    timeout: int = 120,
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


def wait_for_pvc_resize(
    core_api: client.CoreV1Api,
    name: str,
    namespace: str,
    timeout: int = 120,
    at_least: str = "1Gi",
) -> None:
    """
    Wait for kubernetes pvc to reach desired state.

    If the state is not reached within specified timeout, pytest.fail is executed.

    :param core_api: instance of CoreV1 kubernetes api.
    :param name: name of the kubernetes pvc.
    :param namespace: namespace in which the pvc is running
    :param timeout: Maximum seconds to wait for pvc to reach at_least size
    :param at_least: Expected minimum size of the pvc (in bytes).
    :return: None
    """
    k8s_watch = watch.Watch()

    logger.info("Waiting for pvc '%s' to reach size '%s'", name, at_least)
    for event in k8s_watch.stream(
        func=core_api.list_namespaced_persistent_volume_claim,
        namespace=namespace,
        timeout_seconds=timeout,
    ):
        resource = event["object"]
        if resource.metadata.name != name:
            continue
        pvc_size = resource.status.capacity.get("storage", 0)
        logger.debug("%s size: %s", name, pvc_size)
        if pvc_size == at_least:
            break
    else:
        logger.info(f"PVC failed to resize within allotted timeout: '{timeout}s'")
        events: EventsV1EventList = core_api.list_namespaced_event(
            namespace, field_selector=f"involvedObject.name={name}"
        )
        for event in events.items:
            event_interest = ", ".join(
                [event.type, event.reason, pprint.pformat(event.source), event.message]
            )
            logger.info(event_interest)
        pytest.fail(
            "Timeout after {}s while waiting for {} pvc to reach {} bytes".format(
                timeout, name, at_least
            )
        )


def units_have_status(app: Application, status: str, message_substring: Optional[str] = None):
    """Return a function that checks whether all units of an application have a given status.

    :param app: The application to check.
    :param status: The expected status.
    :param message_substring: An optional substring that should be present in the unit's message.
    :return: A function that checks the status of the units.
    """

    def _check() -> bool:
        check_msg = message_substring is not None
        unit_status, unit_message = [], []
        for unit in app.units:
            unit_status.append(unit.workload_status)
            unit_message.append(unit.workload_status_message if check_msg else "")
        success = all(
            s == status and message_substring in m for s, m in zip(unit_status, unit_message)
        )
        if not success:
            current = ""
            for u, s, m in zip(app.units, unit_status, unit_message):
                extra = f", message='{m}'" if check_msg else ""
                current += f"\n - Unit {u.name}: status='{s}'{extra}"
            logger.info(
                "Waiting for all units of app '%s' to have status '%s'%s. " "Current statuses: %s",
                app.name,
                status,
                f" and message containing '{message_substring}'" if check_msg else "",
                current,
            )
        return success

    return _check


@asynccontextmanager
async def set_test_config(app: Application, config: dict):
    """Restore application config to previous state."""
    current_config = await app.get_config()
    await app.set_config(config)
    try:
        yield
    finally:
        await app.set_config(current_config)
