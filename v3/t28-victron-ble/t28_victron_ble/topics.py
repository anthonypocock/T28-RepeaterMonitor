"""MQTT topic contract for Victron fleet config and last-heard telemetry.

WireGuard overlay broker default is mqtt://192.0.2.2:1883. Operators may
override the URL; do not treat that host as the only allowed broker.
"""

from __future__ import annotations

import re

DEVICE_ID_TOPIC_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ._':-]{0,79}$")
DEVICE_ID_MQTT_WILDCARDS = frozenset("+#")

OVERLAY_BROKER_DEFAULT = "mqtt://192.0.2.2:1883"
OVERLAY_BROKER_HOST = "192.0.2.2"
OVERLAY_BROKER_PORT = 1883

CONFIG_TOPIC = "demo/power/config/v1"
CONFIG_TOPIC_FILTER = "demo/power/config/#"
HEARD_TOPIC_TEMPLATE = "demo/power/heard/{hostname}/{deviceId}"
HEARD_PREFIX = "demo/power/heard/"
HEARD_WILDCARD = "demo/power/heard/#"

HUB_USER = "t28-config"
RECEIVER_USER = "t28-publish"


class TopicError(ValueError):
    """Topic template or device id is not safe to publish."""


def expand_topic(template: str, hostname: str, device_id: str | None = None) -> str:
    """Expand {hostname} and optional {deviceId}. Append deviceId when missing."""
    host = str(hostname or "").strip()
    if not host:
        raise TopicError("hostname is required")
    text = str(template or HEARD_TOPIC_TEMPLATE).replace("{hostname}", host)
    if device_id is None:
        return text
    if (not DEVICE_ID_TOPIC_RE.fullmatch(device_id)) or any(char in DEVICE_ID_MQTT_WILDCARDS for char in device_id):
        raise TopicError("device id is not safe for an MQTT topic")
    if "{deviceId}" in text:
        return text.replace("{deviceId}", device_id)
    return text.rstrip("/") + "/" + device_id


def heard_topic(hostname: str, device_id: str, template: str = HEARD_TOPIC_TEMPLATE) -> str:
    return expand_topic(template, hostname, device_id)


def parse_heard_topic(topic: str) -> tuple[str, str] | None:
    """Return (hostname, deviceId) for demo/power/heard/{hostname}/{deviceId}."""
    text = str(topic or "").strip()
    if not text.startswith(HEARD_PREFIX):
        return None
    rest = text[len(HEARD_PREFIX) :]
    parts = [part for part in rest.split("/") if part]
    if len(parts) < 2:
        return None
    hostname, device_id = parts[0], parts[1]
    if not hostname or not DEVICE_ID_TOPIC_RE.fullmatch(device_id) or any(char in DEVICE_ID_MQTT_WILDCARDS for char in device_id):
        return None
    return hostname, device_id
