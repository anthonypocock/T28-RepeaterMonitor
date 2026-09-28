"""Last-heard MQTT payloads. Never include Instant Readout keys."""

from __future__ import annotations

from typing import Any, Optional

from .models import format_utc, utc_now
from .sanitize import sanitize_payload
from .topics import heard_topic

SOURCE = "t28-victron-ble"

HEARD_FIELDS = (
    "deviceId",
    "address",
    "product",
    "productId",
    "observedAtUtc",
    "status",
    "rssiDbm",
    "batteryVoltageV",
    "batteryCurrentA",
    "stateOfChargePercent",
    "timeToGoMinutes",
    "consumedAh",
    "alarmMask",
    "alarms",
)


def build_heard_payload(
    record: dict,
    *,
    hostname: str,
    device_id: str,
    published_at: Optional[str] = None,
) -> dict[str, Any]:
    cleaned = sanitize_payload(record)
    if not isinstance(cleaned, dict):
        cleaned = {}
    payload: dict[str, Any] = {}
    for field in HEARD_FIELDS:
        if field in cleaned:
            payload[field] = cleaned[field]
    payload["deviceId"] = str(cleaned.get("deviceId") or device_id)
    payload["hostname"] = hostname
    payload["receiverId"] = hostname
    payload["source"] = SOURCE
    payload["publishedAtUtc"] = published_at or format_utc(utc_now())
    if "timeToGoMinutes" not in payload:
        payload["timeToGoMinutes"] = None
    return sanitize_payload(payload)


def iter_heard_publications(
    latest: dict,
    *,
    hostname: str,
    topic_template: str,
    published_at: Optional[str] = None,
) -> list[tuple[str, dict[str, Any]]]:
    devices = latest.get("devices") if isinstance(latest, dict) else None
    if not isinstance(devices, dict):
        return []
    publications: list[tuple[str, dict[str, Any]]] = []
    for device_id, record in devices.items():
        if not isinstance(record, dict):
            continue
        topic = heard_topic(hostname, str(device_id), topic_template)
        publications.append(
            (topic, build_heard_payload(record, hostname=hostname, device_id=str(device_id), published_at=published_at))
        )
    return publications
