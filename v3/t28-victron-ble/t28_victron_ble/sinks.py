"""Optional sink interface. Phase 1 implements only the local JSON map."""

from __future__ import annotations

from typing import Protocol


class ReadingSink(Protocol):
    """Destination for a complete latest.json device map.

    The BLE service writes the local map only. Optional MQTT is a sibling
    process (`t28-victron-publish`) that reads latest.json; it is not a sink
    inside this service and is unrelated to T28.V2.Server.
    """

    def publish(self, snapshot: dict) -> None:
        """Accept a schemaVersioned map keyed by logical device id."""


class NullSink:
    def publish(self, snapshot: dict) -> None:
        return None
