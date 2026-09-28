"""Local data-contract types for Phase 1 (no network sink)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from . import SCHEMA_VERSION

STATUSES = ("fresh", "stale", "keyMismatch", "unavailable")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def format_utc(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@dataclass
class HealthCounters:
    advertisements_seen: int = 0
    decoded_ok: int = 0
    duplicate_nonces: int = 0
    key_mismatch: int = 0
    malformed: int = 0
    unknown_records: int = 0
    decoder_errors: int = 0
    stale_transitions: int = 0

    def as_dict(self) -> dict:
        return {
            "advertisementsSeen": self.advertisements_seen,
            "decodedOk": self.decoded_ok,
            "duplicateNonces": self.duplicate_nonces,
            "keyMismatch": self.key_mismatch,
            "malformed": self.malformed,
            "unknownRecords": self.unknown_records,
            "decoderErrors": self.decoder_errors,
            "staleTransitions": self.stale_transitions,
        }


@dataclass
class DeviceSnapshot:
    device_id: str
    product: str = "unknown"
    address: str = ""
    product_id: Optional[int] = None
    record_type: str = "0x02"
    status: str = "unavailable"
    observed_at: Optional[datetime] = None
    last_valid_at: Optional[datetime] = None
    battery_voltage_v: Optional[float] = None
    battery_current_a: Optional[float] = None
    time_to_go_minutes: Optional[int] = None
    state_of_charge_percent: Optional[float] = None
    consumed_ah: Optional[float] = None
    alarm_mask: Optional[int] = None
    alarms: dict[str, bool] = field(default_factory=dict)
    rssi_dbm: Optional[int] = None
    last_nonce: Optional[int] = None
    health: HealthCounters = field(default_factory=HealthCounters)

    def as_dict(self) -> dict:
        return {
            "schemaVersion": SCHEMA_VERSION,
            "deviceId": self.device_id,
            "address": self.address or None,
            "product": self.product,
            "productId": None if self.product_id is None else f"0x{self.product_id:04X}",
            "recordType": self.record_type,
            "observedAtUtc": format_utc(self.observed_at),
            "lastValidAtUtc": format_utc(self.last_valid_at),
            "status": self.status,
            "batteryVoltageV": self.battery_voltage_v,
            "batteryCurrentA": self.battery_current_a,
            "timeToGoMinutes": self.time_to_go_minutes,
            "stateOfChargePercent": self.state_of_charge_percent,
            "consumedAh": self.consumed_ah,
            "alarmMask": self.alarm_mask,
            "alarms": dict(self.alarms),
            "rssiDbm": self.rssi_dbm,
            "health": self.health.as_dict(),
        }
