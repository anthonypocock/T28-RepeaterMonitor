"""Apply decoded advertisements to the latest.json map.

A Bluetooth/decoder failure here only updates sidecar status. It never touches
RF/audio units or asks the caller to restart them.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from .config import ServiceConfig
from .decoder import (
    DecodeError,
    KeyMismatchError,
    UnknownRecordError,
    decode_battery_monitor,
    parse_header,
)
from .hci import CHARGER_PRODUCT_ID
from .models import DeviceSnapshot
from .store import HistoryStore, LatestStore

Clock = Callable[[], datetime]


class AdvertisementObservation:
    __slots__ = ("address", "payload", "rssi", "observed_at")

    def __init__(self, address: str, payload: bytes, rssi: Optional[int], observed_at: datetime) -> None:
        self.address = address.lower()
        self.payload = payload
        self.rssi = rssi
        self.observed_at = observed_at


class VictronEngine:
    def __init__(
        self,
        config: ServiceConfig,
        store: LatestStore,
        *,
        history: Optional[HistoryStore] = None,
        clock: Optional[Clock] = None,
    ) -> None:
        self.config = config
        self.store = store
        self.history = history
        self.clock = clock
        for device in config.devices:
            snapshot = store.ensure_device(device.device_id, device.product or "unknown", device.address)
            if device.product:
                snapshot.product = device.product
            if device.address:
                snapshot.address = device.address
        store.flush()

    def _now(self) -> datetime:
        return self.clock() if self.clock else datetime.now(timezone.utc)

    def handle_advertisement(self, observation: AdvertisementObservation) -> DeviceSnapshot | None:
        device = self.config.device_by_address(observation.address)
        if device is None:
            return None
        snapshot = self.store.ensure_device(device.device_id, device.product or "unknown", device.address)
        try:
            header = parse_header(observation.payload)
        except DecodeError:
            snapshot.health.advertisements_seen += 1
            snapshot.health.malformed += 1
            self.store.flush()
            return snapshot
        if header.model_id == CHARGER_PRODUCT_ID:
            return None
        if device.product_id is not None and header.model_id != device.product_id:
            return None
        snapshot.health.advertisements_seen += 1
        snapshot.observed_at = observation.observed_at
        snapshot.rssi_dbm = observation.rssi
        key = self.config.keys[device.device_id]
        try:
            reading = decode_battery_monitor(observation.payload, key)
        except KeyMismatchError:
            snapshot.status = "keyMismatch"
            snapshot.health.key_mismatch += 1
            self.store.flush()
            return snapshot
        except UnknownRecordError:
            snapshot.health.unknown_records += 1
            self.store.flush()
            return snapshot
        except DecodeError:
            snapshot.health.malformed += 1
            self.store.flush()
            return snapshot
        except Exception:
            snapshot.health.decoder_errors += 1
            self.store.flush()
            return snapshot
        if snapshot.last_nonce is not None and reading.nonce == snapshot.last_nonce:
            snapshot.health.duplicate_nonces += 1
            self.store.flush()
            return snapshot
        snapshot.last_nonce = reading.nonce
        snapshot.product = device.product or reading.product
        snapshot.product_id = reading.product_id
        snapshot.record_type = f"0x{reading.record_type:02X}"
        snapshot.status = "fresh"
        snapshot.last_valid_at = observation.observed_at
        snapshot.battery_voltage_v = reading.battery_voltage_v
        snapshot.battery_current_a = reading.battery_current_a
        snapshot.time_to_go_minutes = reading.time_to_go_minutes
        snapshot.state_of_charge_percent = reading.state_of_charge_percent
        snapshot.consumed_ah = reading.consumed_ah
        snapshot.alarm_mask = reading.alarm_mask
        snapshot.alarms = dict(reading.alarms)
        snapshot.health.decoded_ok += 1
        if self.history is not None:
            self.history.record(snapshot, observation.observed_at)
        self.store.flush()
        return snapshot

    def mark_device_lost(self, address: str, when: Optional[datetime] = None) -> None:
        device = self.config.device_by_address(address)
        if device is None:
            return
        snapshot = self.store.ensure_device(device.device_id)
        snapshot.observed_at = when or self._now()
        if snapshot.status == "fresh":
            snapshot.status = "stale"
            snapshot.health.stale_transitions += 1
        elif snapshot.last_valid_at is None and snapshot.status != "keyMismatch":
            snapshot.status = "unavailable"
        self.store.flush()

    def refresh_stale(self, now: Optional[datetime] = None) -> None:
        current = now or self._now()
        limit = timedelta(seconds=self.config.stale_after_seconds)
        changed = False
        for snapshot in list(self.store.devices().values()):
            if snapshot.last_valid_at is None:
                if snapshot.status not in {"keyMismatch", "unavailable"}:
                    snapshot.status = "unavailable"
                    changed = True
                continue
            if current - snapshot.last_valid_at > limit and snapshot.status == "fresh":
                snapshot.status = "stale"
                snapshot.health.stale_transitions += 1
                changed = True
        if changed:
            self.store.flush()

