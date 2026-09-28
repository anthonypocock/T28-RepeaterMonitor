"""Atomic latest.json map and optional rate-limited commissioning history."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from . import SCHEMA_VERSION
from .models import DeviceSnapshot, format_utc
from .sinks import ReadingSink

DEFAULT_HISTORY_INTERVAL_SECONDS = 60
DEFAULT_HISTORY_MAX_AGE_SECONDS = 24 * 60 * 60
DEFAULT_HISTORY_MAX_BYTES = 256 * 1024


def atomic_write_json(path: Path, payload: dict, *, mode: int = 0o640) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".new")
    encoded = json.dumps(payload, indent=2, sort_keys=False) + "\n"
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    os.replace(temporary, path)
    os.chmod(path, mode)


class LatestStore:
    def __init__(
        self,
        path: Path,
        *,
        sink: Optional[ReadingSink] = None,
        mode: int = 0o640,
    ) -> None:
        self.path = Path(path)
        self.sink = sink
        self.mode = mode
        self._devices: dict[str, DeviceSnapshot] = {}

    def ensure_device(self, device_id: str, product: str = "unknown", address: str = "") -> DeviceSnapshot:
        snapshot = self._devices.get(device_id)
        if snapshot is None:
            snapshot = DeviceSnapshot(device_id=device_id, product=product, address=address)
            self._devices[device_id] = snapshot
        elif address and not snapshot.address:
            snapshot.address = address
        return snapshot

    def devices(self) -> dict[str, DeviceSnapshot]:
        return self._devices

    def replace_devices(self, snapshots: dict[str, DeviceSnapshot]) -> None:
        self._devices = snapshots

    def as_dict(self, *, updated_at: Optional[datetime] = None) -> dict:
        when = updated_at or datetime.now(timezone.utc)
        return {
            "schemaVersion": SCHEMA_VERSION,
            "updatedAtUtc": format_utc(when),
            "devices": {
                device_id: snapshot.as_dict()
                for device_id, snapshot in sorted(self._devices.items())
            },
        }

    def flush(self) -> dict:
        payload = self.as_dict()
        atomic_write_json(self.path, payload, mode=self.mode)
        if self.sink is not None:
            self.sink.publish(payload)
        return payload


class HistoryStore:
    """Rate-limited normalized readings. Never persist raw advertisements or keys."""

    def __init__(
        self,
        path: Path,
        *,
        interval_seconds: int = DEFAULT_HISTORY_INTERVAL_SECONDS,
        max_age_seconds: int = DEFAULT_HISTORY_MAX_AGE_SECONDS,
        max_bytes: int = DEFAULT_HISTORY_MAX_BYTES,
    ) -> None:
        self.path = Path(path)
        self.interval = timedelta(seconds=interval_seconds)
        self.max_age = timedelta(seconds=max_age_seconds)
        self.max_bytes = max_bytes
        self._last_written: dict[str, datetime] = {}

    def record(self, snapshot: DeviceSnapshot, now: datetime) -> None:
        if snapshot.status != "fresh" or snapshot.last_valid_at is None:
            return
        previous = self._last_written.get(snapshot.device_id)
        if previous is not None and now - previous < self.interval:
            return
        line = json.dumps(
            {
                "schemaVersion": SCHEMA_VERSION,
                "recordedAtUtc": format_utc(now),
                "device": snapshot.as_dict(),
            },
            separators=(",", ":"),
        )
        lowered = line.lower()
        if any(token in lowered for token in ('"advertisementkey"', '"aeskey"', '"encryptionkey"', '"rawadvertisement"')):
            raise RuntimeError("history record contained a forbidden secret field")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        self._last_written[snapshot.device_id] = now
        self.prune(now)

    def prune(self, now: datetime) -> None:
        if not self.path.is_file():
            return
        cutoff = now - self.max_age
        kept: list[str] = []
        for raw in self.path.read_text(encoding="utf-8").splitlines():
            try:
                item = json.loads(raw)
                stamp = datetime.fromisoformat(item["recordedAtUtc"].replace("Z", "+00:00"))
            except (ValueError, KeyError, TypeError, json.JSONDecodeError):
                continue
            if stamp >= cutoff:
                kept.append(raw)
        encoded = "".join(line + "\n" for line in kept)
        while len(encoded.encode("utf-8")) > self.max_bytes and kept:
            kept.pop(0)
            encoded = "".join(line + "\n" for line in kept)
        self.path.write_text(encoded, encoding="utf-8")
        os.chmod(self.path, 0o640)
