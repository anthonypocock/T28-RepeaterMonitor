"""Parse and apply retained fleet config from demo/power/config/v1.

The hub is the source of truth. Receivers write devices.json + keys.json and
may bounce only t28-victron-ble.service. Never log key material.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

from . import SCHEMA_VERSION
from .config import ConfigError, DeviceConfig, is_valid_device_id, normalize_mac, parse_min_interval_seconds, parse_product_id
from .decoder import DecodeError, parse_hex_key
from .models import format_utc, utc_now
from .store import atomic_write_json

LOGGER = logging.getLogger("t28-victron-fleet")

DEFAULT_DEVICES = Path("/etc/t28-victron/devices.json")
DEFAULT_KEYS = Path("/etc/t28-victron/keys.json")
DEFAULT_FLEET_STATE = Path("/etc/t28-victron/fleet-state.json")
SIDECAR_UNIT = "t28-victron-ble.service"
FORBIDDEN_UNITS = ("bluetooth.service", "bluetoothd", "t28-v2-receiver", "t28-victron-publish")

Systemctl = Callable[[list[str]], Optional[int]]


@dataclass(frozen=True)
class FleetDevice:
    device_id: str
    address: str
    product: str = ""
    product_id: Optional[int] = None
    min_interval_seconds: int = 0

    def as_devices_entry(self) -> dict[str, Any]:
        item: dict[str, Any] = {"id": self.device_id, "address": self.address}
        if self.product:
            item["product"] = self.product
        if self.product_id is not None:
            item["productId"] = f"0x{self.product_id:04X}"
        item["minIntervalSeconds"] = int(self.min_interval_seconds or 0)
        return item


@dataclass(frozen=True)
class FleetConfig:
    schema_version: int
    revision: int
    updated_at_utc: str
    devices: tuple[FleetDevice, ...]
    keys_hex: dict[str, str]
    stale_after_seconds: int = 90

    def devices_payload(self) -> dict[str, Any]:
        return {
            "schemaVersion": SCHEMA_VERSION,
            "revision": self.revision,
            "updatedAtUtc": self.updated_at_utc,
            "staleAfterSeconds": self.stale_after_seconds,
            "devices": [item.as_devices_entry() for item in self.devices],
        }

    def keys_payload(self) -> dict[str, Any]:
        return {"schemaVersion": SCHEMA_VERSION, "keys": dict(self.keys_hex)}

    def state_payload(self, *, applied_at: Optional[str] = None) -> dict[str, Any]:
        return {
            "schemaVersion": SCHEMA_VERSION,
            "source": "mqtt-fleet",
            "topic": "demo/power/config/v1",
            "revision": self.revision,
            "updatedAtUtc": self.updated_at_utc,
            "appliedAtUtc": applied_at or format_utc(utc_now()),
            "deviceCount": len(self.devices),
            "keyCount": len(self.keys_hex),
        }


@dataclass(frozen=True)
class ApplyResult:
    changed: bool
    revision: int
    device_count: int
    ble_action: str


def parse_fleet_config(payload: Any) -> FleetConfig:
    """Validate schemaVersion 1 fleet map. Extra fields are ignored."""
    if not isinstance(payload, dict):
        raise ConfigError("fleet config must be a JSON object")
    if "schemaVersion" not in payload:
        raise ConfigError("missing schemaVersion")
    try:
        schema = int(payload["schemaVersion"])
    except (TypeError, ValueError) as exc:
        raise ConfigError("schemaVersion must be 1") from exc
    if schema != SCHEMA_VERSION:
        raise ConfigError("unsupported schemaVersion")
    if "revision" not in payload:
        raise ConfigError("missing revision")
    revision = payload["revision"]
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
        raise ConfigError("revision must be a positive integer")
    updated = str(payload.get("updatedAtUtc") or "").strip()
    if not updated:
        updated = format_utc(utc_now()) or ""
    raw_devices = payload.get("devices")
    if not isinstance(raw_devices, list) or not raw_devices:
        raise ConfigError("devices must list at least one device")
    devices: list[FleetDevice] = []
    seen_ids: set[str] = set()
    seen_addresses: set[str] = set()
    for item in raw_devices:
        if not isinstance(item, dict):
            raise ConfigError("each device must be an object")
        device_id = str(item.get("id", "")).strip()
        if not is_valid_device_id(device_id):
            raise ConfigError("device id must be a stable logical name")
        try:
            address = normalize_mac(item.get("address", ""))
        except ConfigError as exc:
            raise ConfigError(f"device {device_id} address must be a Bluetooth MAC") from exc
        if device_id in seen_ids:
            raise ConfigError(f"duplicate device id: {device_id}")
        if address in seen_addresses:
            raise ConfigError(f"duplicate device address: {address}")
        seen_ids.add(device_id)
        seen_addresses.add(address)
        product = str(item.get("product", "")).strip()
        product_id = parse_product_id(item.get("productId"))
        min_interval = parse_min_interval_seconds(item.get("minIntervalSeconds"))
        devices.append(
            FleetDevice(
                device_id=device_id,
                address=address,
                product=product,
                product_id=product_id,
                min_interval_seconds=min_interval,
            )
        )
    raw_keys = payload.get("keys")
    if not isinstance(raw_keys, dict) or not raw_keys:
        raise ConfigError("keys object is required")
    keys_hex: dict[str, str] = {}
    for device_id, value in raw_keys.items():
        if device_id not in seen_ids:
            raise ConfigError(f"key for unknown device id: {device_id}")
        try:
            keys_hex[device_id] = parse_hex_key(str(value)).hex()
        except DecodeError as exc:
            raise ConfigError(f"invalid key for {device_id}") from exc
    missing = seen_ids - set(keys_hex)
    if missing:
        raise ConfigError("missing AES key for: " + ", ".join(sorted(missing)))
    stale = int(payload.get("staleAfterSeconds", 90))
    if stale < 5 or stale > 3600:
        raise ConfigError("staleAfterSeconds must be between 5 and 3600")
    return FleetConfig(
        schema_version=schema,
        revision=revision,
        updated_at_utc=updated,
        devices=tuple(devices),
        keys_hex=keys_hex,
        stale_after_seconds=stale,
    )


def parse_fleet_json(text: str) -> FleetConfig:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ConfigError("invalid fleet JSON") from exc
    return parse_fleet_config(payload)


def _read_json_object(path: Path) -> Optional[dict]:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def files_match(fleet: FleetConfig, devices_path: Path, keys_path: Path) -> bool:
    existing_devices = _read_json_object(devices_path)
    existing_keys = _read_json_object(keys_path)
    if existing_devices is None or existing_keys is None:
        return False
    wanted_devices = [
        {
            "id": item.device_id,
            "address": item.address,
            "product": item.product,
            "productId": item.product_id,
            "minIntervalSeconds": int(item.min_interval_seconds or 0),
        }
        for item in fleet.devices
    ]
    got_devices = []
    for item in existing_devices.get("devices") or []:
        if not isinstance(item, dict):
            return False
        try:
            product_id = parse_product_id(item.get("productId"))
            address = normalize_mac(item.get("address", ""))
            min_interval = parse_min_interval_seconds(item.get("minIntervalSeconds"))
        except ConfigError:
            return False
        got_devices.append(
            {
                "id": str(item.get("id", "")).strip(),
                "address": address,
                "product": str(item.get("product", "")).strip(),
                "productId": product_id,
                "minIntervalSeconds": min_interval,
            }
        )
    raw_keys = existing_keys.get("keys")
    if not isinstance(raw_keys, dict):
        return False
    got_keys: dict[str, str] = {}
    for device_id, value in raw_keys.items():
        try:
            got_keys[str(device_id)] = parse_hex_key(str(value)).hex()
        except DecodeError:
            return False
    return wanted_devices == got_devices and got_keys == fleet.keys_hex


def _chown_group(path: Path, group_name: str) -> None:
    try:
        import grp

        os.chown(path, 0, grp.getgrnam(group_name).gr_gid)
    except (KeyError, OSError, PermissionError):
        try:
            os.chown(path, 0, -1)
        except OSError:
            pass


def write_fleet_files(
    fleet: FleetConfig,
    *,
    devices_path: Path = DEFAULT_DEVICES,
    keys_path: Path = DEFAULT_KEYS,
    state_path: Path = DEFAULT_FLEET_STATE,
    keys_mode: int = 0o640,
) -> None:
    devices_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(devices_path, fleet.devices_payload(), mode=0o644)
    atomic_write_json(keys_path, fleet.keys_payload(), mode=keys_mode)
    _chown_group(keys_path, "t28-victron")
    atomic_write_json(state_path, fleet.state_payload(), mode=0o644)


def _default_systemctl(argv: list[str]) -> Optional[int]:
    joined = " ".join(argv)
    if any(token in joined for token in FORBIDDEN_UNITS):
        raise RuntimeError("fleet apply must not restart bluetoothd, t28-v2-receiver, or publish")
    if SIDECAR_UNIT not in argv:
        raise RuntimeError(f"fleet apply may only touch {SIDECAR_UNIT}")
    try:
        completed = subprocess.run(
            ["systemctl", *argv],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None
    return completed.returncode


def ble_unit_is_enabled(systemctl: Systemctl = _default_systemctl) -> bool:
    code = systemctl(["is-enabled", "--quiet", SIDECAR_UNIT])
    return code == 0


def restart_ble_if_enabled(systemctl: Systemctl = _default_systemctl) -> str:
    if not ble_unit_is_enabled(systemctl):
        LOGGER.info("fleet files written; %s is disabled so it was not restarted", SIDECAR_UNIT)
        return "skipped-disabled"
    code = systemctl(["restart", SIDECAR_UNIT])
    if code == 0:
        LOGGER.info("restarted %s only (bluetoothd and t28-v2-receiver were not touched)", SIDECAR_UNIT)
        return "restarted"
    LOGGER.error("failed to restart %s", SIDECAR_UNIT)
    return "restart-failed"


def apply_fleet_config(
    payload: Any,
    *,
    devices_path: Path = DEFAULT_DEVICES,
    keys_path: Path = DEFAULT_KEYS,
    state_path: Path = DEFAULT_FLEET_STATE,
    restart_ble: bool = True,
    systemctl: Systemctl = _default_systemctl,
) -> ApplyResult:
    fleet = parse_fleet_config(payload)
    changed = not files_match(fleet, devices_path, keys_path)
    if changed:
        write_fleet_files(fleet, devices_path=devices_path, keys_path=keys_path, state_path=state_path)
        LOGGER.info(
            "applied fleet revision %s (%s devices, files changed)",
            fleet.revision,
            len(fleet.devices),
        )
    else:
        write_fleet_files(fleet, devices_path=devices_path, keys_path=keys_path, state_path=state_path)
        LOGGER.info("fleet revision %s already matched on-disk files", fleet.revision)
        return ApplyResult(False, fleet.revision, len(fleet.devices), "skipped-unchanged")
    action = "not-requested"
    if restart_ble:
        action = restart_ble_if_enabled(systemctl)
    return ApplyResult(True, fleet.revision, len(fleet.devices), action)


def apply_fleet_file(path: Path, **kwargs: Any) -> ApplyResult:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigError(f"cannot read fleet payload: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError("invalid fleet JSON") from exc
    return apply_fleet_config(payload, **kwargs)


def build_fleet_payload(
    devices: list[DeviceConfig] | list[FleetDevice] | list[dict],
    keys_hex: dict[str, str],
    *,
    revision: int,
    updated_at_utc: Optional[str] = None,
    stale_after_seconds: int = 90,
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for item in devices:
        if isinstance(item, DeviceConfig):
            entry: dict[str, Any] = {"id": item.device_id, "address": item.address, "product": item.product}
            if item.product_id is not None:
                entry["productId"] = f"0x{item.product_id:04X}"
            items.append(entry)
        elif isinstance(item, FleetDevice):
            items.append(item.as_devices_entry())
        elif isinstance(item, dict):
            items.append(item)
        else:
            raise ConfigError("unsupported device entry")
    return {
        "schemaVersion": SCHEMA_VERSION,
        "revision": revision,
        "updatedAtUtc": updated_at_utc or format_utc(utc_now()),
        "staleAfterSeconds": stale_after_seconds,
        "devices": items,
        "keys": dict(keys_hex),
    }


def apply_fleet_main(argv: list[str] | None = None) -> int:
    """Root helper: apply a validated payload file, then maybe bounce BLE."""
    import argparse
    import sys

    parser = argparse.ArgumentParser(prog="t28-victron-apply-fleet-config")
    parser.add_argument("payload", help="path to a fleet config JSON object")
    parser.add_argument("--devices", default=str(DEFAULT_DEVICES))
    parser.add_argument("--keys", default=str(DEFAULT_KEYS))
    parser.add_argument("--state", default=str(DEFAULT_FLEET_STATE))
    parser.add_argument("--no-restart", action="store_true")
    args = parser.parse_args(argv)
    if os.geteuid() != 0:
        print("ERROR: t28-victron-apply-fleet-config must run as root", file=sys.stderr)
        return 1
    path = Path(args.payload)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        result = apply_fleet_config(
            raw,
            devices_path=Path(args.devices),
            keys_path=Path(args.keys),
            state_path=Path(args.state),
            restart_ble=not args.no_restart,
        )
    except (OSError, json.JSONDecodeError, ConfigError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(
        f"fleet revision={result.revision} devices={result.device_count} "
        f"changed={str(result.changed).lower()} ble={result.ble_action}"
    )
    try:
        if path.is_file() and path.name == "incoming-config.json":
            data = path.read_bytes()
            path.write_bytes(b"\x00" * len(data))
            path.unlink()
    except OSError:
        pass
    return 0 if result.ble_action != "restart-failed" else 3
