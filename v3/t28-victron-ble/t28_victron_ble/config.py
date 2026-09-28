"""Load non-secret device map and AES keys from files or systemd credentials."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .decoder import DecodeError, parse_hex_key
from .hci import FIELD_SMARTSHUNT_ADDRESS, FIELD_SMARTSHUNT_PRODUCT_ID

DEVICE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ._':-]{0,79}$")
DEVICE_ID_MQTT_WILDCARDS = frozenset("+#")


def is_valid_device_id(device_id: str) -> bool:
    """Allow spaces and incident labels; never MQTT wildcards + or #."""
    text = str(device_id or "")
    if not DEVICE_ID_RE.fullmatch(text):
        return False
    if any(char in DEVICE_ID_MQTT_WILDCARDS for char in text):
        return False
    return True


def parse_min_interval_seconds(value) -> int:
    """Missing/blank/null = 0. Reject negative. Cap at 86400."""
    if value is None or value == "":
        return 0
    if isinstance(value, bool):
        raise ConfigError("minIntervalSeconds must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError("minIntervalSeconds must be an integer") from exc
    if parsed < 0:
        raise ConfigError("minIntervalSeconds must not be negative")
    if parsed > 86400:
        parsed = 86400
    return parsed


MAC_RE = re.compile(r"^[0-9a-f]{2}(:[0-9a-f]{2}){5}$")
_HEX_CHARS = frozenset("0123456789abcdef")


class ConfigError(ValueError):
    """Configuration is missing or invalid; the service must not start."""


@dataclass(frozen=True)
class DeviceConfig:
    device_id: str
    address: str
    product: str = ""
    product_id: Optional[int] = None


@dataclass(frozen=True)
class ServiceConfig:
    devices: tuple[DeviceConfig, ...]
    keys: dict[str, bytes]
    stale_after_seconds: int = 90
    history_enabled: bool = False
    history_path: Optional[Path] = None
    history_interval_seconds: int = 60
    history_max_bytes: int = 256 * 1024
    run_dir: Path = Path("/run/t28-victron")
    latest_path: Path = Path("/run/t28-victron/latest.json")

    def device_by_id(self, device_id: str) -> DeviceConfig:
        for device in self.devices:
            if device.device_id == device_id:
                return device
        raise KeyError(device_id)

    def device_by_address(self, address: str) -> Optional[DeviceConfig]:
        try:
            wanted = normalize_mac(address)
        except ConfigError:
            wanted = str(address).strip().lower()
        for device in self.devices:
            if device.address == wanted:
                return device
        return None


def normalize_mac(value: str) -> str:
    """Strip non-hex, require 12 hex chars, return aa:bb:cc:dd:ee:ff lowercase.

    Accepts VictronConnect Instant readout form (`020000000001`) and colon form.
    """
    hex_only = "".join(char for char in str(value).lower() if char in _HEX_CHARS)
    if len(hex_only) != 12:
        raise ConfigError("device address must be a Bluetooth MAC (12 hex characters)")
    return ":".join(hex_only[index : index + 2] for index in range(0, 12, 2))


def parse_product_id(value) -> Optional[int]:
    if value is None or value == "":
        return None
    text = str(value).strip().lower()
    try:
        parsed = int(text, 0)
    except ValueError as exc:
        raise ConfigError(f"invalid productId: {value}") from exc
    if parsed < 0 or parsed > 0xFFFF:
        raise ConfigError("productId must be a 16-bit Victron model id")
    return parsed


def field_smartshunt_defaults() -> tuple[str, int]:
    return FIELD_SMARTSHUNT_ADDRESS, FIELD_SMARTSHUNT_PRODUCT_ID


def _read_json(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"missing configuration file: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(f"invalid JSON in {path}") from exc
    if not isinstance(payload, dict):
        raise ConfigError(f"{path} must contain a JSON object")
    return payload


def _load_devices(path: Path) -> tuple[list[DeviceConfig], dict]:
    payload = _read_json(path)
    raw_devices = payload.get("devices")
    if not isinstance(raw_devices, list) or not raw_devices:
        raise ConfigError("devices.json must list at least one device")
    devices: list[DeviceConfig] = []
    seen_ids: set[str] = set()
    seen_addresses: set[str] = set()
    for item in raw_devices:
        if not isinstance(item, dict):
            raise ConfigError("each device must be an object")
        device_id = str(item.get("id", "")).strip()
        product = str(item.get("product", "")).strip()
        product_id = parse_product_id(item.get("productId"))
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
        devices.append(
            DeviceConfig(device_id=device_id, address=address, product=product, product_id=product_id)
        )
    return devices, payload


def _load_keys(payload: dict, device_ids: set[str]) -> dict[str, bytes]:
    raw = payload.get("keys")
    if not isinstance(raw, dict) or not raw:
        raise ConfigError("keys file must contain a keys object")
    keys: dict[str, bytes] = {}
    for device_id, value in raw.items():
        if device_id not in device_ids:
            raise ConfigError(f"key for unknown device id: {device_id}")
        try:
            keys[device_id] = parse_hex_key(str(value))
        except DecodeError as exc:
            raise ConfigError(f"invalid key for {device_id}") from exc
    missing = device_ids - set(keys)
    if missing:
        raise ConfigError("missing AES key for: " + ", ".join(sorted(missing)))
    return keys


def load_keys_payload(path: Optional[Path] = None) -> dict:
    credential_dir = os.environ.get("CREDENTIALS_DIRECTORY")
    candidates: list[Path] = []
    if credential_dir:
        candidates.append(Path(credential_dir) / "victron-keys")
    if path is not None:
        candidates.append(path)
    for candidate in candidates:
        if candidate.is_file():
            return _read_json(candidate)
    raise ConfigError("no AES key material found (systemd credential or keys file)")


def load_service_config(
    devices_path: Path,
    keys_path: Optional[Path] = None,
    *,
    run_dir: Optional[Path] = None,
) -> ServiceConfig:
    devices, payload = _load_devices(devices_path)
    keys = _load_keys(load_keys_payload(keys_path), {item.device_id for item in devices})
    stale = int(payload.get("staleAfterSeconds", 90))
    if stale < 5 or stale > 3600:
        raise ConfigError("staleAfterSeconds must be between 5 and 3600")
    history = payload.get("commissioningHistory") or {}
    if history and not isinstance(history, dict):
        raise ConfigError("commissioningHistory must be an object")
    history_enabled = bool(history.get("enabled", False))
    history_path = Path(history.get("path", "/var/lib/t28-victron/history.jsonl"))
    run = Path(run_dir or payload.get("runDir") or "/run/t28-victron")
    return ServiceConfig(
        devices=tuple(devices),
        keys=keys,
        stale_after_seconds=stale,
        history_enabled=history_enabled,
        history_path=history_path if history_enabled else None,
        history_interval_seconds=int(history.get("intervalSeconds", 60)),
        history_max_bytes=int(history.get("maxBytes", 256 * 1024)),
        run_dir=run,
        latest_path=run / "latest.json",
    )
