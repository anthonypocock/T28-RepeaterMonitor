"""Hub-side fleet config publisher (demo-hub). Sibling of T28.V2.Server.

Reads /etc/t28-victron/devices.json + keys.json and publishes retained
demo/power/config/v1. CLI add/update/remove uses hidden getpass; never --key.
"""

from __future__ import annotations

import argparse
import getpass
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from . import SCHEMA_VERSION
from .config import DEVICE_ID_RE, ConfigError, is_valid_device_id, normalize_mac, parse_product_id
from .decoder import DecodeError, parse_hex_key
from .fleet import build_fleet_payload, parse_fleet_config
from .models import format_utc, utc_now
from .publish import (
    PahoMqttTransport,
    PublishConfig,
    PublishConfigError,
    is_placeholder_broker,
    load_credentials,
    parse_broker,
)
from .store import atomic_write_json
from .topics import CONFIG_TOPIC, OVERLAY_BROKER_DEFAULT

LOGGER = logging.getLogger("t28-victron-hub-config")

DEFAULT_HUB = Path("/etc/t28-victron/hub.json")
DEFAULT_DEVICES = Path("/etc/t28-victron/devices.json")
DEFAULT_KEYS = Path("/etc/t28-victron/keys.json")
DEFAULT_HUB_CREDENTIALS = Path("/etc/t28-victron/hub-credentials.json")


@dataclass(frozen=True)
class HubConfig:
    broker: str
    topic: str
    client_id: str
    qos: int
    tls: bool
    host: str
    port: int
    credentials_path: Path
    devices_path: Path
    keys_path: Path
    stale_after_seconds: int
    poll_seconds: float


def configure_logging() -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
    handler.flush = sys.stderr.flush  # type: ignore[method-assign]
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)


def load_hub_config(
    path: Path = DEFAULT_HUB,
    *,
    credentials_path: Optional[Path] = None,
) -> HubConfig:
    payload: dict[str, Any] = {}
    if path.is_file():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise PublishConfigError(f"invalid JSON in {path}") from exc
        if not isinstance(loaded, dict):
            raise PublishConfigError("hub.json must be a JSON object")
        payload = loaded
    if "password" in payload or "username" in payload:
        raise PublishConfigError("do not put MQTT credentials in hub.json")
    broker = str(payload.get("broker") or OVERLAY_BROKER_DEFAULT).strip()
    host, port, scheme_tls = parse_broker(broker)
    creds = Path(credentials_path or payload.get("credentialsPath") or DEFAULT_HUB_CREDENTIALS)
    if creds.name == "keys.json":
        raise PublishConfigError("hub credentials must not be keys.json")
    return HubConfig(
        broker=broker,
        topic=str(payload.get("configTopic") or payload.get("topic") or CONFIG_TOPIC),
        client_id=str(payload.get("clientId") or "t28-victron-hub-config"),
        qos=int(payload.get("qos", 1)),
        tls=bool(payload.get("tls", False)) or scheme_tls,
        host=host,
        port=port,
        credentials_path=creds,
        devices_path=Path(payload.get("devicesPath") or DEFAULT_DEVICES),
        keys_path=Path(payload.get("keysPath") or DEFAULT_KEYS),
        stale_after_seconds=int(payload.get("staleAfterSeconds", 90)),
        poll_seconds=float(payload.get("pollSeconds", 2)),
    )


def _as_publish_config(config: HubConfig) -> PublishConfig:
    return PublishConfig(
        enabled=True,
        broker=config.broker,
        topic=config.topic,
        client_id=config.client_id,
        qos=config.qos,
        retain=True,
        interval_seconds=5,
        tls=config.tls,
        host=config.host,
        port=config.port,
        latest_path=Path("/run/t28-victron/heard.json"),
        credentials_path=config.credentials_path,
    )


def _read_json(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"invalid JSON in {path}") from exc
    if not isinstance(payload, dict):
        raise ConfigError(f"{path} must contain a JSON object")
    return payload


def load_local_fleet(devices_path: Path, keys_path: Path) -> dict:
    devices_payload = _read_json(devices_path)
    keys_payload = _read_json(keys_path)
    raw_devices = devices_payload.get("devices")
    raw_keys = keys_payload.get("keys")
    if not isinstance(raw_devices, list) or not raw_devices:
        raise ConfigError("hub devices.json must list at least one device")
    if not isinstance(raw_keys, dict) or not raw_keys:
        raise ConfigError("hub keys.json must contain a keys object")
    revision = int(devices_payload.get("revision") or 1)
    return build_fleet_payload(
        raw_devices,
        {str(device_id): str(value) for device_id, value in raw_keys.items()},
        revision=revision,
        updated_at_utc=str(devices_payload.get("updatedAtUtc") or format_utc(utc_now())),
        stale_after_seconds=int(devices_payload.get("staleAfterSeconds", 90)),
    )


def publish_fleet(config: HubConfig, payload: dict, *, transport: Any = None) -> None:
    parse_fleet_config(payload)
    if is_placeholder_broker(config.broker):
        raise PublishConfigError("hub broker must not be the localhost placeholder")
    body = json.dumps(payload, separators=(",", ":"))
    client = transport or PahoMqttTransport(_as_publish_config(config), load_credentials(config.credentials_path))
    owned = transport is None
    try:
        client.connect()
        client.publish(config.topic, body, qos=config.qos, retain=True)
    finally:
        if owned and hasattr(client, "close"):
            client.close()
    LOGGER.info("published %s revision=%s devices=%s", config.topic, payload.get("revision"), len(payload.get("devices") or []))


def _next_revision(devices_path: Path) -> int:
    payload = _read_json(devices_path)
    current = payload.get("revision")
    try:
        return int(current) + 1 if current is not None else 1
    except (TypeError, ValueError):
        return 1


def list_devices_public(devices_path: Path, keys_path: Path) -> dict[str, Any]:
    """Admin device list. Includes per-device hex `key`, never a `keys` object."""
    devices_payload = _read_json(devices_path)
    keys_payload = _read_json(keys_path)
    raw_keys = keys_payload.get("keys")
    present = raw_keys if isinstance(raw_keys, dict) else {}
    public: list[dict[str, Any]] = []
    for item in devices_payload.get("devices") or []:
        if not isinstance(item, dict):
            continue
        device_id = str(item.get("id") or "").strip()
        if not device_id:
            continue
        try:
            address = normalize_mac(str(item.get("address") or ""))
        except ConfigError:
            address = str(item.get("address") or "").strip()
        key_hex = ""
        raw_key = present.get(device_id)
        if raw_key:
            try:
                key_hex = parse_hex_key(str(raw_key)).hex()
            except DecodeError:
                key_hex = ""
        public.append(
            {
                "id": device_id,
                "address": address,
                "product": str(item.get("product") or "").strip(),
                "productId": item.get("productId"),
                "keyPresent": bool(key_hex),
                "key": key_hex,
            }
        )
    revision = devices_payload.get("revision")
    try:
        revision_n = int(revision) if revision is not None else 0
    except (TypeError, ValueError):
        revision_n = 0
    return {
        "schemaVersion": int(devices_payload.get("schemaVersion") or SCHEMA_VERSION),
        "revision": revision_n,
        "updatedAtUtc": devices_payload.get("updatedAtUtc"),
        "devices": public,
    }


ADMIN_DEVICE_FIELDS = ("id", "address", "product", "productId", "keyPresent", "key")


def admin_devices_response(payload: dict[str, Any]) -> dict[str, Any]:
    """Allowlist admin JSON so a `keys` object can never leak out."""
    devices: list[dict[str, Any]] = []
    for item in payload.get("devices") or []:
        if not isinstance(item, dict):
            continue
        devices.append({field: item.get(field) for field in ADMIN_DEVICE_FIELDS})
    return {
        "schemaVersion": payload.get("schemaVersion"),
        "revision": payload.get("revision"),
        "updatedAtUtc": payload.get("updatedAtUtc"),
        "devices": devices,
    }


def write_device(
    devices_path: Path,
    keys_path: Path,
    *,
    device_id: str,
    address: str,
    product: str,
    product_id: Optional[int],
    key_hex: Optional[str] = None,
    min_interval_seconds: int = 0,
    aliases: Optional[list[str]] = None,
    previous_id: Optional[str] = None,
) -> None:
    device_id = str(device_id).strip()
    if not DEVICE_ID_RE.fullmatch(device_id) and not is_valid_device_id(device_id):
        raise ConfigError("invalid device id")
    previous_id = str(previous_id or "").strip()
    if previous_id == device_id:
        previous_id = ""
    if previous_id and not DEVICE_ID_RE.fullmatch(previous_id) and not is_valid_device_id(previous_id):
        raise ConfigError("invalid previousId")
    address = normalize_mac(address)
    product = str(product or "").strip()
    if not product:
        raise ConfigError("product is required")
    devices_payload = _read_json(devices_path)
    keys_payload = _read_json(keys_path)
    source_id = previous_id or device_id
    preserved_aliases: list[str] = []
    for item in devices_payload.get("devices") or []:
        if isinstance(item, dict) and item.get("id") == source_id:
            preserved_aliases = [str(a).strip() for a in (item.get("aliases") or []) if str(a).strip()]
            break
    if aliases is not None:
        preserved_aliases = [str(a).strip() for a in aliases if str(a).strip()]
    if previous_id and previous_id not in preserved_aliases:
        preserved_aliases.append(previous_id)
    unique_aliases: list[str] = []
    seen_aliases: set[str] = set()
    for alias in preserved_aliases:
        if alias and alias != device_id and alias not in seen_aliases:
            seen_aliases.add(alias)
            unique_aliases.append(alias)
    drop_ids = {device_id}
    if previous_id:
        drop_ids.add(previous_id)
    devices = [
        item
        for item in devices_payload.get("devices", [])
        if isinstance(item, dict) and item.get("id") not in drop_ids
    ]
    entry: dict[str, Any] = {"id": device_id, "address": address, "product": product}
    if product_id is not None:
        entry["productId"] = f"0x{product_id:04X}"
    if unique_aliases:
        entry["aliases"] = unique_aliases
    entry["minIntervalSeconds"] = int(min_interval_seconds or 0)
    devices.append(entry)
    devices_payload["schemaVersion"] = SCHEMA_VERSION
    devices_payload["devices"] = devices
    devices_payload["revision"] = _next_revision(devices_path)
    devices_payload["updatedAtUtc"] = format_utc(utc_now())
    devices_payload.setdefault("staleAfterSeconds", 90)
    keys_payload.setdefault("schemaVersion", SCHEMA_VERSION)
    raw_keys = keys_payload.get("keys")
    if not isinstance(raw_keys, dict):
        raw_keys = {}
        keys_payload["keys"] = raw_keys
    incoming = (key_hex or "").strip()
    write_keys = True
    if incoming:
        raw_keys[device_id] = parse_hex_key(incoming).hex()
    elif raw_keys.get(device_id):
        write_keys = bool(previous_id)
    elif previous_id and raw_keys.get(previous_id):
        raw_keys[device_id] = raw_keys[previous_id]
    else:
        raise ConfigError("AES key required for a new device")
    if previous_id:
        raw_keys.pop(previous_id, None)
        write_keys = True
    devices_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(devices_path, devices_payload, mode=0o644)
    if write_keys:
        atomic_write_json(keys_path, keys_payload, mode=0o640)
    try:
        import grp

        gid = grp.getgrnam("t28-victron").gr_gid
        os.chown(keys_path, 0, gid)
        os.chown(devices_path, -1, gid)
    except (KeyError, OSError, PermissionError):
        pass


def remove_device(devices_path: Path, keys_path: Path, device_id: str) -> None:
    devices_payload = _read_json(devices_path)
    keys_payload = _read_json(keys_path)
    devices = [item for item in devices_payload.get("devices", []) if isinstance(item, dict) and item.get("id") != device_id]
    if len(devices) == len(devices_payload.get("devices", [])):
        raise ConfigError(f"device not found: {device_id}")
    devices_payload["devices"] = devices
    devices_payload["revision"] = _next_revision(devices_path)
    devices_payload["updatedAtUtc"] = format_utc(utc_now())
    raw_keys = keys_payload.get("keys")
    if isinstance(raw_keys, dict):
        raw_keys.pop(device_id, None)
    atomic_write_json(devices_path, devices_payload, mode=0o644)
    atomic_write_json(keys_path, keys_payload, mode=0o640)


def _existing_key_hex(keys_path: Path, device_id: str) -> str | None:
    raw = _read_json(keys_path).get("keys")
    if not isinstance(raw, dict) or device_id not in raw:
        return None
    try:
        return parse_hex_key(str(raw[device_id])).hex()
    except DecodeError:
        return None


def _resolve_key_hex(keys_path: Path, device_id: str) -> str:
    key_text = getpass.getpass("AES-128 advertisement key (32 hex chars, input hidden): ")
    if not str(key_text).strip():
        existing = _existing_key_hex(keys_path, device_id)
        if existing is None:
            raise ConfigError("no AES key provided and no existing key for this id")
        print("keeping existing key")
        return existing
    return parse_hex_key(key_text).hex()


def _show_targets(devices_path: Path, keys_path: Path) -> int:
    if not devices_path.is_file():
        print(f"ERROR: {devices_path} is not present", file=sys.stderr)
        return 1
    payload = _read_json(devices_path)
    present_ids: set[str] = set()
    raw = _read_json(keys_path).get("keys")
    if isinstance(raw, dict):
        present_ids = {str(device_id) for device_id, value in raw.items() if value}
    devices = payload.get("devices")
    if not isinstance(devices, list):
        print("ERROR: devices.json must list devices", file=sys.stderr)
        return 1
    print(f"revision={payload.get('revision', '')} updatedAtUtc={payload.get('updatedAtUtc', '')}")
    for item in devices:
        if not isinstance(item, dict):
            continue
        device_id = str(item.get("id", "")).strip()
        try:
            address = normalize_mac(item.get("address", ""))
        except ConfigError:
            address = str(item.get("address", "")).strip()
        product = str(item.get("product", "")).strip()
        key_present = "yes" if device_id in present_ids else "no"
        print(f"{device_id}  address={address}  product={product}  keyPresent={key_present}")
    return 0


def hub_configure_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="t28-victron-hub-configure")
    parser.add_argument("--hub", default=str(DEFAULT_HUB))
    parser.add_argument("--devices", default=str(DEFAULT_DEVICES))
    parser.add_argument("--keys", default=str(DEFAULT_KEYS))
    parser.add_argument("--credentials", default=str(DEFAULT_HUB_CREDENTIALS))
    parser.add_argument("--broker", default="")
    parser.add_argument("--id", default="")
    parser.add_argument("--address", default="")
    parser.add_argument("--product", default="")
    parser.add_argument("--product-id", default="")
    parser.add_argument("--add", action="store_true")
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--remove", action="store_true")
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--show", action="store_true")
    args = parser.parse_args(argv)
    if os.geteuid() != 0:
        print("ERROR: run t28-victron-hub-configure with sudo", file=sys.stderr)
        return 1
    devices_path = Path(args.devices)
    keys_path = Path(args.keys)
    if args.show and not (args.add or args.update or args.remove or args.publish or args.broker):
        return _show_targets(devices_path, keys_path)
    mutating = args.add or args.update or args.remove
    if mutating and args.remove and (args.add or args.update):
        print("ERROR: use only one of --add/--update or --remove", file=sys.stderr)
        return 1
    try:
        if args.broker:
            hub_path = Path(args.hub)
            payload = _read_json(hub_path) if hub_path.is_file() else {}
            payload["schemaVersion"] = SCHEMA_VERSION
            payload["broker"] = args.broker.strip()
            payload.setdefault("configTopic", CONFIG_TOPIC)
            payload.pop("username", None)
            payload.pop("password", None)
            atomic_write_json(hub_path, payload, mode=0o644)
            print(f"Wrote {hub_path} (no secrets).")
        if args.remove:
            device_id = args.id or input("Logical device id to remove: ").strip()
            if not device_id:
                raise ConfigError("device id is required")
            remove_device(devices_path, keys_path, device_id)
            print(f"Removed {device_id} from hub device map.")
        elif args.add or args.update:
            device_id = args.id or input("Logical device id [house-bank]: ").strip() or "house-bank"
            raw_address = args.address or input("Bluetooth MAC address: ").strip()
            address = normalize_mac(raw_address)
            product = args.product or input("Product label [SmartShunt 500A/50mV]: ").strip() or "SmartShunt 500A/50mV"
            product_id_text = args.product_id.strip()
            product_id = parse_product_id(product_id_text) if product_id_text else None
            key_hex = _resolve_key_hex(keys_path, device_id)
            write_device(
                devices_path,
                keys_path,
                device_id=device_id,
                address=address,
                product=product,
                product_id=product_id,
                key_hex=key_hex,
            )
            print(f"Wrote {devices_path} and a root:t28-victron 0640 keys file.")
        should_publish = args.publish or mutating
        if should_publish:
            config = load_hub_config(Path(args.hub), credentials_path=Path(args.credentials))
            publish_fleet(config, load_local_fleet(devices_path, keys_path))
            print(f"Published retained {CONFIG_TOPIC}.")
        if args.show:
            return _show_targets(devices_path, keys_path)
    except (ConfigError, PublishConfigError, DecodeError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 0


def _mtimes(paths: list[Path]) -> tuple[float, ...]:
    stamps = []
    for path in paths:
        try:
            stamps.append(path.stat().st_mtime)
        except OSError:
            stamps.append(0.0)
    return tuple(stamps)


def run_forever(
    config: HubConfig,
    *,
    transport: Any = None,
    sleep: Any = time.sleep,
    should_continue: Any = None,
) -> int:
    if is_placeholder_broker(config.broker):
        LOGGER.error("hub broker is still the localhost placeholder")
        return 2
    last: tuple[float, ...] | None = None
    keep_going = should_continue or (lambda: True)
    client = transport
    owned = transport is None
    credentials = load_credentials(config.credentials_path)

    def _publish_retained() -> None:
        payload = load_local_fleet(config.devices_path, config.keys_path)
        parse_fleet_config(payload)
        body = json.dumps(payload, separators=(",", ":"))
        client.publish(config.topic, body, qos=config.qos, retain=True)
        LOGGER.info(
            "published %s revision=%s devices=%s",
            config.topic,
            payload.get("revision"),
            len(payload.get("devices") or []),
        )

    try:
        if client is None:
            client = PahoMqttTransport(_as_publish_config(config), credentials)
        if hasattr(client, "set_on_connect_hook"):
            client.set_on_connect_hook(_publish_retained)
        if hasattr(client, "connect"):
            client.connect()
        while keep_going():
            current = _mtimes([config.devices_path, config.keys_path])
            if last is None or current != last:
                try:
                    _publish_retained()
                    last = current
                except (ConfigError, PublishConfigError) as exc:
                    LOGGER.error("hub config publish skipped: %s", exc)
            sleep(max(1.0, config.poll_seconds))
    finally:
        if owned and client is not None and hasattr(client, "close"):
            try:
                client.close()
            except Exception:
                pass
    return 0


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    del argv
    try:
        return run_forever(load_hub_config())
    except PublishConfigError as exc:
        LOGGER.error("%s", exc)
        return 2
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        LOGGER.error("hub-config failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
