"""Operator tools: configure (hidden key), status, check. Never put keys on argv."""

from __future__ import annotations

import argparse
import getpass
import json
import os
import stat
import subprocess
import sys
import time
from pathlib import Path

from . import SCHEMA_VERSION
from .config import ConfigError, field_smartshunt_defaults, load_service_config, normalize_mac, parse_product_id
from .decoder import DecodeError, decode_battery_monitor, parse_hex_key
from .store import LatestStore

DEFAULT_DEVICES = Path("/etc/t28-victron/devices.json")
DEFAULT_KEYS = Path("/etc/t28-victron/keys.json")
DEFAULT_LATEST = Path("/run/t28-victron/latest.json")
SIDECAR_UNIT = "t28-victron-ble.service"
# Synthetic public fixture: sequential-byte key; generated AES-CTR packet, not device data.
GOLDEN_ADVERTISEMENT = bytes.fromhex("100289a302b04000d7f8940e7cf73193ebab1ec2f46596")
GOLDEN_KEY = bytes.fromhex("00112233445566778899aabbccddeeff")


def _die(message: str, code: int = 1) -> int:
    print(f"ERROR: {message}", file=sys.stderr)
    return code


def configure_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="t28-victron-configure")
    parser.add_argument("--devices", default=str(DEFAULT_DEVICES))
    parser.add_argument("--keys", default=str(DEFAULT_KEYS))
    parser.add_argument("--latest", default=str(DEFAULT_LATEST))
    parser.add_argument("--id", default="")
    parser.add_argument("--address", default="")
    parser.add_argument("--product", default="")
    parser.add_argument("--product-id", default="")
    parser.add_argument(
        "--key-file",
        default="",
        help="read the 32-hex Instant Readout key from this file (never pass the key on argv)",
    )
    parser.add_argument(
        "--shred-key-file",
        action="store_true",
        help="overwrite then delete --key-file after a successful write (default: keep the file)",
    )
    parser.add_argument("--show", action="store_true", help="print configured targets; never print key material")
    parser.add_argument("--enable", action="store_true")
    parser.add_argument(
        "--verify",
        action="store_true",
        help="after write, wait for latest.json fresh or keyMismatch (implied by --enable)",
    )
    parser.add_argument("--verify-timeout", type=float, default=30.0)
    args = parser.parse_args(argv)
    if os.geteuid() != 0:
        return _die("run t28-victron-configure with sudo")
    devices_path = Path(args.devices)
    keys_path = Path(args.keys)
    if args.show:
        return _show_targets(devices_path, keys_path)
    default_address, default_product_id = field_smartshunt_defaults()
    device_id = args.id or input("Logical device id [house-bank]: ").strip() or "house-bank"
    raw_address = args.address or input(f"Bluetooth MAC address [{default_address}]: ") or default_address
    try:
        address = normalize_mac(raw_address)
    except ConfigError as exc:
        return _die(str(exc))
    product = args.product or input("Product label [SmartShunt 500A/50mV]: ").strip() or "SmartShunt 500A/50mV"
    product_id_text = args.product_id or input(f"Product id [0x{default_product_id:04X}]: ").strip() or f"0x{default_product_id:04X}"
    try:
        product_id = parse_product_id(product_id_text)
    except ConfigError as exc:
        return _die(str(exc))
    try:
        key_hex = _resolve_key_hex(args.key_file, keys_path, device_id)
    except (ConfigError, DecodeError, OSError) as exc:
        return _die(str(exc))
    devices_path.parent.mkdir(parents=True, exist_ok=True)
    existing = {"schemaVersion": SCHEMA_VERSION, "devices": []}
    if devices_path.is_file():
        existing = json.loads(devices_path.read_text(encoding="utf-8"))
    devices = [item for item in existing.get("devices", []) if item.get("id") != device_id]
    device_entry = {"id": device_id, "address": address, "product": product}
    if product_id is not None:
        device_entry["productId"] = f"0x{product_id:04X}"
    devices.append(device_entry)
    existing["schemaVersion"] = SCHEMA_VERSION
    existing["devices"] = devices
    existing.setdefault("staleAfterSeconds", 90)
    keys_payload = {"schemaVersion": SCHEMA_VERSION, "keys": {}}
    if keys_path.is_file():
        keys_payload = json.loads(keys_path.read_text(encoding="utf-8"))
    keys_payload.setdefault("keys", {})[device_id] = key_hex
    _atomic_private(devices_path, existing, 0o644)
    _atomic_private(keys_path, keys_payload, 0o640)
    os.chown(keys_path, 0, _group_id("t28-victron"))
    if args.shred_key_file and args.key_file:
        _shred_file(Path(args.key_file))
    print(f"Wrote {devices_path} and a root:t28-victron 0640 keys file.")
    print("The service is not started by the receiver installer. Enable it only on this Pi.")
    if args.enable:
        print("Wrapper will prepare hci0 (rfkill / power on) then enable the unit.")
        print("bluetoothd and t28-v2-receiver will not be restarted.")
    if args.enable or _sidecar_is_active():
        if _restart_sidecar_only():
            print(f"Restarted {SIDECAR_UNIT} so LoadCredential picks up keys.")
            print("bluetoothd and t28-v2-receiver were not restarted.")
        elif not args.enable:
            return _die(f"failed to restart {SIDECAR_UNIT}")
    if args.enable or args.verify:
        return _verify_reading(Path(args.latest), device_id, args.verify_timeout)
    return 0


def _show_targets(devices_path: Path, keys_path: Path) -> int:
    if not devices_path.is_file():
        return _die(f"{devices_path} is not present")
    try:
        payload = json.loads(devices_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return _die(f"invalid JSON in {devices_path}: {exc}")
    present_ids: set[str] = set()
    if keys_path.is_file():
        try:
            keys_payload = json.loads(keys_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            keys_payload = {}
        raw = keys_payload.get("keys") if isinstance(keys_payload, dict) else None
        if isinstance(raw, dict):
            present_ids = {str(device_id) for device_id, value in raw.items() if value}
    devices = payload.get("devices") if isinstance(payload, dict) else None
    if not isinstance(devices, list):
        return _die("devices.json must list devices")
    for item in devices:
        if not isinstance(item, dict):
            continue
        device_id = str(item.get("id", "")).strip()
        try:
            address = normalize_mac(item.get("address", ""))
        except ConfigError:
            address = str(item.get("address", "")).strip()
        product = str(item.get("product", "")).strip()
        product_id = item.get("productId", "")
        key_present = "yes" if device_id in present_ids else "no"
        print(
            f"{device_id}  address={address}  product={product}  "
            f"productId={product_id}  keyPresent={key_present}"
        )
    fleet_state = devices_path.with_name("fleet-state.json")
    if fleet_state.is_file():
        try:
            state = json.loads(fleet_state.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            state = {}
        if isinstance(state, dict) and state.get("source") == "mqtt-fleet":
            print(
                f"fleetSource=mqtt revision={state.get('revision', '')} "
                f"(local configure is fallback; MQTT wins when present)"
            )
    return 0


def _existing_key_hex(keys_path: Path, device_id: str) -> str | None:
    if not keys_path.is_file():
        return None
    try:
        payload = json.loads(keys_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    raw = payload.get("keys") if isinstance(payload, dict) else None
    if not isinstance(raw, dict) or device_id not in raw:
        return None
    try:
        return parse_hex_key(str(raw[device_id])).hex()
    except DecodeError:
        return None


def _resolve_key_hex(key_file: str, keys_path: Path, device_id: str) -> str:
    if key_file:
        return parse_hex_key(Path(key_file).read_text(encoding="utf-8")).hex()
    key_text = getpass.getpass("AES-128 advertisement key (32 hex chars, input hidden): ")
    if not str(key_text).strip():
        existing = _existing_key_hex(keys_path, device_id)
        if existing is None:
            raise ConfigError("no AES key provided and no existing key for this id")
        print("keeping existing key")
        return existing
    return parse_hex_key(key_text).hex()


def _shred_file(path: Path) -> None:
    with open(path, "r+b", buffering=0) as handle:
        size = handle.seek(0, os.SEEK_END)
        handle.seek(0)
        handle.write(b"\x00" * size)
        handle.flush()
        os.fsync(handle.fileno())
    path.unlink()


def _run_systemctl(argv: list[str]) -> subprocess.CompletedProcess[str] | None:
    joined = " ".join(argv)
    if any(token in joined for token in ("bluetooth.service", "bluetoothd", "t28-v2-receiver")):
        raise RuntimeError("configure must not restart bluetoothd or t28-v2-receiver")
    if SIDECAR_UNIT not in argv:
        raise RuntimeError(f"configure may only touch {SIDECAR_UNIT}")
    try:
        return subprocess.run(
            ["systemctl", *argv],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None


def _sidecar_is_active() -> bool:
    completed = _run_systemctl(["is-active", "--quiet", SIDECAR_UNIT])
    return completed is not None and completed.returncode == 0


def _restart_sidecar_only() -> bool:
    completed = _run_systemctl(["restart", SIDECAR_UNIT])
    return completed is not None and completed.returncode == 0


def _fmt_null(value) -> object:
    return "null" if value is None else value


def _verify_reading(latest_path: Path, device_id: str, timeout: float) -> int:
    deadline = time.monotonic() + max(0.0, timeout)
    record: dict | None = None
    status = None
    while True:
        if latest_path.is_file():
            try:
                payload = json.loads(latest_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                payload = {}
            devices = payload.get("devices") if isinstance(payload, dict) else None
            if isinstance(devices, dict):
                candidate = devices.get(device_id)
                if isinstance(candidate, dict):
                    record = candidate
                    status = candidate.get("status")
                    if status in {"fresh", "keyMismatch"}:
                        break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(0.25, remaining))
    if record is None or status not in {"fresh", "keyMismatch"}:
        return _die(f"timed out waiting for {device_id} to become fresh or keyMismatch", 3)
    print(
        f"{device_id} status={status} "
        f"V={_fmt_null(record.get('batteryVoltageV'))} "
        f"I={_fmt_null(record.get('batteryCurrentA'))} "
        f"SoC={_fmt_null(record.get('stateOfChargePercent'))} "
        f"TTG={_fmt_null(record.get('timeToGoMinutes'))}"
    )
    if status == "keyMismatch":
        return 2
    return 0


def status_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="t28-victron-status")
    parser.add_argument("--latest", default=str(DEFAULT_LATEST))
    args = parser.parse_args(argv)
    path = Path(args.latest)
    if not path.is_file():
        return _die(f"{path} is not present (service disabled, not yet run, or /run was emptied)")
    payload = json.loads(path.read_text(encoding="utf-8"))
    devices = payload.get("devices")
    if not isinstance(devices, dict):
        return _die("latest.json is not a device map")
    print(f"updated {payload.get('updatedAtUtc')}")
    if not devices:
        print("no configured devices")
        return 0
    for device_id, record in devices.items():
        print(
            f"{device_id}: {record.get('status')} "
            f"V={record.get('batteryVoltageV')} I={record.get('batteryCurrentA')} "
            f"SoC={record.get('stateOfChargePercent')} TTG={record.get('timeToGoMinutes')}"
        )
    return 0


def check_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="t28-victron-check")
    parser.add_argument("--devices", default=str(DEFAULT_DEVICES))
    parser.add_argument("--keys", default=str(DEFAULT_KEYS))
    parser.add_argument("--unit", default="/etc/systemd/system/t28-victron-ble.service")
    parser.add_argument("--latest", default=str(DEFAULT_LATEST))
    parser.add_argument(
        "--adapter-state",
        default="",
        help="UP, DOWN, or empty to probe hci0. Tests pass this; operators usually omit it.",
    )
    parser.add_argument(
        "--service-enabled",
        default="",
        help="yes, no, or empty to probe systemd. Empty treats DOWN as INFO.",
    )
    args = parser.parse_args(argv)
    failures = 0

    def pass_(message: str) -> None:
        print(f"PASS  {message}")

    def fail(message: str) -> None:
        nonlocal failures
        print(f"FAIL  {message}")
        failures += 1

    unit = Path(args.unit)
    if unit.is_file():
        text = unit.read_text(encoding="utf-8")
        if "IPAddressDeny=any" in text:
            pass_("unit denies IP networking")
        else:
            fail("unit does not deny IP networking")
        if any(token in text.lower() for token in ("aes", "advertisement-key", "0xkey")):
            fail("unit file looks like it contains a secret")
        else:
            pass_("unit file has no key material")
        if "StartDiscovery" in text or "systemctl restart bluetooth" in text:
            fail("unit must not restart bluetoothd")
        if "AdvertisementMonitor" in text:
            fail("unit must not require AdvertisementMonitor1")
        if "CAP_NET_RAW" in text and "AF_BLUETOOTH" in text:
            pass_("unit allows HCI/mgmt passive scan capabilities")
        if "WantedBy" in text:
            pass_("unit has an [Install] section but installer must not enable it")
    else:
        fail(f"unit not installed: {unit}")

    try:
        reading = decode_battery_monitor(GOLDEN_ADVERTISEMENT, GOLDEN_KEY)
    except Exception as exc:
        fail(f"decoder self-test failed: {exc}")
    else:
        if reading.battery_voltage_v == 12.53 and reading.time_to_go_minutes is None:
            pass_("decoder self-test matches public Battery Monitor vector")
        else:
            fail("decoder self-test produced unexpected values")

    if Path(args.devices).is_file():
        try:
            load_service_config(Path(args.devices), Path(args.keys))
            pass_("devices.json and keys validate")
        except ConfigError as exc:
            fail(f"configured but invalid: {exc}")
        keys = Path(args.keys)
        if keys.is_file():
            mode = stat.S_IMODE(keys.stat().st_mode)
            if mode == 0o640 or mode == 0o600:
                pass_(f"keys file mode is {mode:03o}")
            else:
                fail(f"keys file mode is {mode:03o}, expected 0640")
    else:
        print("INFO  no devices.json; service must stay disabled")

    latest = Path(args.latest)
    if latest.is_file():
        payload = json.loads(latest.read_text(encoding="utf-8"))
        if isinstance(payload.get("devices"), dict):
            pass_("latest.json is a device map")
        else:
            fail("latest.json must be a map keyed by logical device id")
        dumped = json.dumps(payload).lower()
        if "advertisementkey" in dumped or "rawadvertisement" in dumped:
            fail("latest.json contains forbidden raw advertisement or key fields")

    adapter = (args.adapter_state or _probe_hci0_state()).strip().upper()
    enabled = _service_is_enabled(args.service_enabled)
    if adapter == "DOWN":
        if enabled is True:
            fail("hci0 is DOWN while t28-victron-ble is enabled; run t28-victron-configure --enable")
        else:
            print("INFO  hci0 is DOWN (expected on demo-receiver until t28-victron-configure --enable)")
    elif adapter == "UP":
        pass_("hci0 is UP")
    else:
        print("INFO  hci0 state was not probed (no hciconfig / --adapter-state)")
    return 1 if failures else 0


def _atomic_private(path: Path, payload: dict, mode: int) -> None:
    temporary = path.with_name(path.name + ".new")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.chmod(temporary, mode)
    temporary.replace(path)
    os.chmod(path, mode)


def _group_id(name: str) -> int:
    import grp

    try:
        return grp.getgrnam(name).gr_gid
    except KeyError:
        return 0


def _probe_hci0_state() -> str:
    """Return UP, DOWN, or unknown. Never raise; this is an operator hint."""
    import re
    import subprocess

    try:
        completed = subprocess.run(
            ["hciconfig", "hci0"],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return "unknown"
    text = f"{completed.stdout} {completed.stderr}"
    if re.search(r"\bUP\b", text):
        return "UP"
    if re.search(r"\bDOWN\b", text):
        return "DOWN"
    return "unknown"


def _service_is_enabled(explicit: str) -> bool | None:
    value = (explicit or "").strip().lower()
    if value in {"yes", "true", "1", "enabled"}:
        return True
    if value in {"no", "false", "0", "disabled"}:
        return False
    import subprocess

    try:
        completed = subprocess.run(
            ["systemctl", "is-enabled", "t28-victron-ble.service"],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None
    return completed.returncode == 0
