"""Operator tool for fleet MQTT config-sync. Never print passwords or keys."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from . import SCHEMA_VERSION
from .config_sync import DEFAULT_SYNC, SyncConfig, assert_enable_allowed, load_sync_config
from .publish import DEFAULT_CREDENTIALS, PublishConfigError
from .topics import CONFIG_TOPIC, OVERLAY_BROKER_DEFAULT

SYNC_UNIT = "t28-victron-config-sync.service"
EXAMPLE_BROKER = "mqtt://127.0.0.1:1883"


def _die(message: str, code: int = 1) -> int:
    print(f"ERROR: {message}", file=sys.stderr)
    return code


def config_sync_configure_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="t28-victron-config-sync-configure")
    parser.add_argument("--config", default=str(DEFAULT_SYNC))
    parser.add_argument("--credentials", default=str(DEFAULT_CREDENTIALS))
    parser.add_argument("--broker", default="")
    parser.add_argument("--topic", default="")
    parser.add_argument("--overlay", action="store_true", help=f"set broker {OVERLAY_BROKER_DEFAULT} and {CONFIG_TOPIC}")
    parser.add_argument("--client-id", default="")
    parser.add_argument("--tls", action="store_true")
    parser.add_argument("--no-tls", action="store_true")
    parser.add_argument("--show", action="store_true")
    parser.add_argument("--enable", action="store_true")
    parser.add_argument("--disable", action="store_true")
    args = parser.parse_args(argv)
    if os.geteuid() != 0:
        return _die("run t28-victron-config-sync-configure with sudo")
    if args.enable and args.disable:
        return _die("use only one of --enable or --disable")
    config_path = Path(args.config)
    credentials_path = Path(args.credentials)
    show_only = (
        args.show
        and not args.broker
        and not args.topic
        and not args.overlay
        and not args.client_id
        and not args.tls
        and not args.no_tls
        and not args.enable
        and not args.disable
    )
    if show_only:
        return _show_sync(config_path, credentials_path)
    try:
        payload = _read_or_default(config_path)
        if args.overlay:
            payload["broker"] = args.broker.strip() or OVERLAY_BROKER_DEFAULT
            payload["topic"] = args.topic or CONFIG_TOPIC
        if args.broker:
            payload["broker"] = args.broker.strip()
        if args.topic:
            payload["topic"] = args.topic
        if args.client_id:
            payload["clientId"] = args.client_id
        if args.tls:
            payload["tls"] = True
        if args.no_tls:
            payload["tls"] = False
        payload["schemaVersion"] = SCHEMA_VERSION
        payload.pop("username", None)
        payload.pop("password", None)
        if args.disable:
            payload["enabled"] = False
            _atomic_write(config_path, payload, 0o644)
        elif args.enable:
            payload["enabled"] = True
            _atomic_write(config_path, payload, 0o644)
            loaded = load_sync_config(config_path, credentials_path=credentials_path)
            assert_enable_allowed(loaded)
        else:
            payload.setdefault("enabled", False)
            _atomic_write(config_path, payload, 0o644)
        os.chmod(config_path, 0o644)
    except PublishConfigError as exc:
        _revert_enabled(config_path)
        return _die(str(exc))
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        return _die(str(exc))
    print(f"Wrote {config_path} (no secrets).")
    if args.show:
        shown = _show_sync(config_path, credentials_path)
        if shown != 0:
            return shown
    if args.disable:
        _systemctl(["disable", "--now", SYNC_UNIT])
        print(f"Disabled {SYNC_UNIT}. bluetoothd, t28-v2-receiver, and t28-victron-ble were not restarted.")
        return 0
    if args.enable:
        if not _systemctl(["enable", "--now", SYNC_UNIT]):
            return _die(f"failed to enable {SYNC_UNIT}")
        print(f"Enabled {SYNC_UNIT} only. bluetoothd, t28-v2-receiver, and t28-victron-ble were not restarted.")
    return 0


def _revert_enabled(path: Path) -> None:
    if not path.is_file():
        return
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return
    if isinstance(payload, dict) and payload.get("enabled"):
        payload["enabled"] = False
        try:
            _atomic_write(path, payload, 0o644)
        except OSError:
            pass


def _read_or_default(path: Path) -> dict:
    if path.is_file():
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise PublishConfigError("config-sync.json must be a JSON object")
        return payload
    return {
        "schemaVersion": SCHEMA_VERSION,
        "enabled": False,
        "broker": EXAMPLE_BROKER,
        "topic": CONFIG_TOPIC,
        "clientId": "t28-victron-config-sync",
        "qos": 1,
        "tls": False,
    }


def _show_sync(config_path: Path, credentials_path: Path) -> int:
    if not config_path.is_file():
        return _die(f"{config_path} is not present")
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return _die(f"invalid JSON in {config_path}: {exc}")
    if not isinstance(payload, dict):
        return _die("config-sync.json must be a JSON object")
    print(f"broker={payload.get('broker', '')}")
    print(f"topic={payload.get('topic', '')}")
    print(f"tls={'yes' if payload.get('tls') else 'no'}")
    print(f"credentialsPresent={'yes' if _credentials_present(credentials_path) else 'no'}")
    enabled, active = _unit_flags()
    print(f"unitEnabled={enabled}")
    print(f"unitActive={active}")
    return 0


def _credentials_present(path: Path) -> bool:
    if not path.is_file() or path.name == "keys.json":
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return isinstance(payload, dict) and bool(payload.get("username") or "password" in payload)


def _unit_flags() -> tuple[str, str]:
    enabled_proc = _run_systemctl(["is-enabled", SYNC_UNIT])
    active_proc = _run_systemctl(["is-active", SYNC_UNIT])
    enabled = "unknown" if enabled_proc is None else ("yes" if enabled_proc.returncode == 0 else "no")
    active = "unknown" if active_proc is None else ("yes" if active_proc.returncode == 0 else "no")
    return enabled, active


def _systemctl(argv: list[str]) -> bool:
    completed = _run_systemctl(argv)
    return completed is not None and completed.returncode == 0


def _run_systemctl(argv: list[str]) -> subprocess.CompletedProcess[str] | None:
    joined = " ".join(argv)
    forbidden = ("bluetooth.service", "bluetoothd", "t28-v2-receiver", "t28-victron-ble.service")
    if any(token in joined for token in forbidden):
        raise RuntimeError("config-sync-configure must not touch bluetoothd, t28-v2-receiver, or t28-victron-ble")
    if SYNC_UNIT not in argv:
        raise RuntimeError(f"config-sync-configure may only touch {SYNC_UNIT}")
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


def _atomic_write(path: Path, payload: dict, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".new")
    safe = {key: value for key, value in payload.items() if key not in {"password", "username"}}
    temporary.write_text(json.dumps(safe, indent=2) + "\n", encoding="utf-8")
    os.chmod(temporary, mode)
    temporary.replace(path)
    os.chmod(path, mode)
