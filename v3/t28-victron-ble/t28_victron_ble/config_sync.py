"""Subscribe to retained fleet config and apply it on the Pi.

This process is networked. It must not open HCI. It does not enable
t28-victron-ble if that unit is disabled. Never log key material.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from .config import ConfigError
from .fleet import apply_fleet_config
from .publish import (
    DEFAULT_CREDENTIALS,
    PahoMqttTransport,
    PublishConfig,
    PublishConfigError,
    is_placeholder_broker,
    load_credentials,
    parse_broker,
)
from .store import atomic_write_json
from .topics import CONFIG_TOPIC, OVERLAY_BROKER_DEFAULT

LOGGER = logging.getLogger("t28-victron-config-sync")

DEFAULT_SYNC = Path("/etc/t28-victron/config-sync.json")
DEFAULT_INCOMING = Path("/run/t28-victron/incoming-config.json")
HELPER = Path("/usr/local/sbin/t28-victron-apply-fleet-config")


@dataclass(frozen=True)
class SyncConfig:
    enabled: bool
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
    incoming_path: Path
    helper: Path


def configure_logging() -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
    handler.flush = sys.stderr.flush  # type: ignore[method-assign]
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)


def load_sync_config(
    path: Path = DEFAULT_SYNC,
    *,
    credentials_path: Optional[Path] = None,
) -> SyncConfig:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise PublishConfigError(f"missing {path}") from exc
    except json.JSONDecodeError as exc:
        raise PublishConfigError(f"invalid JSON in {path}") from exc
    if not isinstance(payload, dict):
        raise PublishConfigError("config-sync.json must be a JSON object")
    if "password" in payload or "username" in payload:
        raise PublishConfigError("do not put MQTT credentials in config-sync.json")
    broker = str(payload.get("broker", "")).strip()
    host, port, scheme_tls = parse_broker(broker)
    creds = Path(credentials_path or payload.get("credentialsPath") or DEFAULT_CREDENTIALS)
    if creds.name == "keys.json":
        raise PublishConfigError("config-sync must not load keys.json as credentials")
    qos = int(payload.get("qos", 1))
    if qos not in {0, 1, 2}:
        raise PublishConfigError("qos must be 0, 1, or 2")
    return SyncConfig(
        enabled=bool(payload.get("enabled", False)),
        broker=broker,
        topic=str(payload.get("topic") or CONFIG_TOPIC),
        client_id=str(payload.get("clientId") or "t28-victron-config-sync"),
        qos=qos,
        tls=bool(payload.get("tls", False)) or scheme_tls,
        host=host,
        port=port,
        credentials_path=creds,
        devices_path=Path(payload.get("devicesPath") or "/etc/t28-victron/devices.json"),
        keys_path=Path(payload.get("keysPath") or "/etc/t28-victron/keys.json"),
        incoming_path=Path(payload.get("incomingPath") or DEFAULT_INCOMING),
        helper=Path(payload.get("helper") or HELPER),
    )


def assert_enable_allowed(config: SyncConfig) -> None:
    if not config.broker or is_placeholder_broker(config.broker):
        raise PublishConfigError("broker must be set to a non-localhost host before --enable")
    if "password" in config.broker.lower() or "username" in config.broker.lower():
        raise PublishConfigError("do not put credentials in the broker URL")


def _as_publish_config(config: SyncConfig) -> PublishConfig:
    return PublishConfig(
        enabled=config.enabled,
        broker=config.broker,
        topic=config.topic,
        client_id=config.client_id,
        qos=config.qos,
        retain=True,
        interval_seconds=5,
        tls=config.tls,
        host=config.host,
        port=config.port,
        latest_path=Path("/run/t28-victron/latest.json"),
        credentials_path=config.credentials_path,
    )


def _shred(path: Path) -> None:
    try:
        if not path.is_file():
            return
        size = path.stat().st_size
        path.write_bytes(b"\x00" * size)
        path.unlink()
    except OSError:
        pass


def apply_via_helper_or_direct(
    payload: dict,
    config: SyncConfig,
    *,
    apply_direct: Optional[Any] = None,
) -> None:
    """Root helper is preferred so this unit can stay User=t28-victron."""
    if apply_direct is not None:
        apply_direct(payload)
        return
    if os.geteuid() == 0:
        apply_fleet_config(
            payload,
            devices_path=config.devices_path,
            keys_path=config.keys_path,
            restart_ble=True,
        )
        return
    incoming = config.incoming_path
    incoming.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(incoming, payload, mode=0o600)
    try:
        completed = subprocess.run(
            ["sudo", "-n", str(config.helper), str(incoming)],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as exc:
        _shred(incoming)
        raise ConfigError(f"fleet apply helper failed: {exc}") from exc
    if completed.returncode != 0:
        _shred(incoming)
        detail = (completed.stderr or completed.stdout or "").strip()
        raise ConfigError(f"fleet apply helper exited {completed.returncode}" + (f": {detail}" if detail else ""))
    LOGGER.info("%s", (completed.stdout or "").strip() or "fleet apply helper succeeded")


def handle_config_message(
    topic: str,
    body: str,
    config: SyncConfig,
    *,
    apply_direct: Optional[Any] = None,
) -> None:
    if topic != config.topic and not topic.startswith("demo/power/config/"):
        LOGGER.info("ignored unexpected topic")
        return
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        LOGGER.error("fleet config payload was not JSON")
        return
    try:
        apply_via_helper_or_direct(payload, config, apply_direct=apply_direct)
    except (ConfigError, PublishConfigError) as exc:
        LOGGER.error("fleet config rejected: %s", exc)


def run_forever(
    config: SyncConfig,
    *,
    transport: Any = None,
    sleep: Any = time.sleep,
    should_continue: Any = None,
    apply_direct: Optional[Any] = None,
) -> int:
    if not config.enabled:
        LOGGER.error("config-sync.json has enabled=false; not subscribing")
        return 0
    if is_placeholder_broker(config.broker):
        LOGGER.error("broker is still the localhost placeholder; refusing to subscribe")
        return 2
    credentials = load_credentials(config.credentials_path)
    client = transport
    connected = False
    keep_going = should_continue or (lambda: True)
    while keep_going():
        try:
            if client is None:
                client = PahoMqttTransport(_as_publish_config(config), credentials)
            if not connected:
                client.connect()
                client.subscribe(
                    config.topic,
                    lambda topic, body: handle_config_message(
                        topic, body, config, apply_direct=apply_direct
                    ),
                    qos=config.qos,
                )
                connected = True
                LOGGER.info("subscribed %s (retained apply on connect, no poll)", config.topic)
            sleep(5)
        except Exception as exc:
            LOGGER.error("config-sync failed: %s", exc)
            connected = False
            if client is not None and hasattr(client, "close") and transport is None:
                try:
                    client.close()
                except Exception:
                    pass
                client = None
            sleep(5)
    return 0


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    del argv
    try:
        config = load_sync_config()
        return run_forever(config)
    except PublishConfigError as exc:
        LOGGER.error("%s", exc)
        return 2
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        LOGGER.error("config-sync failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
