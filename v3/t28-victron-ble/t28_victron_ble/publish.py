"""Optional MQTT publisher for /run/t28-victron/latest.json.

This process must not open HCI, must not load /etc/t28-victron/keys.json, and
must not log credentials. It is unrelated to T28.V2.Server.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

from .heard import iter_heard_publications
from .models import format_utc, utc_now
from .sanitize import is_forbidden_field, sanitize_payload
from .topics import HEARD_TOPIC_TEMPLATE, expand_topic as expand_mqtt_topic

LOGGER = logging.getLogger("t28-victron-publish")

DEFAULT_PUBLISH = Path("/etc/t28-victron/publish.json")
DEFAULT_CREDENTIALS = Path("/etc/t28-victron/publish-credentials.json")
DEFAULT_LATEST = Path("/run/t28-victron/latest.json")
DEFAULT_DEVICES = Path("/etc/t28-victron/devices.json")
FORBIDDEN_KEYS_PATH = Path("/etc/t28-victron/keys.json")

PLACEHOLDER_HOSTS = frozenset({"127.0.0.1", "localhost", "::1", "0.0.0.0"})
SOURCE = "t28-victron-ble"
DEFAULT_TOPIC = HEARD_TOPIC_TEMPLATE
# Overlay default is allowed. It is not a placeholder and may be overridden.

class PublishConfigError(ValueError):
    """Publisher configuration is missing or invalid."""


@dataclass(frozen=True)
class PublishCredentials:
    username: str = ""
    password: str = ""


@dataclass(frozen=True)
class PublishConfig:
    enabled: bool
    broker: str
    topic: str
    client_id: str
    qos: int
    retain: bool
    interval_seconds: float
    tls: bool
    host: str
    port: int
    latest_path: Path
    credentials_path: Path


def configure_logging() -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
    handler.flush = sys.stderr.flush  # type: ignore[method-assign]
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)


def parse_broker(url: str) -> tuple[str, int, bool]:
    text = str(url or "").strip()
    if not text:
        raise PublishConfigError("broker is required")
    parsed = urlparse(text if "://" in text else f"mqtt://{text}")
    host = parsed.hostname or ""
    if not host:
        raise PublishConfigError("broker host is required")
    tls = parsed.scheme in {"mqtts", "ssl", "tls"}
    port = parsed.port or (8883 if tls else 1883)
    return host, port, tls


def is_placeholder_broker(url: str) -> bool:
    try:
        host, _port, _tls = parse_broker(url)
    except PublishConfigError:
        return True
    return host.lower() in PLACEHOLDER_HOSTS


def expand_topic(template: str, hostname: str, device_id: str | None = None) -> str:
    return expand_mqtt_topic(template, hostname, device_id)


def load_credentials(path: Path) -> Optional[PublishCredentials]:
    if path.name == "keys.json" or path == FORBIDDEN_KEYS_PATH:
        raise PublishConfigError("publisher must not load keys.json")
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise PublishConfigError(f"invalid JSON in {path}") from exc
    if not isinstance(payload, dict):
        raise PublishConfigError("credentials file must be a JSON object")
    username = payload.get("username", "")
    password = payload.get("password", "")
    if username is None:
        username = ""
    if password is None:
        password = ""
    if not isinstance(username, str) or not isinstance(password, str):
        raise PublishConfigError("credentials username/password must be strings")
    if not username and not password:
        return None
    return PublishCredentials(username=username, password=password)


def load_publish_config(
    path: Path = DEFAULT_PUBLISH,
    *,
    credentials_path: Optional[Path] = None,
    latest_path: Optional[Path] = None,
) -> PublishConfig:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise PublishConfigError(f"missing {path}") from exc
    except json.JSONDecodeError as exc:
        raise PublishConfigError(f"invalid JSON in {path}") from exc
    if not isinstance(payload, dict):
        raise PublishConfigError("publish.json must be a JSON object")
    if "password" in payload or "username" in payload:
        raise PublishConfigError("do not put MQTT credentials in publish.json")
    broker = str(payload.get("broker", "")).strip()
    host, port, scheme_tls = parse_broker(broker)
    tls = bool(payload.get("tls", False)) or scheme_tls
    qos = int(payload.get("qos", 1))
    if qos not in {0, 1, 2}:
        raise PublishConfigError("qos must be 0, 1, or 2")
    interval = float(payload.get("intervalSeconds", 5))
    if interval < 1 or interval > 300:
        raise PublishConfigError("intervalSeconds must be between 1 and 300")
    creds = Path(credentials_path or payload.get("credentialsPath") or DEFAULT_CREDENTIALS)
    if creds.name == "keys.json":
        raise PublishConfigError("publisher must not load keys.json")
    return PublishConfig(
        enabled=bool(payload.get("enabled", False)),
        broker=broker,
        topic=str(payload.get("topic") or HEARD_TOPIC_TEMPLATE),
        client_id=str(payload.get("clientId") or "t28-victron-publish"),
        qos=qos,
        retain=bool(payload.get("retain", True)),
        interval_seconds=interval,
        tls=tls,
        host=host,
        port=port,
        latest_path=Path(latest_path or payload.get("latestPath") or DEFAULT_LATEST),
        credentials_path=creds,
    )


def assert_enable_allowed(config: PublishConfig) -> None:
    if not config.broker or is_placeholder_broker(config.broker):
        raise PublishConfigError("broker must be set to a non-localhost host before --enable")
    if "password" in config.broker.lower() or "username" in config.broker.lower():
        raise PublishConfigError("do not put credentials in the broker URL")
    if config.tls:
        load_credentials(config.credentials_path)


def read_latest_map(path: Path) -> Optional[dict]:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def build_publish_payload(latest: dict, *, hostname: str, published_at: Optional[str] = None) -> dict:
    cleaned = sanitize_payload(latest)
    if not isinstance(cleaned, dict):
        cleaned = {}
    cleaned["source"] = SOURCE
    cleaned["host"] = hostname
    cleaned["publishedAtUtc"] = published_at or format_utc(utc_now())
    return cleaned


class PahoMqttTransport:
    """Thin paho-mqtt wrapper. Imported lazily so sanitizer tests need no wheel."""

    def __init__(self, config: PublishConfig, credentials: Optional[PublishCredentials]) -> None:
        try:
            import paho.mqtt.client as mqtt
        except ImportError as exc:
            raise PublishConfigError("paho-mqtt is not installed in the victron venv") from exc
        kwargs: dict[str, Any] = {"client_id": config.client_id}
        callback_api = getattr(mqtt, "CallbackAPIVersion", None)
        if callback_api is not None:
            kwargs["callback_api_version"] = callback_api.VERSION2
        self._client = mqtt.Client(**kwargs)
        if credentials is not None and (credentials.username or credentials.password):
            self._client.username_pw_set(credentials.username, credentials.password or None)
        if config.tls:
            self._client.tls_set()
        self._host = config.host
        self._port = config.port
        self._subscriptions: dict[str, int] = {}
        self._on_connect_hook: Optional[Any] = None
        self._started = False
        reconnect_delay_set = getattr(self._client, "reconnect_delay_set", None)
        if callable(reconnect_delay_set):
            reconnect_delay_set(min_delay=1, max_delay=120)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect

    def set_on_connect_hook(self, hook: Any) -> None:
        self._on_connect_hook = hook

    def _reason_text(self, *args: Any) -> str:
        for item in args:
            rc = getattr(item, "value", item)
            if isinstance(rc, int) or hasattr(item, "is_failure"):
                return str(item)
        return "unknown"

    def _on_connect(self, _client: Any, _userdata: Any, *args: Any) -> None:
        LOGGER.info("mqtt connected rc=%s", self._reason_text(*args))
        for topic, qos in list(self._subscriptions.items()):
            self._client.subscribe(topic, qos=qos)
            LOGGER.info("resubscribed %s", topic)
        hook = self._on_connect_hook
        if callable(hook):
            try:
                hook()
            except Exception as exc:
                LOGGER.error("mqtt on_connect hook failed: %s", exc)

    def _on_disconnect(self, _client: Any, _userdata: Any, *args: Any) -> None:
        LOGGER.warning("mqtt disconnected rc=%s; will reconnect", self._reason_text(*args))

    def connect(self) -> None:
        if not self._started:
            self._client.connect(self._host, self._port, keepalive=60)
            self._client.loop_start()
            self._started = True

    def publish(self, topic: str, payload: str, *, qos: int, retain: bool) -> None:
        info = self._client.publish(topic, payload, qos=qos, retain=retain)
        wait = getattr(info, "wait_for_publish", None)
        if callable(wait):
            wait(timeout=10)

    def subscribe(self, topic: str, on_message: Any, qos: int = 1) -> None:
        def _cb(_client: Any, _userdata: Any, message: Any) -> None:
            payload = getattr(message, "payload", b"")
            if isinstance(payload, bytes):
                text = payload.decode("utf-8", errors="replace")
            else:
                text = str(payload)
            on_message(getattr(message, "topic", topic), text)

        self._client.on_message = _cb
        self._subscriptions[topic] = qos
        self._client.subscribe(topic, qos=qos)

    def close(self) -> None:
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception:
            pass
        self._started = False


def _refuse_keys_json() -> None:
    if os.environ.get("CREDENTIALS_DIRECTORY"):
        LOGGER.warning("publisher ignores CREDENTIALS_DIRECTORY; it must not load BLE keys")


def _parse_min_interval_seconds(value: Any) -> int:
    if value is None or value == "":
        return 0
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    if parsed < 0:
        return 0
    if parsed > 86400:
        return 86400
    return parsed


def _devices_mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


def load_device_min_intervals(path: Path = DEFAULT_DEVICES) -> dict[str, int]:
    """Map device id and aliases to minIntervalSeconds. devices.json only."""
    mapping: dict[str, int] = {}
    if path.name == "keys.json" or path == FORBIDDEN_KEYS_PATH:
        raise PublishConfigError("publisher must not load keys.json")
    if not path.is_file():
        return mapping
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return mapping
    if not isinstance(payload, dict):
        return mapping
    for item in payload.get("devices") or []:
        if not isinstance(item, dict):
            continue
        device_id = str(item.get("id") or "").strip()
        if not device_id:
            continue
        interval = _parse_min_interval_seconds(item.get("minIntervalSeconds"))
        mapping[device_id] = interval
        for alias in item.get("aliases") or []:
            alias_id = str(alias or "").strip()
            if alias_id:
                mapping[alias_id] = interval
    return mapping


def run_forever(
    config: PublishConfig,
    *,
    transport: Any = None,
    hostname: Optional[str] = None,
    sleep: Any = time.sleep,
    should_continue: Any = None,
) -> int:
    _refuse_keys_json()
    if not config.enabled:
        LOGGER.error("publish.json has enabled=false; not publishing")
        return 0
    if is_placeholder_broker(config.broker):
        LOGGER.error("broker is still the localhost placeholder; refusing to publish")
        return 2
    host = hostname or socket.gethostname()
    credentials = load_credentials(config.credentials_path)
    client = transport
    connected = False
    missing_backoff = 1.0
    keep_going = should_continue or (lambda: True)
    last_bodies: dict[str, str] = {}
    last_sent: dict[str, float] = {}
    last_skip_log: dict[str, float] = {}
    devices_path = DEFAULT_DEVICES
    intervals = load_device_min_intervals(devices_path)
    intervals_mtime = _devices_mtime(devices_path)
    while keep_going():
        mtime = _devices_mtime(devices_path)
        if mtime != intervals_mtime:
            intervals = load_device_min_intervals(devices_path)
            intervals_mtime = mtime
            LOGGER.info("reloaded minIntervalSeconds for %s device ids", len(intervals))
        latest = read_latest_map(config.latest_path)
        if latest is None:
            LOGGER.info("latest.json not ready; skip publish and retry")
            sleep(missing_backoff)
            missing_backoff = min(30.0, missing_backoff * 2.0)
            continue
        missing_backoff = 1.0
        publications = iter_heard_publications(latest, hostname=host, topic_template=config.topic)
        try:
            if client is None:
                client = PahoMqttTransport(config, credentials)
            if not connected:
                client.connect()
                connected = True
            for topic, payload in publications:
                body = json.dumps(payload, separators=(",", ":"))
                if last_bodies.get(topic) == body:
                    continue
                device_id = str((payload or {}).get("deviceId") or "").strip()
                interval = intervals.get(device_id, 0) if device_id else 0
                now_mono = time.monotonic()
                if interval > 0 and device_id:
                    previous_sent = last_sent.get(device_id)
                    if previous_sent is not None and (now_mono - previous_sent) < interval:
                        previous_log = last_skip_log.get(device_id, 0.0)
                        if (now_mono - previous_log) >= 30.0:
                            LOGGER.info("skipping %s: minIntervalSeconds=%s not elapsed", topic, interval)
                            last_skip_log[device_id] = now_mono
                        continue
                client.publish(topic, body, qos=config.qos, retain=config.retain)
                last_bodies[topic] = body
                if device_id:
                    last_sent[device_id] = time.monotonic()
                LOGGER.info("published %s (%s bytes, retain=%s)", topic, len(body), config.retain)
        except Exception as exc:
            LOGGER.error("publish failed: %s", exc)
            connected = False
            if client is not None and hasattr(client, "close"):
                try:
                    client.close()
                except Exception:
                    pass
            if transport is None:
                client = None
            sleep(min(30.0, config.interval_seconds))
            continue
        sleep(config.interval_seconds)
    return 0


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    del argv
    try:
        config = load_publish_config()
        return run_forever(config)
    except PublishConfigError as exc:
        LOGGER.error("%s", exc)
        return 2
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        LOGGER.error("publisher failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
