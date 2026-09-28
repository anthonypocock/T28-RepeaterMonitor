"""Hub last-heard map plus a capped on-disk look-back. No charts, no CSV."""

from __future__ import annotations

import argparse
import json
import sqlite3
import logging
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, unquote, urlparse

from .config import ConfigError, is_valid_device_id, normalize_mac, parse_min_interval_seconds, parse_product_id
from .decoder import DecodeError, parse_hex_key
from .models import format_utc, utc_now
from .publish import (
    PahoMqttTransport,
    PublishConfig,
    PublishConfigError,
    is_placeholder_broker,
    load_credentials,
    sanitize_payload,
)
from .hub_config import (
    DEFAULT_DEVICES,
    DEFAULT_HUB,
    DEFAULT_HUB_CREDENTIALS,
    DEFAULT_KEYS,
    HubConfig,
    _existing_key_hex,
    _read_json,
    admin_devices_response,
    list_devices_public,
    load_hub_config,
    remove_device,
    write_device,
)
from .sanitize import is_forbidden_field
from .store import atomic_write_json
from .topics import HEARD_WILDCARD, parse_heard_topic

LOGGER = logging.getLogger("t28-victron-hub-live")

DEFAULT_HEARD = Path("/run/t28-victron/heard.json")
DEFAULT_HTML = Path("/usr/local/lib/t28-victron/hub-live.html")
DEFAULT_ADMIN_HTML = Path("/usr/local/lib/t28-victron/hub-admin.html")
DEFAULT_HISTORY_HTML = Path("/usr/local/lib/t28-victron/hub-history.html")
DEFAULT_HISTORY = Path("/var/lib/t28-victron/history.sqlite")
HISTORY_KEEP_SECONDS = 48 * 3600
HISTORY_MAX_BYTES = 8 * 1024 * 1024
HISTORY_MIN_INTERVAL_SECONDS = 10.0
HISTORY_QUERY_LIMIT = 400
STUB_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>T28 Victron last-heard</title>
<style>
body { font-family: sans-serif; background: #111; color: #eee; margin: 1.5rem; }
table { border-collapse: collapse; width: 100%; }
th, td { border-bottom: 1px solid #333; padding: 0.4rem 0.6rem; text-align: left; }
.stale { color: #f0a000; }
.fresh { color: #7dcea0; }
</style>
</head>
<body>
<h1>Victron last-heard</h1>
<p>Realtime only. No history. Polls /heard.json on this localhost.</p>
<p><a href="/victron/admin">Admin</a></p>
<table>
<thead><tr>
<th>device</th><th>V</th><th>A</th><th>SoC</th><th>heard by</th><th>RSSI</th><th>age</th><th>status</th>
</tr></thead>
<tbody id="rows"></tbody>
</table>
<script>
async function refresh() {
  const response = await fetch("/heard.json", {cache: "no-store"});
  const data = await response.json();
  const rows = data.devices || Object.values(data.topics || {});
  document.getElementById("rows").innerHTML = rows.map((item) => {
    const stale = item.stale ? "stale" : "fresh";
    return `<tr class="${stale}">
      <td>${item.deviceId || ""}</td>
      <td>${item.batteryVoltageV ?? ""}</td>
      <td>${item.batteryCurrentA ?? ""}</td>
      <td>${item.stateOfChargePercent ?? ""}</td>
      <td>${item.hostname || ""}</td>
      <td>${item.rssiDbm ?? ""}</td>
      <td>${item.ageSeconds ?? ""}</td>
      <td>${stale}</td>
    </tr>`;
  }).join("");
}
refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>
"""


def configure_logging() -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
    handler.flush = sys.stderr.flush  # type: ignore[method-assign]
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)


def _parse_observed(value: Any) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def decorate_heard(record: dict, *, now: Optional[datetime] = None, stale_after_seconds: int = 90) -> dict:
    cleaned = sanitize_payload(record)
    if not isinstance(cleaned, dict):
        cleaned = {}
    current = now or datetime.now(timezone.utc)
    observed = _parse_observed(cleaned.get("observedAtUtc"))
    age = None if observed is None else max(0.0, (current - observed).total_seconds())
    stale = age is None or age > stale_after_seconds
    status = str(cleaned.get("status") or ("stale" if stale else "unavailable"))
    if stale and status == "fresh":
        status = "stale"
    cleaned["ageSeconds"] = None if age is None else round(age, 1)
    cleaned["stale"] = stale
    cleaned["status"] = status
    return cleaned


def _age_number(value: Any) -> float:
    return float(value) if isinstance(value, (int, float)) else 1e9


def group_heard_devices(topics: dict[str, dict]) -> list[dict]:
    """One row per deviceId. Freshest host wins; others listed in alsoHeardBy."""
    grouped: dict[str, dict] = {}
    hosts_by_id: dict[str, list[dict]] = {}
    for record in topics.values():
        if not isinstance(record, dict):
            continue
        device_id = str(record.get("deviceId") or "").strip()
        if not device_id:
            continue
        host = {
            "hostname": str(record.get("hostname") or record.get("receiverId") or ""),
            "rssiDbm": record.get("rssiDbm"),
            "ageSeconds": record.get("ageSeconds"),
            "stale": bool(record.get("stale")),
            "status": str(record.get("status") or ""),
        }
        hosts_by_id.setdefault(device_id, []).append(host)
        current = grouped.get(device_id)
        if current is None:
            grouped[device_id] = dict(record)
            continue
        prefer_fresh = (not record.get("stale")) and current.get("stale")
        prefer_newer = _age_number(record.get("ageSeconds")) < _age_number(current.get("ageSeconds"))
        if prefer_fresh or (record.get("stale") == current.get("stale") and prefer_newer):
            grouped[device_id] = dict(record)
    rows: list[dict] = []
    for device_id, item in grouped.items():
        primary = str(item.get("hostname") or item.get("receiverId") or "")
        also = [
            host
            for host in hosts_by_id.get(device_id, [])
            if host.get("hostname") and host["hostname"] != primary
        ]
        also.sort(key=lambda host: _age_number(host.get("ageSeconds")))
        item["alsoHeardBy"] = also
        item["heardBy"] = primary
        extras = []
        for host in also:
            age = host.get("ageSeconds")
            label = str(host.get("hostname") or "")
            if not label:
                continue
            if isinstance(age, (int, float)):
                extras.append(f"{label} {int(age)}s")
            else:
                extras.append(label)
        item["heardByDisplay"] = primary if not extras else f"{primary} also: {', '.join(extras)}"
        rows.append(item)
    rows.sort(key=lambda item: str(item.get("deviceId") or ""))
    return rows


def devices_public_list(devices_path: Path, keys_path: Path) -> dict:
    devices_payload = _read_json(devices_path)
    keys_payload = _read_json(keys_path)
    raw_keys = keys_payload.get("keys")
    key_map: dict[str, str] = {}
    if isinstance(raw_keys, dict):
        for device_id, value in raw_keys.items():
            if not value:
                continue
            try:
                key_map[str(device_id)] = parse_hex_key(str(value)).hex()
            except DecodeError:
                continue
    rows = []
    for item in devices_payload.get("devices") or []:
        if not isinstance(item, dict):
            continue
        device_id = str(item.get("id") or "").strip()
        key_hex = key_map.get(device_id, "")
        try:
            min_interval = parse_min_interval_seconds(item.get("minIntervalSeconds"))
        except ConfigError:
            min_interval = 0
        rows.append(
            {
                "id": device_id,
                "address": item.get("address"),
                "product": item.get("product"),
                "productId": item.get("productId"),
                "minIntervalSeconds": min_interval,
                "hasKey": bool(key_hex),
                "keyPresent": bool(key_hex),
                "key": key_hex,
            }
        )
    return {
        "schemaVersion": 1,
        "revision": devices_payload.get("revision"),
        "updatedAtUtc": devices_payload.get("updatedAtUtc"),
        "devices": rows,
    }


def save_device_from_body(body: dict, devices_path: Path, keys_path: Path) -> dict:
    device_id = str(body.get("id") or "").strip()
    previous_id = str(body.get("previousId") or body.get("previous_id") or "").strip()
    if not device_id:
        raise ConfigError("device name is required")
    if not is_valid_device_id(device_id):
        raise ConfigError("device id must be a stable logical name")
    address = normalize_mac(str(body.get("address") or "").strip())
    product = str(body.get("product") or "").strip() or "SmartShunt 500A/50mV"
    product_id_text = str(body.get("productId") or body.get("product_id") or "").strip()
    product_id = parse_product_id(product_id_text) if product_id_text else None
    min_interval = parse_min_interval_seconds(body.get("minIntervalSeconds"))
    key_text = str(body.get("key") or "").strip()
    key_lookup = previous_id or device_id
    if key_text:
        key_hex = parse_hex_key(key_text).hex()
    else:
        existing = _existing_key_hex(keys_path, key_lookup) or _existing_key_hex(keys_path, device_id)
        if existing is None:
            raise ConfigError("no AES key provided and no existing key for this id")
        key_hex = existing
    aliases = None
    renamed = bool(previous_id and previous_id != device_id)
    if renamed:
        payload = _read_json(devices_path)
        aliases = []
        for item in payload.get("devices") or []:
            if isinstance(item, dict) and item.get("id") == previous_id:
                aliases = [str(a) for a in (item.get("aliases") or []) if str(a).strip()]
                break
        if previous_id not in aliases:
            aliases.append(previous_id)
    write_device(
        devices_path,
        keys_path,
        device_id=device_id,
        address=address,
        product=product,
        product_id=product_id,
        key_hex=key_hex,
        min_interval_seconds=min_interval,
        aliases=aliases,
    )
    if renamed:
        try:
            remove_device(devices_path, keys_path, previous_id)
        except ConfigError:
            pass
    return {"ok": True, "id": device_id, "hasKey": True, "renamedFrom": previous_id if renamed else None, "minIntervalSeconds": min_interval}




def device_name_map(devices_path: Path) -> dict[str, str]:
    """Map MQTT deviceId / previous names to the current admin name."""
    mapping: dict[str, str] = {}
    try:
        payload = _read_json(devices_path)
    except Exception:
        return mapping
    for item in payload.get("devices") or []:
        if not isinstance(item, dict):
            continue
        current = str(item.get("id") or "").strip()
        if not current:
            continue
        mapping[current] = current
        for alias in item.get("aliases") or []:
            alias_id = str(alias or "").strip()
            if alias_id:
                mapping[alias_id] = current
    return mapping


def overlay_device_names(topics: dict[str, dict], mapping: dict[str, str]) -> dict[str, dict]:
    overlaid: dict[str, dict] = {}
    for topic, record in topics.items():
        copied = dict(record)
        device_id = str(copied.get("deviceId") or "").strip()
        if device_id and device_id in mapping:
            copied["deviceId"] = mapping[device_id]
        overlaid[topic] = copied
    return overlaid


class HistoryStore:
    """48h / 8MB SQLite look-back on disk. Not tmpfs. No keys."""

    def __init__(
        self,
        path: Path = DEFAULT_HISTORY,
        *,
        keep_seconds: int = HISTORY_KEEP_SECONDS,
        max_bytes: int = HISTORY_MAX_BYTES,
        min_interval_seconds: float = HISTORY_MIN_INTERVAL_SECONDS,
        devices_path: Path = DEFAULT_DEVICES,
    ) -> None:
        self.path = path
        self.keep_seconds = keep_seconds
        self.max_bytes = max_bytes
        self.min_interval_seconds = min_interval_seconds
        self.devices_path = devices_path
        self._lock = threading.Lock()
        self._last: dict[tuple[str, str], float] = {}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        return conn

    def _init_db(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS samples (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts REAL NOT NULL,
                        device_id TEXT NOT NULL,
                        hostname TEXT NOT NULL DEFAULT '',
                        voltage REAL,
                        current REAL,
                        soc REAL,
                        rssi INTEGER,
                        observed_at TEXT
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS samples_device_ts ON samples(device_id, ts)"
                )
                conn.commit()
            finally:
                conn.close()
            try:
                self.path.chmod(0o640)
            except OSError:
                pass

    def _bytes_used(self) -> int:
        total = 0
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(self.path) + suffix) if suffix else self.path
            try:
                total += candidate.stat().st_size
            except OSError:
                continue
        return total

    def record(self, payload: dict, *, now: Optional[datetime] = None) -> None:
        cleaned = sanitize_payload(payload)
        if not isinstance(cleaned, dict):
            return
        device_id = str(cleaned.get("deviceId") or "").strip()
        if not device_id:
            return
        device_id = device_name_map(self.devices_path).get(device_id, device_id)
        hostname = str(cleaned.get("hostname") or cleaned.get("receiverId") or "").strip()
        current = now or utc_now()
        ts = current.timestamp()
        key = (device_id, hostname)
        with self._lock:
            last = self._last.get(key)
            if last is not None and (ts - last) < self.min_interval_seconds:
                return
            self._last[key] = ts
            observed = cleaned.get("observedAtUtc")
            conn = self._connect()
            try:
                conn.execute(
                    """
                    INSERT INTO samples (ts, device_id, hostname, voltage, current, soc, rssi, observed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ts,
                        device_id,
                        hostname,
                        cleaned.get("batteryVoltageV"),
                        cleaned.get("batteryCurrentA"),
                        cleaned.get("stateOfChargePercent"),
                        cleaned.get("rssiDbm"),
                        None if observed is None else str(observed),
                    ),
                )
                conn.commit()
            finally:
                conn.close()
        self.prune(now=current)

    def prune(self, *, now: Optional[datetime] = None) -> None:
        cutoff = (now or utc_now()).timestamp() - self.keep_seconds
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("DELETE FROM samples WHERE ts < ?", (cutoff,))
                conn.commit()
                used = self._bytes_used()
                if used > self.max_bytes:
                    row = conn.execute("SELECT COUNT(*) FROM samples").fetchone()
                    count = int(row[0] if row else 0)
                    if count > 0:
                        drop = max(1, int(count * 0.2))
                        conn.execute(
                            "DELETE FROM samples WHERE id IN (SELECT id FROM samples ORDER BY ts ASC LIMIT ?)",
                            (drop,),
                        )
                        conn.commit()
                if used > self.max_bytes:
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            finally:
                conn.close()

    def rename(self, previous_id: str, new_id: str) -> None:
        old = str(previous_id or "").strip()
        new = str(new_id or "").strip()
        if not old or not new or old == new:
            return
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("UPDATE samples SET device_id = ? WHERE device_id = ?", (new, old))
                conn.commit()
            finally:
                conn.close()
            self._last = {
                ((new if device == old else device), host): ts
                for (device, host), ts in self._last.items()
            }

    def query(self, *, device_id: str = "", limit: int = HISTORY_QUERY_LIMIT) -> dict:
        wanted = str(device_id or "").strip()
        cap = max(1, min(int(limit), HISTORY_QUERY_LIMIT))
        cutoff = utc_now().timestamp() - self.keep_seconds
        sql = """
            SELECT ts, device_id, hostname, voltage, current, soc, rssi, observed_at
            FROM samples
            WHERE ts >= ?
        """
        args: list[Any] = [cutoff]
        if wanted:
            sql += " AND device_id = ?"
            args.append(wanted)
        sql += " ORDER BY ts DESC LIMIT ?"
        args.append(cap)
        with self._lock:
            conn = self._connect()
            try:
                rows = conn.execute(sql, args).fetchall()
            finally:
                conn.close()
            used = self._bytes_used()
        mapping = device_name_map(self.devices_path)
        samples = []
        for ts, device, hostname, voltage, current, soc, rssi, observed in rows:
            samples.append(
                {
                    "observedAtUtc": observed or format_utc(datetime.fromtimestamp(ts, tz=timezone.utc)),
                    "deviceId": mapping.get(str(device or ""), device),
                    "hostname": hostname,
                    "batteryVoltageV": voltage,
                    "batteryCurrentA": current,
                    "stateOfChargePercent": soc,
                    "rssiDbm": rssi,
                }
            )
        return {
            "schemaVersion": 1,
            "keepHours": int(self.keep_seconds / 3600),
            "maxBytes": self.max_bytes,
            "bytesUsed": used,
            "samples": samples,
        }


class HeardMap:
    def __init__(self, path: Path, *, stale_after_seconds: int = 90, devices_path: Path = DEFAULT_DEVICES) -> None:
        self.path = path
        self.stale_after_seconds = stale_after_seconds
        self.devices_path = devices_path
        self._topics: dict[str, dict] = {}
        self._lock = threading.Lock()

    def upsert(self, topic: str, payload: dict, *, now: Optional[datetime] = None) -> dict:
        parsed = parse_heard_topic(topic)
        cleaned = decorate_heard(payload, now=now, stale_after_seconds=self.stale_after_seconds)
        if parsed is not None:
            cleaned.setdefault("hostname", parsed[0])
            cleaned.setdefault("receiverId", parsed[0])
            cleaned.setdefault("deviceId", parsed[1])
        with self._lock:
            self._topics[topic] = cleaned
            return self.flush(now=now)

    def refresh(self, *, now: Optional[datetime] = None) -> dict:
        with self._lock:
            for topic, record in list(self._topics.items()):
                self._topics[topic] = decorate_heard(record, now=now, stale_after_seconds=self.stale_after_seconds)
            return self.flush(now=now)

    def snapshot(self, *, now: Optional[datetime] = None) -> dict:
        current = now or utc_now()
        mapping = device_name_map(self.devices_path)
        topics = overlay_device_names(self._topics, mapping)
        return {
            "schemaVersion": 1,
            "updatedAtUtc": format_utc(current),
            "staleAfterSeconds": self.stale_after_seconds,
            "topics": topics,
            "devices": group_heard_devices(topics),
        }

    def flush(self, *, now: Optional[datetime] = None) -> dict:
        payload = self.snapshot(now=now)
        atomic_write_json(self.path, payload, mode=0o640)
        return payload

    def drop_device(self, device_id: str) -> None:
        wanted = str(device_id or "").strip()
        if not wanted:
            return
        with self._lock:
            for topic, record in list(self._topics.items()):
                if str((record or {}).get("deviceId") or "") == wanted:
                    self._topics.pop(topic, None)
            self.flush()

    def as_dict(self) -> dict:
        with self._lock:
            return self.snapshot()


def _html_bytes(html_path: Optional[Path], fallback: Path, stub: str) -> bytes:
    for candidate in (html_path, fallback):
        if candidate is not None and candidate.is_file():
            return candidate.read_bytes()
    return stub.encode("utf-8")


def make_handler(
    store: HeardMap,
    html_path: Optional[Path] = None,
    *,
    admin_html_path: Optional[Path] = None,
    history_html_path: Optional[Path] = None,
    history: Optional[HistoryStore] = None,
    devices_path: Path = DEFAULT_DEVICES,
    keys_path: Path = DEFAULT_KEYS,
):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            return None

        def _send(self, code: int, body: bytes, content_type: str, *, cache: str | None = None) -> None:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            if cache:
                self.send_header("Cache-Control", cache)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _json(self, code: int, payload: dict) -> None:
            dumped = json.dumps(payload)
            lowered = dumped.lower()
            if "advertisementkey" in lowered:
                payload = {"error": "response contained forbidden fields"}
                code = 500
                dumped = json.dumps(payload)
            body = dumped.encode("utf-8")
            self._send(code, body, "application/json", cache="no-store")

        def _read_json_body(self) -> dict:
            length = int(self.headers.get("Content-Length") or 0)
            if length > 8192:
                raise ConfigError("payload too large")
            raw = self.rfile.read(length) if length else b"{}"
            try:
                payload = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ConfigError("body must be JSON") from exc
            if not isinstance(payload, dict):
                raise ConfigError("body must be a JSON object")
            return payload

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            if path in {"/", "/victron", "/index.html"}:
                body = _html_bytes(html_path, DEFAULT_HTML, STUB_HTML)
                self._send(200, body, "text/html; charset=utf-8")
                return
            if path in {"/admin", "/victron/admin"}:
                body = _html_bytes(admin_html_path, DEFAULT_ADMIN_HTML, STUB_HTML)
                self._send(200, body, "text/html; charset=utf-8")
                return
            if path in {"/history", "/victron/history"}:
                body = _html_bytes(history_html_path, DEFAULT_HISTORY_HTML, STUB_HTML)
                self._send(200, body, "text/html; charset=utf-8")
                return
            if path in {"/history.json", "/victron/history.json"}:
                device = (parse_qs(parsed.query).get("device") or [""])[0]
                if history is None:
                    self._json(200, {"schemaVersion": 1, "samples": [], "bytesUsed": 0, "keepHours": 48})
                    return
                try:
                    self._json(200, history.query(device_id=device))
                except OSError as exc:
                    self._json(500, {"error": str(exc)})
                return
            if path in {"/heard.json", "/victron/heard.json"}:
                self._json(200, store.as_dict())
                return
            if path in {"/devices.json", "/victron/devices.json"}:
                try:
                    self._json(200, devices_public_list(devices_path, keys_path))
                except (ConfigError, OSError) as exc:
                    self._json(500, {"error": str(exc)})
                return
            if path in {"/keys.json", "/victron/keys.json"}:
                self._json(403, {"error": "forbidden"})
                return
            self.send_error(404, "not found")

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            if path not in {"/devices", "/victron/devices"}:
                self.send_error(404, "not found")
                return
            try:
                body = self._read_json_body()
                result = save_device_from_body(body, devices_path, keys_path)
                previous_id = str(body.get("previousId") or body.get("previous_id") or "").strip()
                new_id = str(result.get("id") or "").strip()
                if previous_id and new_id and previous_id != new_id:
                    store.drop_device(previous_id)
                    if history is not None:
                        history.rename(previous_id, new_id)
                self._json(200, admin_devices_response(list_devices_public(devices_path, keys_path)))
            except (ConfigError, DecodeError, OSError) as exc:
                self._json(400, {"error": str(exc)})

        def do_DELETE(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            prefix = None
            for candidate in ("/victron/devices/", "/devices/"):
                if path.startswith(candidate.rstrip("/") + "/") or path.startswith(candidate):
                    prefix = candidate
                    break
            if prefix is None:
                self.send_error(404, "not found")
                return
            device_id = unquote(path[len(prefix.rstrip("/")) :].lstrip("/")).strip()
            if not device_id or "/" in device_id:
                self._json(400, {"error": "device id is required"})
                return
            try:
                remove_device(devices_path, keys_path, device_id)
                store.drop_device(device_id)
                self._json(200, admin_devices_response(list_devices_public(devices_path, keys_path)))
            except (ConfigError, OSError) as exc:
                self._json(400, {"error": str(exc)})

    return Handler


def start_localhost_http(
    store: HeardMap,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    html_path: Optional[Path] = None,
    admin_html_path: Optional[Path] = None,
    history_html_path: Optional[Path] = None,
    history: Optional[HistoryStore] = None,
    devices_path: Path = DEFAULT_DEVICES,
    keys_path: Path = DEFAULT_KEYS,
) -> ThreadingHTTPServer:
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise PublishConfigError("hub live HTTP must bind localhost only")
    server = ThreadingHTTPServer(
        (host, port),
        make_handler(
            store,
            html_path,
            admin_html_path=admin_html_path,
            history_html_path=history_html_path,
            history=history,
            devices_path=devices_path,
            keys_path=keys_path,
        ),
    )
    thread = threading.Thread(target=server.serve_forever, name="t28-victron-hub-http", daemon=True)
    thread.start()
    LOGGER.info("localhost live page on http://%s:%s/ (Caddy /victron can reverse-proxy later)", host, port)
    return server


def handle_heard_message(store: HeardMap, topic: str, body: str, history: Optional[HistoryStore] = None) -> None:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        LOGGER.error("heard payload was not JSON")
        return
    if not isinstance(payload, dict):
        return
    dumped = json.dumps(payload).lower()
    if "advertisementkey" in dumped or '"keys"' in dumped:
        LOGGER.error("heard payload contained forbidden fields; dropped")
        return
    store.upsert(topic, payload)
    if history is not None:
        try:
            parsed = parse_heard_topic(topic)
            sample = dict(payload)
            if parsed is not None:
                sample.setdefault("hostname", parsed[0])
                sample.setdefault("receiverId", parsed[0])
                sample.setdefault("deviceId", parsed[1])
            history.record(sample)
        except Exception as exc:
            LOGGER.error("history record failed: %s", exc)


def run_forever(
    config: HubConfig,
    *,
    store: Optional[HeardMap] = None,
    transport: Any = None,
    sleep: Any = time.sleep,
    should_continue: Any = None,
    heard_path: Path = DEFAULT_HEARD,
    http_host: str = "127.0.0.1",
    http_port: int = 8765,
    enable_http: bool = True,
    html_path: Optional[Path] = None,
    admin_html_path: Optional[Path] = None,
    history_path: Path = DEFAULT_HISTORY,
) -> int:
    if is_placeholder_broker(config.broker):
        LOGGER.error("hub broker is still the localhost placeholder")
        return 2
    live = store or HeardMap(heard_path, stale_after_seconds=config.stale_after_seconds, devices_path=config.devices_path)
    live.flush()
    history = HistoryStore(history_path, devices_path=config.devices_path)
    httpd = None
    if enable_http:
        httpd = start_localhost_http(
            live,
            host=http_host,
            port=http_port,
            html_path=html_path,
            admin_html_path=admin_html_path,
            history=history,
            devices_path=config.devices_path,
            keys_path=config.keys_path,
        )
    credentials = load_credentials(config.credentials_path)
    publish_config = PublishConfig(
        enabled=True,
        broker=config.broker,
        topic=HEARD_WILDCARD,
        client_id=str(config.client_id).replace("hub-config", "hub-live"),
        qos=config.qos,
        retain=False,
        interval_seconds=5,
        tls=config.tls,
        host=config.host,
        port=config.port,
        latest_path=heard_path,
        credentials_path=config.credentials_path,
    )
    client = transport
    connected = False
    keep_going = should_continue or (lambda: True)
    try:
        while keep_going():
            try:
                if client is None:
                    client = PahoMqttTransport(publish_config, credentials)
                if not connected:
                    client.connect()
                    client.subscribe(
                        HEARD_WILDCARD,
                        lambda topic, body: handle_heard_message(live, topic, body, history),
                        qos=config.qos,
                    )
                    connected = True
                    LOGGER.info("subscribed %s (latest overwrite only; auto-reconnect resubscribes)", HEARD_WILDCARD)
                live.refresh()
                sleep(2)
            except Exception as exc:
                LOGGER.error("hub-live failed: %s", exc)
                connected = False
                if client is not None and hasattr(client, "close") and transport is None:
                    try:
                        client.close()
                    except Exception:
                        pass
                    client = None
                sleep(5)
    finally:
        if httpd is not None:
            httpd.shutdown()
    return 0


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = argparse.ArgumentParser(prog="t28-victron-hub-live")
    parser.add_argument("--hub", default=str(DEFAULT_HUB))
    parser.add_argument("--heard", default=str(DEFAULT_HEARD))
    parser.add_argument("--http-port", type=int, default=8765)
    parser.add_argument("--no-http", action="store_true")
    args = parser.parse_args(argv)
    try:
        config = load_hub_config(Path(args.hub))
        return run_forever(
            config,
            heard_path=Path(args.heard),
            http_port=args.http_port,
            enable_http=not args.no_http,
        )
    except PublishConfigError as exc:
        LOGGER.error("%s", exc)
        return 2
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        LOGGER.error("hub-live failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
