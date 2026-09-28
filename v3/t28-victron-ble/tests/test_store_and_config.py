from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from t28_victron_ble.config import ConfigError, load_service_config, normalize_mac
from t28_victron_ble.models import DeviceSnapshot
from t28_victron_ble.store import HistoryStore, LatestStore, atomic_write_json


class StoreTests(unittest.TestCase):
    def test_atomic_map_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "latest.json"
            store = LatestStore(path)
            store.ensure_device("house", "SmartShunt")
            store.ensure_device("starter", "SmartShunt")
            payload = store.flush()
            self.assertEqual(set(payload["devices"]), {"house", "starter"})
            on_disk = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(on_disk["devices"]["house"]["deviceId"], "house")
            self.assertFalse((path.parent / "latest.json.new").exists())

    def test_history_rate_limit_age_and_byte_cap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "history.jsonl"
            history = HistoryStore(path, interval_seconds=60, max_age_seconds=3600, max_bytes=4096)
            now = datetime(2026, 8, 30, tzinfo=timezone.utc)
            snap = DeviceSnapshot(
                device_id="house",
                status="fresh",
                last_valid_at=now,
                battery_voltage_v=12.5,
            )
            history.record(snap, now)
            history.record(snap, now + timedelta(seconds=10))
            self.assertEqual(path.read_text(encoding="utf-8").count("\n"), 1)
            history.record(snap, now + timedelta(seconds=61))
            self.assertEqual(path.read_text(encoding="utf-8").count("\n"), 2)
            old = DeviceSnapshot(device_id="house", status="fresh", last_valid_at=now)
            # Force an old line then prune far in the future.
            history.record(old, now + timedelta(hours=2))
            self.assertGreaterEqual(path.read_text(encoding="utf-8").count("\n"), 1)
            history.max_bytes = 1
            history.prune(now + timedelta(hours=2))
            self.assertLessEqual(len(path.read_text(encoding="utf-8").encode("utf-8")), 1)
            history.prune(now + timedelta(hours=3))
            for line in path.read_text(encoding="utf-8").splitlines():
                self.assertNotIn("rawAdvertisement", line)
                self.assertNotIn("encryptionKey", line)


class ConfigTests(unittest.TestCase):
    def test_normalize_mac_colon_and_12_hex(self) -> None:
        self.assertEqual(normalize_mac("02:00:00:00:00:01"), "02:00:00:00:00:01")
        self.assertEqual(normalize_mac("020000000001"), "02:00:00:00:00:01")
        self.assertEqual(normalize_mac("02 00 00 00 00 01"), "02:00:00:00:00:01")
        with self.assertRaises(ConfigError):
            normalize_mac("c69953bf943")
        with self.assertRaises(ConfigError):
            normalize_mac("not-a-mac")
        with self.assertRaises(ConfigError):
            normalize_mac("0200000000011")

    def test_loads_12_hex_address_from_devices_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            devices = Path(tmp) / "devices.json"
            keys = Path(tmp) / "keys.json"
            devices.write_text(
                json.dumps(
                    {
                        "schemaVersion": 1,
                        "devices": [{"id": "house", "address": "020000000001", "product": "SmartShunt", "productId": "0xA389"}],
                    }
                ),
                encoding="utf-8",
            )
            keys.write_text(
                json.dumps({"keys": {"house": "00112233445566778899aabbccddeeff"}}),
                encoding="utf-8",
            )
            config = load_service_config(devices, keys, run_dir=Path(tmp))
            self.assertEqual(config.devices[0].address, "02:00:00:00:00:01")
            self.assertEqual(config.device_by_address("020000000001").device_id, "house")

    def test_requires_devices_and_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            devices = Path(tmp) / "devices.json"
            keys = Path(tmp) / "keys.json"
            devices.write_text(json.dumps({"devices": []}), encoding="utf-8")
            with self.assertRaises(ConfigError):
                load_service_config(devices, keys)

    def test_valid_config_and_credential_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            devices = Path(tmp) / "devices.json"
            keys = Path(tmp) / "keys.json"
            devices.write_text(
                json.dumps(
                    {
                        "schemaVersion": 1,
                        "devices": [{"id": "house", "address": "AA:BB:CC:DD:EE:FF", "product": "SmartShunt"}],
                    }
                ),
                encoding="utf-8",
            )
            keys.write_text(
                json.dumps({"keys": {"house": "00112233445566778899aabbccddeeff"}}),
                encoding="utf-8",
            )
            config = load_service_config(devices, keys, run_dir=Path(tmp))
            self.assertEqual(config.devices[0].address, "aa:bb:cc:dd:ee:ff")
            self.assertEqual(len(config.keys["house"]), 16)

    def test_unknown_key_device_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            devices = Path(tmp) / "devices.json"
            keys = Path(tmp) / "keys.json"
            devices.write_text(
                json.dumps({"devices": [{"id": "house", "address": "aa:bb:cc:dd:ee:ff"}]}),
                encoding="utf-8",
            )
            keys.write_text(json.dumps({"keys": {"other": "aa" * 16}}), encoding="utf-8")
            with self.assertRaises(ConfigError):
                load_service_config(devices, keys)
