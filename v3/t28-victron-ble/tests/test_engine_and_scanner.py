from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from t28_victron_ble.config import DeviceConfig, ServiceConfig
from t28_victron_ble.hci import CHARGER_PRODUCT_ID
from t28_victron_ble.engine import AdvertisementObservation, VictronEngine
from t28_victron_ble.scanner import BoundedBackoff, PassiveScanUnavailable, run_with_backoff
from t28_victron_ble.store import LatestStore

from support import PUBLIC_ADVERTISEMENT, PUBLIC_KEY, battery_plaintext, encode_advertisement


def _config(tmp: Path, devices: list[tuple[str, str, bytes]]) -> ServiceConfig:
    return ServiceConfig(
        devices=tuple(
            DeviceConfig(device_id=item[0], address=item[1], product="SmartShunt")
            for item in devices
        ),
        keys={item[0]: item[2] for item in devices},
        stale_after_seconds=30,
        run_dir=tmp,
        latest_path=tmp / "latest.json",
    )


class EngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.now = datetime(2026, 8, 30, 12, 0, tzinfo=timezone.utc)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_latest_json_is_a_map_and_duplicate_nonces_are_ignored(self) -> None:
        config = _config(self.root, [("house", "aa:bb:cc:dd:ee:01", PUBLIC_KEY)])
        store = LatestStore(config.latest_path)
        engine = VictronEngine(config, store, clock=lambda: self.now)
        first = AdvertisementObservation("AA:BB:CC:DD:EE:01", PUBLIC_ADVERTISEMENT, -70, self.now)
        engine.handle_advertisement(first)
        engine.handle_advertisement(
            AdvertisementObservation("aa:bb:cc:dd:ee:01", PUBLIC_ADVERTISEMENT, -71, self.now + timedelta(seconds=1))
        )
        payload = json.loads(config.latest_path.read_text(encoding="utf-8"))
        self.assertIn("house", payload["devices"])
        self.assertNotIsInstance(payload["devices"], list)
        house = payload["devices"]["house"]
        self.assertEqual(house["status"], "fresh")
        self.assertEqual(house["address"], "aa:bb:cc:dd:ee:01")
        self.assertEqual(house["batteryVoltageV"], 12.53)
        dumped = json.dumps(payload).lower()
        self.assertNotIn("advertisementkey", dumped)
        self.assertNotIn("keys", dumped)
        self.assertIsNone(house["timeToGoMinutes"])
        self.assertEqual(house["health"]["decodedOk"], 1)
        self.assertEqual(house["health"]["duplicateNonces"], 1)

    def test_multiple_devices_stale_independently(self) -> None:
        other_key = bytes(range(16))
        other_packet = encode_advertisement(other_key, battery_plaintext(voltage=2400), nonce=9)
        config = _config(
            self.root,
            [("house", "aa:bb:cc:dd:ee:01", PUBLIC_KEY), ("starter", "aa:bb:cc:dd:ee:02", other_key)],
        )
        store = LatestStore(config.latest_path)
        engine = VictronEngine(config, store, clock=lambda: self.now)
        engine.handle_advertisement(AdvertisementObservation("aa:bb:cc:dd:ee:01", PUBLIC_ADVERTISEMENT, -60, self.now))
        later = self.now + timedelta(seconds=10)
        engine.handle_advertisement(AdvertisementObservation("aa:bb:cc:dd:ee:02", other_packet, -80, later))
        engine.refresh_stale(self.now + timedelta(seconds=32))
        payload = json.loads(config.latest_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["devices"]["house"]["status"], "stale")
        self.assertEqual(payload["devices"]["starter"]["status"], "fresh")
        self.assertEqual(payload["devices"]["starter"]["batteryVoltageV"], 24.0)

    def test_key_mismatch_and_recovery(self) -> None:
        config = _config(self.root, [("house", "aa:bb:cc:dd:ee:01", PUBLIC_KEY)])
        store = LatestStore(config.latest_path)
        engine = VictronEngine(config, store, clock=lambda: self.now)
        bad = bytearray(PUBLIC_ADVERTISEMENT)
        bad[7] ^= 0xFF
        engine.handle_advertisement(AdvertisementObservation("aa:bb:cc:dd:ee:01", bytes(bad), -70, self.now))
        self.assertEqual(store.devices()["house"].status, "keyMismatch")
        engine.handle_advertisement(AdvertisementObservation("aa:bb:cc:dd:ee:01", PUBLIC_ADVERTISEMENT, -70, self.now))
        self.assertEqual(store.devices()["house"].status, "fresh")

    def test_device_loss_and_recovery(self) -> None:
        config = _config(self.root, [("house", "aa:bb:cc:dd:ee:01", PUBLIC_KEY)])
        store = LatestStore(config.latest_path)
        engine = VictronEngine(config, store, clock=lambda: self.now)
        engine.handle_advertisement(AdvertisementObservation("aa:bb:cc:dd:ee:01", PUBLIC_ADVERTISEMENT, -70, self.now))
        engine.mark_device_lost("aa:bb:cc:dd:ee:01", self.now + timedelta(seconds=2))
        self.assertEqual(store.devices()["house"].status, "stale")
        recovered = encode_advertisement(PUBLIC_KEY, battery_plaintext(voltage=1260), nonce=0x1111)
        engine.handle_advertisement(
            AdvertisementObservation("aa:bb:cc:dd:ee:01", recovered, -65, self.now + timedelta(seconds=3))
        )
        self.assertEqual(store.devices()["house"].status, "fresh")
        self.assertEqual(store.devices()["house"].battery_voltage_v, 12.60)

    def test_charger_product_is_not_decoded_as_a_shunt(self) -> None:
        config = ServiceConfig(
            devices=(DeviceConfig("house", "02:00:00:00:00:01", "SmartShunt 500A/50mV", 0xA389),),
            keys={"house": PUBLIC_KEY},
            stale_after_seconds=30,
            run_dir=self.root,
            latest_path=self.root / "latest.json",
        )
        store = LatestStore(config.latest_path)
        engine = VictronEngine(config, store, clock=lambda: self.now)
        charger = bytearray(PUBLIC_ADVERTISEMENT)
        charger[2:4] = CHARGER_PRODUCT_ID.to_bytes(2, "little")
        self.assertIsNone(
            engine.handle_advertisement(
                AdvertisementObservation("02:00:00:00:00:01", bytes(charger), -50, self.now)
            )
        )
        engine.handle_advertisement(AdvertisementObservation("02:00:00:00:00:01", PUBLIC_ADVERTISEMENT, -57, self.now))
        self.assertEqual(store.devices()["house"].status, "fresh")


class FakeSource:
    def __init__(self, events) -> None:
        self.events = list(events)
        self.started = 0
        self.stopped = False

    async def run(self, on_advertisement, on_device_lost=None) -> None:
        self.started += 1
        try:
            for event in self.events:
                if event == "lost":
                    if on_device_lost:
                        await on_device_lost("aa:bb:cc:dd:ee:01")
                elif event == "drop":
                    raise ConnectionError("bluez gone")
                elif event == "hang":
                    await asyncio.Event().wait()
                else:
                    await on_advertisement(event)
        finally:
            self.stopped = True


class ScannerTests(unittest.TestCase):
    def test_bounded_backoff_and_bluez_restart(self) -> None:
        delays: list[float] = []
        attempts = {"n": 0}

        async def connect() -> None:
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise ConnectionError("restart")

        async def sleep(delay: float) -> None:
            delays.append(delay)

        asyncio.run(
            run_with_backoff(
                connect,
                backoff=BoundedBackoff(1, 8, 2),
                sleep=sleep,
                should_continue=lambda: attempts["n"] < 3,
            )
        )
        self.assertEqual(delays, [1, 2])
        self.assertLessEqual(max(delays), 8)

    def test_passive_unavailable_is_not_retried(self) -> None:
        async def connect() -> None:
            raise PassiveScanUnavailable("nope")

        with self.assertRaises(PassiveScanUnavailable):
            asyncio.run(run_with_backoff(connect, sleep=lambda _: asyncio.sleep(0)))

    def test_graceful_shutdown_cancels_source(self) -> None:
        source = FakeSource(["hang"])

        async def body() -> None:
            task = asyncio.create_task(source.run(lambda *_: None))
            await asyncio.sleep(0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertTrue(source.stopped)

        asyncio.run(body())

    def test_fake_source_drives_multiple_configured_devices(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        root = Path(tmp.name)
        other_key = bytes(range(16))
        other = encode_advertisement(other_key, battery_plaintext(voltage=1100), nonce=3)
        now = datetime(2026, 8, 30, tzinfo=timezone.utc)
        config = _config(
            root,
            [("house", "aa:bb:cc:dd:ee:01", PUBLIC_KEY), ("bank2", "aa:bb:cc:dd:ee:02", other_key)],
        )
        engine = VictronEngine(config, LatestStore(config.latest_path), clock=lambda: now)
        source = FakeSource(
            [
                AdvertisementObservation("aa:bb:cc:dd:ee:01", PUBLIC_ADVERTISEMENT, -50, now),
                AdvertisementObservation("aa:bb:cc:dd:ee:02", other, -55, now),
                "lost",
            ]
        )

        async def on_ad(obs) -> None:
            engine.handle_advertisement(obs)

        async def on_lost(address: str) -> None:
            engine.mark_device_lost(address, now)

        asyncio.run(source.run(on_ad, on_lost))
        devices = json.loads(config.latest_path.read_text(encoding="utf-8"))["devices"]
        self.assertEqual(devices["house"]["status"], "stale")
        self.assertEqual(devices["bank2"]["status"], "fresh")
        tmp.cleanup()
