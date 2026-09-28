from __future__ import annotations

import json
import re
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from t28_victron_ble.config import ConfigError
from t28_victron_ble.fleet import apply_fleet_config, parse_fleet_config
from t28_victron_ble.heard import build_heard_payload, iter_heard_publications
from t28_victron_ble.hub_config import admin_devices_response, list_devices_public, write_device
from t28_victron_ble.hub_live import HeardMap, decorate_heard, group_heard_devices, make_handler
from t28_victron_ble.publish import run_forever
from t28_victron_ble.topics import heard_topic, parse_heard_topic

REPO = Path(__file__).resolve().parents[3]
MODULE = REPO / "v3" / "t28-victron-ble"
ACL = MODULE / "packaging" / "mosquitto" / "t28-victron.acl.example"
MOSQUITTO = MODULE / "packaging" / "mosquitto" / "mosquitto-overlay.conf.example"
EXAMPLE = MODULE / "packaging" / "publish.example.json"
SYNC_UNIT = MODULE / "packaging" / "t28-victron-config-sync.service"
BLE_UNIT = MODULE / "packaging" / "t28-victron-ble.service"
HUB_LIVE_UNIT = MODULE / "packaging" / "hub" / "t28-victron-hub-live.service"

# Length-check fixture only. Not a real Instant Readout key.
OTHER_FAKE_KEY = "ab" * 16

# Length-check fixture only. Not a real Instant Readout key.
FAKE_KEY = "0" * 32


def _fleet(**overrides):
    payload = {
        "schemaVersion": 1,
        "revision": 3,
        "updatedAtUtc": "2026-08-31T00:00:00.000Z",
        "devices": [
            {
                "id": "house-bank",
                "address": "020000000001",
                "product": "SmartShunt 500A/50mV",
                "productId": "0xA389",
                "ignoredExtra": True,
            }
        ],
        "keys": {"house-bank": FAKE_KEY},
        "unexpected": {"keep": False},
    }
    payload.update(overrides)
    return payload


class FleetParseTests(unittest.TestCase):
    def test_parse_applies_devices_and_keys_and_ignores_extra_fields(self) -> None:
        fleet = parse_fleet_config(_fleet())
        self.assertEqual(fleet.revision, 3)
        self.assertEqual(fleet.devices[0].address, "02:00:00:00:00:01")
        self.assertEqual(fleet.keys_hex["house-bank"], FAKE_KEY)
        with tempfile.TemporaryDirectory() as tmp:
            devices = Path(tmp) / "devices.json"
            keys = Path(tmp) / "keys.json"
            state = Path(tmp) / "fleet-state.json"
            calls: list[list[str]] = []

            def systemctl(argv: list[str]) -> int:
                calls.append(argv)
                if argv[:2] == ["is-enabled", "--quiet"]:
                    return 0
                return 0

            result = apply_fleet_config(
                _fleet(),
                devices_path=devices,
                keys_path=keys,
                state_path=state,
                restart_ble=True,
                systemctl=systemctl,
            )
            self.assertTrue(result.changed)
            self.assertEqual(result.ble_action, "restarted")
            self.assertEqual(calls[-1], ["restart", "t28-victron-ble.service"])
            written_devices = json.loads(devices.read_text(encoding="utf-8"))
            written_keys = json.loads(keys.read_text(encoding="utf-8"))
            self.assertNotIn("ignoredExtra", json.dumps(written_devices))
            self.assertNotIn("unexpected", json.dumps(written_devices))
            self.assertEqual(written_devices["devices"][0]["address"], "02:00:00:00:00:01")
            self.assertEqual(written_keys["keys"]["house-bank"], FAKE_KEY)
            again = apply_fleet_config(
                _fleet(),
                devices_path=devices,
                keys_path=keys,
                state_path=state,
                restart_ble=True,
                systemctl=systemctl,
            )
            self.assertFalse(again.changed)
            self.assertEqual(again.ble_action, "skipped-unchanged")
            self.assertEqual(sum(1 for item in calls if item[0] == "restart"), 1)

    def test_rejects_missing_schema_and_revision(self) -> None:
        missing_schema = _fleet()
        del missing_schema["schemaVersion"]
        with self.assertRaises(ConfigError):
            parse_fleet_config(missing_schema)
        missing_revision = _fleet()
        del missing_revision["revision"]
        with self.assertRaises(ConfigError):
            parse_fleet_config(missing_revision)
        with self.assertRaises(ConfigError):
            parse_fleet_config(_fleet(revision=0))
        with self.assertRaises(ConfigError):
            parse_fleet_config({"schemaVersion": 1, "revision": 1, "devices": [], "keys": {}})

    def test_does_not_restart_disabled_ble(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            def systemctl(argv: list[str]) -> int:
                if argv[:2] == ["is-enabled", "--quiet"]:
                    return 1
                raise AssertionError(f"unexpected systemctl {argv}")

            result = apply_fleet_config(
                _fleet(),
                devices_path=Path(tmp) / "devices.json",
                keys_path=Path(tmp) / "keys.json",
                state_path=Path(tmp) / "state.json",
                systemctl=systemctl,
            )
            self.assertEqual(result.ble_action, "skipped-disabled")


class ReceiverAclTests(unittest.TestCase):
    def test_example_acl_denies_receiver_config_writes(self) -> None:
        text = ACL.read_text(encoding="utf-8")
        self.assertIn("user t28-config", text)
        self.assertIn("topic write demo/power/config/#", text)
        self.assertIn("user t28-publish", text)
        self.assertIn("topic read demo/power/config/#", text)
        self.assertIn("topic write demo/power/heard/#", text)
        publish_block = text.split("user t28-publish", 1)[1]
        self.assertNotIn("topic write demo/power/config", publish_block)
        self.assertNotIn("topic write demo/power/config/#", publish_block)
        self.assertNotRegex(text, r"(?im)^(password|username)\s")
        snippet = MOSQUITTO.read_text(encoding="utf-8")
        self.assertIn("listener 1883 192.0.2.2", snippet)
        self.assertIn("allow_anonymous false", snippet)
        self.assertNotRegex(snippet, r"(?im)^(password|username)\s")


class HeardPayloadTests(unittest.TestCase):
    def test_topic_encoding_and_no_keys_in_payloads(self) -> None:
        self.assertEqual(heard_topic("demo-receiver", "house-bank"), "demo/power/heard/demo-receiver/house-bank")
        self.assertEqual(parse_heard_topic("demo/power/heard/demo-receiver/house-bank"), ("demo-receiver", "house-bank"))
        latest = {
            "schemaVersion": 1,
            "devices": {
                "house-bank": {
                    "deviceId": "house-bank",
                    "address": "02:00:00:00:00:01",
                    "productId": "0xA389",
                    "status": "fresh",
                    "observedAtUtc": "2026-08-31T00:00:00.000Z",
                    "rssiDbm": -57,
                    "batteryVoltageV": 12.53,
                    "batteryCurrentA": None,
                    "stateOfChargePercent": 50.0,
                    "timeToGoMinutes": None,
                    "consumedAh": 1.5,
                    "alarmMask": 0,
                    "alarms": {},
                    "advertisementKey": "should-not-leave",
                    "keys": {"house-bank": FAKE_KEY},
                }
            },
            "keys": {"house-bank": FAKE_KEY},
        }
        dumped_latest = json.dumps(latest)
        self.assertNotIn(FAKE_KEY, json.dumps(build_heard_payload(latest["devices"]["house-bank"], hostname="demo-receiver", device_id="house-bank")))
        publications = iter_heard_publications(
            latest,
            hostname="demo-receiver",
            topic_template="demo/power/heard/{hostname}/{deviceId}",
            published_at="2026-08-31T00:00:01.000Z",
        )
        self.assertEqual(publications[0][0], "demo/power/heard/demo-receiver/house-bank")
        payload = publications[0][1]
        self.assertEqual(payload["hostname"], "demo-receiver")
        self.assertEqual(payload["receiverId"], "demo-receiver")
        self.assertEqual(payload["status"], "fresh")
        self.assertIsNone(payload["timeToGoMinutes"])
        dumped = json.dumps(payload).lower()
        self.assertNotIn("advertisementkey", dumped)
        self.assertNotIn("should-not-leave", dumped)
        self.assertNotIn("keys", dumped)
        self.assertNotIn(FAKE_KEY, dumped)
        self.assertIn("keys", dumped_latest)

    def test_publisher_emits_per_device_retained_topics(self) -> None:
        published: list[tuple[str, str, int, bool]] = []

        class Fake:
            def connect(self) -> None:
                return None

            def publish(self, topic: str, payload: str, *, qos: int, retain: bool) -> None:
                published.append((topic, payload, qos, retain))

        with tempfile.TemporaryDirectory() as tmp:
            latest = Path(tmp) / "latest.json"
            latest.write_text(
                json.dumps(
                    {
                        "devices": {
                            "house-bank": {
                                "deviceId": "house-bank",
                                "address": "02:00:00:00:00:01",
                                "status": "stale",
                                "timeToGoMinutes": None,
                                "advertisementKey": "nope",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            publish = Path(tmp) / "publish.json"
            publish.write_text(
                json.dumps(
                    {
                        "schemaVersion": 1,
                        "enabled": True,
                        "broker": "mqtt://192.0.2.2:1883",
                        "topic": "demo/power/heard/{hostname}/{deviceId}",
                        "qos": 1,
                        "retain": True,
                        "intervalSeconds": 5,
                    }
                ),
                encoding="utf-8",
            )
            from t28_victron_ble.publish import load_publish_config

            config = load_publish_config(publish, latest_path=latest)
            calls = {"n": 0}

            def stop_after_one() -> bool:
                calls["n"] += 1
                return calls["n"] <= 1

            code = run_forever(
                config,
                transport=Fake(),
                hostname="demo-receiver",
                sleep=lambda _seconds: None,
                should_continue=stop_after_one,
            )
        self.assertEqual(code, 0)
        self.assertEqual(published[0][0], "demo/power/heard/demo-receiver/house-bank")
        self.assertEqual(published[0][2], 1)
        self.assertTrue(published[0][3])
        body = json.loads(published[0][1])
        self.assertEqual(body["status"], "stale")
        self.assertNotIn("advertisementKey", body)
        self.assertNotIn("key", json.dumps(body).lower())


class HubLiveTests(unittest.TestCase):
    def test_marks_stale_from_observed_at_and_overwrites_topic(self) -> None:
        now = datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc)
        fresh = decorate_heard(
            {"deviceId": "house-bank", "observedAtUtc": "2026-08-31T11:59:30.000Z", "status": "fresh"},
            now=now,
            stale_after_seconds=90,
        )
        self.assertFalse(fresh["stale"])
        old = decorate_heard(
            {"deviceId": "house-bank", "observedAtUtc": "2026-08-31T11:58:00.000Z", "status": "fresh"},
            now=now,
            stale_after_seconds=90,
        )
        self.assertTrue(old["stale"])
        self.assertEqual(old["status"], "stale")
        with tempfile.TemporaryDirectory() as tmp:
            store = HeardMap(Path(tmp) / "heard.json", stale_after_seconds=90)
            store.upsert(
                "demo/power/heard/demo-receiver/house-bank",
                {"deviceId": "house-bank", "batteryVoltageV": 12.5, "observedAtUtc": format_iso(now), "status": "fresh"},
                now=now,
            )
            store.upsert(
                "demo/power/heard/demo-receiver/house-bank",
                {"deviceId": "house-bank", "batteryVoltageV": 12.6, "observedAtUtc": format_iso(now), "status": "fresh"},
                now=now,
            )
            payload = json.loads((Path(tmp) / "heard.json").read_text(encoding="utf-8"))
            self.assertEqual(len(payload["topics"]), 1)
            self.assertEqual(payload["topics"]["demo/power/heard/demo-receiver/house-bank"]["batteryVoltageV"], 12.6)
            self.assertNotIn("history", payload)
            self.assertEqual(len(payload["devices"]), 1)
            self.assertEqual(payload["devices"][0]["heardBy"], "demo-receiver")

    def test_group_heard_two_hosts_one_row_freshest_wins(self) -> None:
        topics = {
            "demo/power/heard/other-host/house-bank": {
                "deviceId": "house-bank",
                "hostname": "other-host",
                "status": "fresh",
                "stale": False,
                "ageSeconds": 12.0,
                "batteryVoltageV": 12.4,
            },
            "demo/power/heard/demo-receiver/house-bank": {
                "deviceId": "house-bank",
                "hostname": "demo-receiver",
                "status": "fresh",
                "stale": False,
                "ageSeconds": 3.0,
                "batteryVoltageV": 12.6,
            },
        }
        rows = group_heard_devices(topics)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["deviceId"], "house-bank")
        self.assertEqual(rows[0]["heardBy"], "demo-receiver")
        self.assertEqual(rows[0]["batteryVoltageV"], 12.6)
        also = rows[0]["alsoHeardBy"]
        self.assertEqual(len(also), 1)
        self.assertEqual(also[0]["hostname"], "other-host")
        self.assertEqual(also[0]["ageSeconds"], 12.0)
        self.assertEqual(also[0]["status"], "fresh")
        self.assertEqual(rows[0]["heardByDisplay"], "demo-receiver also: other-host 12s")
        stale_newer = {
            "demo/power/heard/other-host/house-bank": {
                "deviceId": "house-bank",
                "hostname": "other-host",
                "status": "stale",
                "stale": True,
                "ageSeconds": 1.0,
            },
            "demo/power/heard/demo-receiver/house-bank": {
                "deviceId": "house-bank",
                "hostname": "demo-receiver",
                "status": "fresh",
                "stale": False,
                "ageSeconds": 20.0,
            },
        }
        preferred = group_heard_devices(stale_newer)
        self.assertEqual(preferred[0]["heardBy"], "demo-receiver")


class HubAdminTests(unittest.TestCase):
    def test_write_device_empty_key_keeps_existing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            devices = Path(tmp) / "devices.json"
            keys = Path(tmp) / "keys.json"
            write_device(
                devices,
                keys,
                device_id="house-bank",
                address="020000000001",
                product="SmartShunt 500A/50mV",
                product_id=0xA389,
                key_hex=OTHER_FAKE_KEY,
            )
            before = json.loads(keys.read_text(encoding="utf-8"))["keys"]["house-bank"]
            write_device(
                devices,
                keys,
                device_id="house-bank",
                address="020000000003",
                product="SmartShunt 500A/50mV",
                product_id=0xA389,
                key_hex="",
            )
            after = json.loads(keys.read_text(encoding="utf-8"))["keys"]["house-bank"]
            self.assertEqual(after, before)
            public = list_devices_public(devices, keys)
            self.assertTrue(public["devices"][0]["keyPresent"])
            self.assertEqual(public["devices"][0]["address"], "02:00:00:00:00:03")
            self.assertEqual(public["devices"][0]["key"], before)
            dumped = json.dumps(admin_devices_response(public))
            self.assertIn('"key"', dumped)
            self.assertNotIn('"keys"', dumped)
            hostile = admin_devices_response({"devices": [{"id": "x", "key": before, "keys": {"x": before}}]})
            self.assertEqual(hostile["devices"][0]["key"], before)
            self.assertNotIn("keys", json.dumps(hostile))
            with self.assertRaises(ConfigError):
                write_device(
                    devices,
                    Path(tmp) / "missing-keys.json",
                    device_id="starter",
                    address="020000000001",
                    product="SmartShunt 500A/50mV",
                    product_id=None,
                    key_hex="",
                )

    def test_rename_moves_key_and_drops_old_heard_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            devices = Path(tmp) / "devices.json"
            keys = Path(tmp) / "keys.json"
            write_device(
                devices,
                keys,
                device_id="house-bank",
                address="020000000001",
                product="SmartShunt 500A/50mV",
                product_id=0xA389,
                key_hex=OTHER_FAKE_KEY,
            )
            write_device(
                devices,
                keys,
                device_id="house",
                address="020000000001",
                product="SmartShunt 500A/50mV",
                product_id=0xA389,
                previous_id="house-bank",
            )
            written = json.loads(devices.read_text(encoding="utf-8"))
            self.assertEqual([item["id"] for item in written["devices"]], ["house"])
            moved = json.loads(keys.read_text(encoding="utf-8"))["keys"]
            self.assertEqual(moved.get("house"), OTHER_FAKE_KEY)
            self.assertNotIn("house-bank", moved)
            public = list_devices_public(devices, keys)
            self.assertEqual(public["devices"][0]["id"], "house")
            self.assertEqual(public["devices"][0]["key"], OTHER_FAKE_KEY)
            self.assertNotIn("keys", json.dumps(admin_devices_response(public)))
            store = HeardMap(Path(tmp) / "heard.json")
            now = datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc)
            store.upsert(
                "demo/power/heard/demo-receiver/house-bank",
                {
                    "deviceId": "house-bank",
                    "hostname": "demo-receiver",
                    "status": "fresh",
                    "observedAtUtc": "2026-08-31T11:59:57.000Z",
                },
                now=now,
            )
            store.upsert(
                "demo/power/heard/other-host/starter",
                {
                    "deviceId": "starter",
                    "hostname": "other-host",
                    "status": "fresh",
                    "observedAtUtc": "2026-08-31T11:59:50.000Z",
                },
                now=now,
            )
            store.drop_device("house-bank")
            remaining = store.as_dict()
            self.assertEqual(list(remaining["topics"]), ["demo/power/heard/other-host/starter"])
            self.assertEqual([item["deviceId"] for item in remaining["devices"]], ["starter"])
            self.assertIsNone(re.search(r"(?i)\b[0-9a-f]{32}\b", json.dumps(remaining)))

    def test_devices_json_includes_key_but_heard_strips_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            devices = Path(tmp) / "devices.json"
            keys = Path(tmp) / "keys.json"
            write_device(
                devices,
                keys,
                device_id="house-bank",
                address="02:00:00:00:00:01",
                product="SmartShunt 500A/50mV",
                product_id=0xA389,
                key_hex=FAKE_KEY,
            )
            store = HeardMap(Path(tmp) / "heard.json")
            now = datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc)
            store.upsert(
                "demo/power/heard/demo-receiver/house-bank",
                {
                    "deviceId": "house-bank",
                    "hostname": "demo-receiver",
                    "status": "fresh",
                    "batteryVoltageV": 12.6,
                    "observedAtUtc": "2026-08-31T11:59:57.000Z",
                },
                now=now,
            )
            store.upsert(
                "demo/power/heard/other-host/house-bank",
                {
                    "deviceId": "house-bank",
                    "hostname": "other-host",
                    "status": "fresh",
                    "batteryVoltageV": 12.4,
                    "observedAtUtc": "2026-08-31T11:59:48.000Z",
                    "key": FAKE_KEY,
                    "advertisementKey": FAKE_KEY,
                },
                now=now,
            )
            server = ThreadingHTTPServer(
                ("127.0.0.1", 0),
                make_handler(
                    store,
                    devices_path=devices,
                    keys_path=keys,
                    admin_html_path=MODULE / "packaging" / "hub" / "admin.html",
                ),
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            port = server.server_address[1]
            try:
                heard_body = urlopen(f"http://127.0.0.1:{port}/heard.json").read().decode("utf-8")
                heard = json.loads(heard_body)
                self.assertEqual(len(heard["topics"]), 2)
                self.assertEqual(len(heard["devices"]), 1)
                self.assertEqual(heard["devices"][0]["heardBy"], "demo-receiver")
                self.assertIn("also: other-host", heard["devices"][0]["heardByDisplay"])
                self.assertIsNone(re.search(r"(?i)\b[0-9a-f]{32}\b", heard_body))
                self.assertNotIn('"key"', heard_body.replace("keyPresent", ""))
                self.assertNotIn('"keys"', heard_body)
                for path in ("/devices.json", "/victron/devices.json"):
                    body = urlopen(f"http://127.0.0.1:{port}{path}").read().decode("utf-8")
                    payload = json.loads(body)
                    self.assertTrue(payload["devices"][0]["keyPresent"])
                    self.assertEqual(payload["devices"][0]["id"], "house-bank")
                    self.assertEqual(payload["devices"][0]["key"], FAKE_KEY)
                    self.assertNotIn('"keys"', body)
                for path in ("/keys.json", "/victron/keys.json"):
                    with self.assertRaises(HTTPError) as raised:
                        urlopen(f"http://127.0.0.1:{port}{path}")
                    self.assertEqual(raised.exception.code, 403)
                    forbidden = raised.exception.read().decode("utf-8")
                    self.assertIsNone(re.search(r"(?i)\b[0-9a-f]{32}\b", forbidden))
                live = urlopen(f"http://127.0.0.1:{port}/victron").read().decode("utf-8")
                self.assertIn("/victron/admin", live)
                self.assertIsNone(re.search(r"(?i)\b[0-9a-f]{32}\b", live))
                admin = urlopen(f"http://127.0.0.1:{port}/admin").read().decode("utf-8")
                self.assertIn("Add device", admin)
                self.assertIn('data-action="edit"', admin)
                self.assertIn('data-action="remove"', admin)
                self.assertIn('id="cancel"', admin)
                self.assertIn('id="editor"', admin)
                self.assertIn("previousId", admin)
                self.assertNotIn("form.id.readOnly = true", admin)
                self.assertIn("device?.key", admin)
                self.assertNotIn('value="' + FAKE_KEY, admin)
                update = Request(
                    f"http://127.0.0.1:{port}/devices",
                    data=json.dumps(
                        {
                            "id": "house-bank",
                            "address": "020000000003",
                            "product": "SmartShunt 500A/50mV",
                            "productId": "0xA389",
                        }
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                updated = json.loads(urlopen(update).read())
                self.assertEqual(updated["devices"][0]["address"], "02:00:00:00:00:03")
                self.assertTrue(updated["devices"][0]["keyPresent"])
                self.assertEqual(updated["devices"][0]["key"], FAKE_KEY)
                self.assertEqual(json.loads(keys.read_text(encoding="utf-8"))["keys"]["house-bank"], FAKE_KEY)
                create = Request(
                    f"http://127.0.0.1:{port}/victron/devices",
                    data=json.dumps(
                        {
                            "id": "starter",
                            "address": "020000000001",
                            "product": "SmartShunt 500A/50mV",
                            "key": OTHER_FAKE_KEY,
                        }
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                created = json.loads(urlopen(create).read())
                self.assertEqual({item["id"] for item in created["devices"]}, {"house-bank", "starter"})
                self.assertNotIn("keys", json.dumps(created))
                starter = next(item for item in created["devices"] if item["id"] == "starter")
                self.assertEqual(starter["key"], OTHER_FAKE_KEY)
                deleted = json.loads(
                    urlopen(Request(f"http://127.0.0.1:{port}/devices/starter", method="DELETE")).read()
                )
                self.assertEqual([item["id"] for item in deleted["devices"]], ["house-bank"])
                rename = Request(
                    f"http://127.0.0.1:{port}/devices",
                    data=json.dumps(
                        {
                            "id": "house",
                            "previousId": "house-bank",
                            "address": "020000000003",
                            "product": "SmartShunt 500A/50mV",
                            "productId": "0xA389",
                        }
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                renamed = json.loads(urlopen(rename).read())
                self.assertEqual([item["id"] for item in renamed["devices"]], ["house"])
                self.assertEqual(renamed["devices"][0]["key"], FAKE_KEY)
                self.assertNotIn("keys", json.dumps(renamed))
                self.assertEqual(json.loads(keys.read_text(encoding="utf-8"))["keys"], {"house": FAKE_KEY})
                heard_after = json.loads(urlopen(f"http://127.0.0.1:{port}/heard.json").read())
                self.assertEqual(heard_after["topics"], {})
                self.assertEqual(heard_after["devices"], [])
                self.assertIsNone(re.search(r"(?i)\b[0-9a-f]{32}\b", json.dumps(heard_after)))
            finally:
                server.shutdown()
                server.server_close()


class IsolationContractTests(unittest.TestCase):
    def test_ble_stays_airgapped_and_examples_have_no_passwords(self) -> None:
        ble = BLE_UNIT.read_text(encoding="utf-8")
        self.assertIn("IPAddressDeny=any", ble)
        self.assertIn("RestrictAddressFamilies=AF_UNIX AF_BLUETOOTH", ble)
        sync = SYNC_UNIT.read_text(encoding="utf-8")
        self.assertNotIn("IPAddressDeny=", sync)
        self.assertNotIn("AF_BLUETOOTH", sync)
        self.assertIn("ReadWritePaths=/run/t28-victron /etc/t28-victron", sync)
        self.assertNotIn("ReadOnlyPaths=/etc/t28-victron", sync)
        self.assertNotIn("InaccessiblePaths=", sync)
        hub_live = HUB_LIVE_UNIT.read_text(encoding="utf-8")
        self.assertIn("User=t28-victron", hub_live)
        self.assertNotIn("User=root", hub_live)
        self.assertIn("ReadWritePaths=/run/t28-victron /etc/t28-victron", hub_live)
        self.assertNotIn("ReadOnlyPaths=/etc/t28-victron", hub_live)
        self.assertNotIn("InaccessiblePaths=", hub_live)
        self.assertNotIn("IPAddressDeny=", hub_live)
        example = EXAMPLE.read_text(encoding="utf-8")
        self.assertIn("demo/power/heard/{hostname}/{deviceId}", example)
        self.assertIn("127.0.0.1", example)
        self.assertNotIn("192.0.2.2", example)
        self.assertNotIn("password", example)


def format_iso(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S.000Z")
