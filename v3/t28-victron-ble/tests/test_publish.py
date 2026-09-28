from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from t28_victron_ble.publish import (
    PublishConfigError,
    assert_enable_allowed,
    build_publish_payload,
    expand_topic,
    is_placeholder_broker,
    load_credentials,
    load_publish_config,
    run_forever,
    sanitize_payload,
)
from t28_victron_ble.publish_cli import publish_configure_main

REPO = Path(__file__).resolve().parents[3]
UNIT = REPO / "v3" / "t28-victron-ble" / "packaging" / "t28-victron-publish.service"
BLE_UNIT = REPO / "v3" / "t28-victron-ble" / "packaging" / "t28-victron-ble.service"
EXAMPLE = REPO / "v3" / "t28-victron-ble" / "packaging" / "publish.example.json"
PUBLISH_PY = REPO / "v3" / "t28-victron-ble" / "t28_victron_ble" / "publish.py"


class PayloadSanitizerTests(unittest.TestCase):
    def test_strips_forbidden_fields_from_hostile_latest_json(self) -> None:
        hostile = {
            "schemaVersion": 1,
            "devices": {
                "house-bank": {
                    "status": "keyMismatch",
                    "batteryVoltageV": None,
                    "advertisementKey": "should-not-publish",
                    "rawAdvertisement": "aabbcc",
                    "encryptionKey": "nope",
                    "nested": {"deviceKey": "hidden", "ok": True},
                }
            },
            "keys": {"house-bank": "nope"},
            "password": "nope",
            "source": "attacker",
        }
        cleaned = sanitize_payload(hostile)
        dumped = json.dumps(cleaned).lower()
        self.assertNotIn("advertisementkey", dumped)
        self.assertNotIn("rawadvertisement", dumped)
        self.assertNotIn("encryptionkey", dumped)
        self.assertNotIn("devicekey", dumped)
        self.assertNotIn("should-not-publish", dumped)
        self.assertNotIn("aabbcc", dumped)
        self.assertNotIn("password", dumped)
        self.assertNotIn("nope", dumped)
        self.assertEqual(cleaned["devices"]["house-bank"]["status"], "keyMismatch")
        self.assertTrue(cleaned["devices"]["house-bank"]["nested"]["ok"])

    def test_build_payload_adds_source_host_and_keeps_stale_status(self) -> None:
        latest = {
            "devices": {
                "house-bank": {"status": "stale", "batteryVoltageV": 12.5, "rawAdvertisement": "ff"}
            }
        }
        payload = build_publish_payload(latest, hostname="demo-receiver", published_at="2026-08-31T00:00:00.000Z")
        self.assertEqual(payload["source"], "t28-victron-ble")
        self.assertEqual(payload["host"], "demo-receiver")
        self.assertEqual(payload["publishedAtUtc"], "2026-08-31T00:00:00.000Z")
        self.assertEqual(payload["devices"]["house-bank"]["status"], "stale")
        self.assertNotIn("rawAdvertisement", payload["devices"]["house-bank"])

    def test_placeholder_and_topic_expansion(self) -> None:
        self.assertTrue(is_placeholder_broker("mqtt://127.0.0.1:1883"))
        self.assertTrue(is_placeholder_broker("mqtt://localhost:1883"))
        self.assertTrue(is_placeholder_broker(""))
        self.assertFalse(is_placeholder_broker("mqtt://broker.example:1883"))
        self.assertFalse(is_placeholder_broker("mqtt://192.0.2.2:1883"))
        self.assertEqual(expand_topic("demo/power/{hostname}", "demo-receiver"), "demo/power/demo-receiver")
        self.assertEqual(
            expand_topic("demo/power/heard/{hostname}/{deviceId}", "demo-receiver", "house-bank"),
            "demo/power/heard/demo-receiver/house-bank",
        )
        self.assertEqual(
            expand_topic("demo/power/heard/{hostname}", "demo-receiver", "house-bank"),
            "demo/power/heard/demo-receiver/house-bank",
        )

    def test_missing_latest_json_skips_without_raising(self) -> None:
        published: list[str] = []

        class Fake:
            def connect(self) -> None:
                return None

            def publish(self, topic: str, payload: str, *, qos: int, retain: bool) -> None:
                published.append(payload)

        with tempfile.TemporaryDirectory() as tmp:
            config = load_publish_config(_write_publish(Path(tmp), "mqtt://broker.example:1883", enabled=True))
            config = config.__class__(**{**config.__dict__, "latest_path": Path(tmp) / "missing.json"})
            calls = {"n": 0}

            def once(_seconds: float) -> None:
                return None

            def stop_after_one() -> bool:
                calls["n"] += 1
                return calls["n"] <= 1

            code = run_forever(
                config,
                transport=Fake(),
                hostname="demo-receiver",
                sleep=once,
                should_continue=stop_after_one,
            )
        self.assertEqual(code, 0)
        self.assertEqual(published, [])

    def test_refuses_keys_json_as_credentials(self) -> None:
        with self.assertRaises(PublishConfigError):
            load_credentials(Path("/etc/t28-victron/keys.json"))

    def test_publish_module_does_not_open_hci_or_keys(self) -> None:
        text = PUBLISH_PY.read_text(encoding="utf-8")
        self.assertNotIn("hci_scan", text)
        self.assertNotIn("AF_BLUETOOTH", text)
        self.assertNotIn("parse_hex_key", text)
        self.assertNotIn("load_service_config", text)
        self.assertIn("must not load", text)


class PublishConfigureTests(unittest.TestCase):
    def test_enable_refuses_placeholder_and_empty_broker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            publish = Path(tmp) / "publish.json"
            creds = Path(tmp) / "publish-credentials.json"
            with mock.patch("os.geteuid", return_value=0), mock.patch("t28_victron_ble.publish_cli._systemctl") as sysctl:
                code = publish_configure_main(
                    ["--publish", str(publish), "--credentials", str(creds), "--enable"]
                )
            self.assertEqual(code, 1)
            sysctl.assert_not_called()
            self.assertTrue(publish.is_file())
            self.assertFalse(json.loads(publish.read_text(encoding="utf-8")).get("enabled"))
            with mock.patch("os.geteuid", return_value=0), mock.patch("t28_victron_ble.publish_cli._systemctl") as sysctl:
                code = publish_configure_main(
                    [
                        "--publish",
                        str(publish),
                        "--credentials",
                        str(creds),
                        "--broker",
                        "mqtt://127.0.0.1:1883",
                        "--enable",
                    ]
                )
            self.assertEqual(code, 1)
            sysctl.assert_not_called()

    def test_enable_accepts_real_broker_and_only_touches_publish_unit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            publish = Path(tmp) / "publish.json"
            creds = Path(tmp) / "publish-credentials.json"
            with (
                mock.patch("os.geteuid", return_value=0),
                mock.patch("t28_victron_ble.publish_cli._systemctl", return_value=True) as sysctl,
            ):
                code = publish_configure_main(
                    [
                        "--publish",
                        str(publish),
                        "--credentials",
                        str(creds),
                        "--broker",
                        "mqtt://broker.example:1883",
                        "--topic",
                        "demo/power/{hostname}",
                        "--interval",
                        "5",
                        "--enable",
                    ]
                )
            self.assertEqual(code, 0)
            sysctl.assert_called_once()
            self.assertEqual(sysctl.call_args[0][0], ["enable", "--now", "t28-victron-publish.service"])
            written = json.loads(publish.read_text(encoding="utf-8"))
            self.assertEqual(written["broker"], "mqtt://broker.example:1883")
            self.assertTrue(written["enabled"])
            self.assertNotIn("password", written)
            self.assertNotIn("username", written)

    def test_show_does_not_print_password(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            publish = Path(tmp) / "publish.json"
            creds = Path(tmp) / "publish-credentials.json"
            publish.write_text(
                json.dumps(
                    {
                        "schemaVersion": 1,
                        "enabled": False,
                        "broker": "mqtt://broker.example:1883",
                        "topic": "demo/power/{hostname}",
                        "tls": False,
                    }
                ),
                encoding="utf-8",
            )
            creds.write_text(json.dumps({"username": "unit-test"}), encoding="utf-8")
            buf = io.StringIO()
            with mock.patch("os.geteuid", return_value=0), mock.patch("sys.stdout", buf), mock.patch(
                "t28_victron_ble.publish_cli._unit_flags", return_value=("no", "no")
            ):
                code = publish_configure_main(
                    ["--publish", str(publish), "--credentials", str(creds), "--show"]
                )
            output = buf.getvalue()
            self.assertEqual(code, 0)
            self.assertIn("broker=mqtt://broker.example:1883", output)
            self.assertIn("topic=demo/power/{hostname}", output)
            self.assertIn("tls=no", output)
            self.assertIn("credentialsPresent=yes", output)
            self.assertIn("unitEnabled=no", output)
            self.assertNotIn("password", output.lower())
            self.assertNotIn("unit-test", output)

    def test_assert_enable_allowed_rejects_placeholder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_publish(Path(tmp), "mqtt://127.0.0.1:1883", enabled=True)
            config = load_publish_config(path)
            with self.assertRaises(PublishConfigError):
                assert_enable_allowed(config)


class PublishUnitFileTests(unittest.TestCase):
    def test_publish_unit_has_no_ip_deny_or_bluetoothd_restart(self) -> None:
        text = UNIT.read_text(encoding="utf-8")
        self.assertNotIn("IPAddressDeny=", text)
        self.assertIn("RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6", text)
        self.assertNotIn("AF_BLUETOOTH", text)
        self.assertNotIn("CAP_NET_RAW", text)
        self.assertNotIn("AmbientCapabilities", text)
        self.assertNotIn("systemctl restart bluetooth", text)
        self.assertNotIn("bluetoothd", text)
        self.assertNotIn("Requires=", text)
        self.assertNotIn("t28-v2-receiver", text)
        self.assertNotIn("EnvironmentFile=", text)
        self.assertNotIn("LoadCredential=victron-keys", text)
        self.assertIn("InaccessiblePaths=-/etc/t28-victron/keys.json", text)
        self.assertIn("User=t28-victron", text)
        self.assertIn("ConditionPathExists=/etc/t28-victron/publish.json", text)
        self.assertIn("WantedBy=multi-user.target", text)
        ble = BLE_UNIT.read_text(encoding="utf-8")
        self.assertIn("IPAddressDeny=any", ble)
        example = EXAMPLE.read_text(encoding="utf-8")
        self.assertIn('"enabled": false', example)
        self.assertNotIn("password", example)
        self.assertNotIn("username", example)
        self.assertNotRegex(example, r"(?i)live")


def _write_publish(root: Path, broker: str, *, enabled: bool) -> Path:
    path = root / "publish.json"
    path.write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "enabled": enabled,
                "broker": broker,
                "topic": "demo/power/{hostname}",
                "clientId": "t28-victron-publish",
                "qos": 1,
                "retain": True,
                "intervalSeconds": 5,
                "tls": False,
            }
        ),
        encoding="utf-8",
    )
    return path
