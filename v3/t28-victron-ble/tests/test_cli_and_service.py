from __future__ import annotations

import asyncio
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from t28_victron_ble.cli import check_main, configure_main, status_main
from t28_victron_ble import cli as cli_mod
from t28_victron_ble.config import DeviceConfig, ServiceConfig
from t28_victron_ble.scanner import PassiveScanUnavailable
from t28_victron_ble.service import main as service_main
from t28_victron_ble.service import serve

from support import PUBLIC_KEY

REPO = Path(__file__).resolve().parents[3]


class CliAndServiceTests(unittest.TestCase):
    def test_scan_failure_exits_nonzero_instead_of_waiting(self) -> None:
        class FailingSource:
            async def run(self, on_advertisement, on_device_lost=None) -> None:
                raise PassiveScanUnavailable("HCI filter rejected")

        async def body() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                config = ServiceConfig(
                    devices=(DeviceConfig("house", "02:00:00:00:00:01", "SmartShunt 500A/50mV", 0xA389),),
                    keys={"house": PUBLIC_KEY},
                    stale_after_seconds=30,
                    run_dir=root,
                    latest_path=root / "latest.json",
                )
                with self.assertRaises(PassiveScanUnavailable):
                    await asyncio.wait_for(serve(config, FailingSource()), timeout=2)

        asyncio.run(body())

    def test_main_returns_3_when_scan_is_unavailable(self) -> None:
        class Boom:
            def __init__(self, **_kwargs) -> None:
                return None

            async def run(self, *_args, **_kwargs) -> None:
                raise PassiveScanUnavailable("HCI filter rejected")

        with tempfile.TemporaryDirectory() as tmp:
            devices = Path(tmp) / "devices.json"
            keys = Path(tmp) / "keys.json"
            devices.write_text(
                '{"schemaVersion":1,"devices":[{"id":"house","address":"02:00:00:00:00:01","productId":"0xA389"}]}',
                encoding="utf-8",
            )
            keys.write_text(
                '{"schemaVersion":1,"keys":{"house":"00112233445566778899aabbccddeeff"}}',
                encoding="utf-8",
            )
            with mock.patch("t28_victron_ble.hci_scan.HciPassiveScanner", Boom):
                code = service_main(["--devices", str(devices), "--keys", str(keys), "--run-dir", tmp])
        self.assertEqual(code, 3)

    def test_service_refuses_to_start_without_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            code = service_main(["--devices", str(Path(tmp) / "missing.json"), "--keys", str(Path(tmp) / "keys.json")])
        self.assertEqual(code, 2)

    def test_check_passes_unit_and_decoder_self_test(self) -> None:
        unit = REPO / "v3" / "t28-victron-ble" / "packaging" / "t28-victron-ble.service"
        with tempfile.TemporaryDirectory() as tmp:
            code = check_main(
                [
                    "--unit",
                    str(unit),
                    "--devices",
                    str(Path(tmp) / "nope.json"),
                    "--latest",
                    str(Path(tmp) / "nope.json"),
                    "--adapter-state",
                    "unknown",
                    "--service-enabled",
                    "no",
                ]
            )
        self.assertEqual(code, 0)

    def test_check_hci0_down_is_info_when_service_disabled(self) -> None:
        unit = REPO / "v3" / "t28-victron-ble" / "packaging" / "t28-victron-ble.service"
        buf = io.StringIO()
        with tempfile.TemporaryDirectory() as tmp, mock.patch("sys.stdout", buf):
            code = check_main(
                [
                    "--unit",
                    str(unit),
                    "--devices",
                    str(Path(tmp) / "nope.json"),
                    "--latest",
                    str(Path(tmp) / "nope.json"),
                    "--adapter-state",
                    "DOWN",
                    "--service-enabled",
                    "no",
                ]
            )
        self.assertEqual(code, 0)
        self.assertIn("INFO  hci0 is DOWN", buf.getvalue())
        self.assertNotIn("FAIL  hci0", buf.getvalue())

    def test_check_hci0_down_fails_when_service_enabled(self) -> None:
        unit = REPO / "v3" / "t28-victron-ble" / "packaging" / "t28-victron-ble.service"
        buf = io.StringIO()
        with tempfile.TemporaryDirectory() as tmp, mock.patch("sys.stdout", buf):
            code = check_main(
                [
                    "--unit",
                    str(unit),
                    "--devices",
                    str(Path(tmp) / "nope.json"),
                    "--latest",
                    str(Path(tmp) / "nope.json"),
                    "--adapter-state",
                    "DOWN",
                    "--service-enabled",
                    "yes",
                ]
            )
        self.assertEqual(code, 1)
        self.assertIn("FAIL  hci0 is DOWN", buf.getvalue())

    def test_configure_enable_does_not_exec_systemctl(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            devices = Path(tmp) / "devices.json"
            keys = Path(tmp) / "keys.json"
            latest = Path(tmp) / "latest.json"
            latest.write_text(
                json.dumps(
                    {
                        "devices": {
                            "house": {
                                "status": "fresh",
                                "batteryVoltageV": 12.53,
                                "batteryCurrentA": None,
                                "stateOfChargePercent": 50,
                                "timeToGoMinutes": None,
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            with (
                mock.patch("os.geteuid", return_value=0),
                mock.patch("os.chown"),
                mock.patch("getpass.getpass", return_value="00112233445566778899aabbccddeeff"),
                mock.patch("t28_victron_ble.cli._group_id", return_value=0),
                mock.patch("t28_victron_ble.cli._sidecar_is_active", return_value=False),
                mock.patch("t28_victron_ble.cli._restart_sidecar_only", return_value=True) as restart,
                mock.patch("os.execvp") as execvp,
            ):
                code = configure_main(
                    [
                        "--devices",
                        str(devices),
                        "--keys",
                        str(keys),
                        "--latest",
                        str(latest),
                        "--id",
                        "house",
                        "--address",
                        "aa:bb:cc:dd:ee:ff",
                        "--product",
                        "shunt",
                        "--product-id",
                        "0xA389",
                        "--enable",
                    ]
                )
            self.assertEqual(code, 0)
            execvp.assert_not_called()
            restart.assert_called_once()
            self.assertTrue(devices.is_file())
            self.assertNotIn("execvp", (REPO / "v3" / "t28-victron-ble" / "t28_victron_ble" / "cli.py").read_text(encoding="utf-8"))

    def test_configure_normalizes_12_hex_mac_and_key_file(self) -> None:
        golden = "00112233445566778899aabbccddeeff"
        with tempfile.TemporaryDirectory() as tmp:
            devices = Path(tmp) / "devices.json"
            keys = Path(tmp) / "keys.json"
            key_file = Path(tmp) / "ir.key"
            key_file.write_text("0011:2233:4455:6677:8899:aabb:ccdd:eeff\n", encoding="utf-8")
            os.chmod(key_file, 0o600)
            with (
                mock.patch("os.geteuid", return_value=0),
                mock.patch("os.chown"),
                mock.patch("t28_victron_ble.cli._group_id", return_value=0),
                mock.patch("t28_victron_ble.cli._sidecar_is_active", return_value=False),
                mock.patch("getpass.getpass") as prompt,
            ):
                code = configure_main(
                    [
                        "--devices",
                        str(devices),
                        "--keys",
                        str(keys),
                        "--id",
                        "house-bank",
                        "--address",
                        "020000000001",
                        "--product",
                        "SmartShunt 500A/50mV",
                        "--product-id",
                        "0xA389",
                        "--key-file",
                        str(key_file),
                    ]
                )
            self.assertEqual(code, 0)
            prompt.assert_not_called()
            written = json.loads(devices.read_text(encoding="utf-8"))
            self.assertEqual(written["devices"][0]["address"], "02:00:00:00:00:01")
            stored = json.loads(keys.read_text(encoding="utf-8"))
            self.assertEqual(stored["keys"]["house-bank"], golden)
            self.assertTrue(key_file.is_file())

    def test_configure_shreds_key_file_when_asked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            devices = Path(tmp) / "devices.json"
            keys = Path(tmp) / "keys.json"
            key_file = Path(tmp) / "ir.key"
            key_file.write_text("00112233445566778899aabbccddeeff\n", encoding="utf-8")
            with (
                mock.patch("os.geteuid", return_value=0),
                mock.patch("os.chown"),
                mock.patch("t28_victron_ble.cli._group_id", return_value=0),
                mock.patch("t28_victron_ble.cli._sidecar_is_active", return_value=False),
            ):
                code = configure_main(
                    [
                        "--devices",
                        str(devices),
                        "--keys",
                        str(keys),
                        "--id",
                        "house-bank",
                        "--address",
                        "02:00:00:00:00:01",
                        "--product",
                        "SmartShunt 500A/50mV",
                        "--product-id",
                        "0xA389",
                        "--key-file",
                        str(key_file),
                        "--shred-key-file",
                    ]
                )
            self.assertEqual(code, 0)
            self.assertFalse(key_file.exists())
            self.assertEqual(
                json.loads(keys.read_text(encoding="utf-8"))["keys"]["house-bank"],
                "00112233445566778899aabbccddeeff",
            )

    def test_configure_show_does_not_print_key(self) -> None:
        golden = "00112233445566778899aabbccddeeff"
        with tempfile.TemporaryDirectory() as tmp:
            devices = Path(tmp) / "devices.json"
            keys = Path(tmp) / "keys.json"
            devices.write_text(
                json.dumps(
                    {
                        "devices": [
                            {
                                "id": "house-bank",
                                "address": "020000000001",
                                "product": "SmartShunt 500A/50mV",
                                "productId": "0xA389",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            keys.write_text(json.dumps({"keys": {"house-bank": golden}}), encoding="utf-8")
            buf = io.StringIO()
            with mock.patch("os.geteuid", return_value=0), mock.patch("sys.stdout", buf):
                code = configure_main(["--devices", str(devices), "--keys", str(keys), "--show"])
            output = buf.getvalue()
            self.assertEqual(code, 0)
            self.assertIn("house-bank", output)
            self.assertIn("address=02:00:00:00:00:01", output)
            self.assertIn("productId=0xA389", output)
            self.assertIn("keyPresent=yes", output)
            self.assertNotIn(golden, output)
            self.assertNotIn("0011", output.lower())

    def test_configure_empty_getpass_keeps_existing_key(self) -> None:
        golden = "00112233445566778899aabbccddeeff"
        with tempfile.TemporaryDirectory() as tmp:
            devices = Path(tmp) / "devices.json"
            keys = Path(tmp) / "keys.json"
            devices.write_text(
                json.dumps(
                    {
                        "devices": [
                            {"id": "house-bank", "address": "02:00:00:00:00:01", "product": "SmartShunt", "productId": "0xA389"}
                        ]
                    }
                ),
                encoding="utf-8",
            )
            keys.write_text(json.dumps({"keys": {"house-bank": golden}}), encoding="utf-8")
            buf = io.StringIO()
            with (
                mock.patch("os.geteuid", return_value=0),
                mock.patch("os.chown"),
                mock.patch("t28_victron_ble.cli._group_id", return_value=0),
                mock.patch("t28_victron_ble.cli._sidecar_is_active", return_value=False),
                mock.patch("getpass.getpass", return_value=""),
                mock.patch("sys.stdout", buf),
            ):
                code = configure_main(
                    [
                        "--devices",
                        str(devices),
                        "--keys",
                        str(keys),
                        "--id",
                        "house-bank",
                        "--address",
                        "02:00:00:00:00:01",
                        "--product",
                        "SmartShunt",
                        "--product-id",
                        "0xA389",
                    ]
                )
            self.assertEqual(code, 0)
            self.assertIn("keeping existing key", buf.getvalue())
            self.assertEqual(json.loads(keys.read_text(encoding="utf-8"))["keys"]["house-bank"], golden)

    def test_configure_empty_getpass_dies_without_existing_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            devices = Path(tmp) / "devices.json"
            keys = Path(tmp) / "keys.json"
            with (
                mock.patch("os.geteuid", return_value=0),
                mock.patch("os.chown"),
                mock.patch("t28_victron_ble.cli._group_id", return_value=0),
                mock.patch("getpass.getpass", return_value=""),
            ):
                code = configure_main(
                    [
                        "--devices",
                        str(devices),
                        "--keys",
                        str(keys),
                        "--id",
                        "house-bank",
                        "--address",
                        "02:00:00:00:00:01",
                        "--product",
                        "SmartShunt",
                        "--product-id",
                        "0xA389",
                    ]
                )
            self.assertEqual(code, 1)
            self.assertFalse(keys.is_file())

    def test_configure_bad_mac_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch("os.geteuid", return_value=0), mock.patch("getpass.getpass", return_value="00112233445566778899aabbccddeeff"):
                code = configure_main(
                    [
                        "--devices",
                        str(Path(tmp) / "devices.json"),
                        "--keys",
                        str(Path(tmp) / "keys.json"),
                        "--id",
                        "house-bank",
                        "--address",
                        "c69953bf943",
                        "--product",
                        "SmartShunt",
                        "--product-id",
                        "0xA389",
                    ]
                )
            self.assertEqual(code, 1)

    def test_configure_verify_fresh_and_key_mismatch_and_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            latest = Path(tmp) / "latest.json"
            latest.write_text(
                json.dumps(
                    {
                        "devices": {
                            "house-bank": {
                                "status": "fresh",
                                "batteryVoltageV": 12.53,
                                "batteryCurrentA": None,
                                "stateOfChargePercent": 50.0,
                                "timeToGoMinutes": None,
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            buf = io.StringIO()
            with mock.patch("sys.stdout", buf):
                code = cli_mod._verify_reading(latest, "house-bank", 0)
            self.assertEqual(code, 0)
            self.assertIn("status=fresh", buf.getvalue())
            self.assertIn("V=12.53", buf.getvalue())
            self.assertIn("I=null", buf.getvalue())
            self.assertIn("TTG=null", buf.getvalue())
            latest.write_text(
                json.dumps({"devices": {"house-bank": {"status": "keyMismatch", "batteryVoltageV": None}}}),
                encoding="utf-8",
            )
            self.assertEqual(cli_mod._verify_reading(latest, "house-bank", 0), 2)
            self.assertEqual(cli_mod._verify_reading(Path(tmp) / "missing.json", "house-bank", 0), 3)

    def test_restart_only_touches_sidecar_unit(self) -> None:
        with mock.patch("subprocess.run") as run:
            run.return_value = mock.Mock(returncode=0)
            self.assertTrue(cli_mod._restart_sidecar_only())
            self.assertEqual(run.call_args[0][0], ["systemctl", "restart", "t28-victron-ble.service"])
            self.assertNotIn("bluetooth", " ".join(run.call_args[0][0]))
            self.assertNotIn("t28-v2-receiver", " ".join(run.call_args[0][0]))
            with self.assertRaises(RuntimeError):
                cli_mod._run_systemctl(["restart", "bluetooth.service"])

    def test_status_requires_device_map(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            latest = Path(tmp) / "latest.json"
            latest.write_text(json.dumps({"schemaVersion": 1, "voltage": 12.5}), encoding="utf-8")
            code = status_main(["--latest", str(latest)])
        self.assertEqual(code, 1)

    def test_status_prints_map_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            latest = Path(tmp) / "latest.json"
            latest.write_text(
                json.dumps(
                    {
                        "schemaVersion": 1,
                        "updatedAtUtc": "2026-08-30T00:00:00.000Z",
                        "devices": {"house": {"status": "fresh", "batteryVoltageV": 12.5, "batteryCurrentA": None, "stateOfChargePercent": 50, "timeToGoMinutes": None}},
                    }
                ),
                encoding="utf-8",
            )
            buf = io.StringIO()
            with mock.patch("sys.stdout", buf):
                code = status_main(["--latest", str(latest)])
            self.assertEqual(code, 0)
            self.assertIn("house", buf.getvalue())
