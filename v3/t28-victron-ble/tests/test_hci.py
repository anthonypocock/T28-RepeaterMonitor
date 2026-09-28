from __future__ import annotations

import asyncio
import socket
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from t28_victron_ble.hci import (
    CHARGER_PRODUCT_ID,
    FIELD_SMARTSHUNT_ADDRESS,
    FIELD_SMARTSHUNT_PRODUCT_ID,
    OPCODE_LE_SET_SCAN_ENABLE,
    OPCODE_LE_SET_SCAN_PARAMETERS,
    SCAN_TYPE_ACTIVE,
    disable_scan_command,
    hcitool_disable_argv,
    hci_filter_events,
    le_set_scan_enable,
    le_set_scan_parameters,
    manufacturer_payload,
    parse_command_complete,
    parse_le_advertising_reports,
    victron_report,
)
from t28_victron_ble.hci_scan import HciPassiveScanner, open_hci_socket
from t28_victron_ble.scanner import PassiveScanUnavailable

from support import PUBLIC_ADVERTISEMENT


def _legacy_report(address: str, ad_data: bytes, rssi: int = -57) -> bytes:
    addr = bytes(int(part, 16) for part in reversed(address.split(":")))
    rssi_byte = rssi & 0xFF
    body = bytes([1, 0x02, 0x01]) + addr + bytes([len(ad_data)]) + ad_data + bytes([rssi_byte])
    return bytes([0x04, 0x3E, len(body) + 1, 0x02]) + body


def _victron_ad(payload: bytes) -> bytes:
    company = bytes([0xE1, 0x02]) + payload
    return bytes([len(company) + 1, 0xFF]) + company


def _charger_payload() -> bytes:
    data = bytearray(PUBLIC_ADVERTISEMENT)
    data[2:4] = CHARGER_PRODUCT_ID.to_bytes(2, "little")
    return bytes(data)


class FakeHciTransport:
    def __init__(self) -> None:
        self.sent: list[bytes] = []
        self.incoming: asyncio.Queue[bytes] = asyncio.Queue()
        self.closed = False

    def send(self, packet: bytes) -> None:
        self.sent.append(packet)
        if packet[:1] == b"\x01":
            complete = bytes([0x04, 0x0E, 4, 1, packet[1], packet[2], 0x00])
            self.incoming.put_nowait(complete)

    async def async_recv(self) -> bytes:
        return await self.incoming.get()

    def close(self) -> None:
        self.closed = True


class HciPacketTests(unittest.TestCase):
    def test_hci_filter_is_16_bytes(self) -> None:
        packed = hci_filter_events()
        self.assertEqual(len(packed), 16)
        self.assertEqual(packed[-2:], b"\x00\x00")

    def test_scan_parameters_are_passive_only(self) -> None:
        packet = le_set_scan_parameters()
        self.assertEqual(packet[4], 0x00)
        with self.assertRaises(ValueError):
            le_set_scan_parameters(scan_type=SCAN_TYPE_ACTIVE)
        self.assertEqual(disable_scan_command(), le_set_scan_enable(False))
        self.assertEqual(hcitool_disable_argv(), ["hcitool", "-i", "hci0", "cmd", "0x08", "0x000C", "00", "00"])

    def test_command_complete_status(self) -> None:
        complete = bytes([0x04, 0x0E, 4, 1, OPCODE_LE_SET_SCAN_PARAMETERS & 0xFF, OPCODE_LE_SET_SCAN_PARAMETERS >> 8, 0x00])
        self.assertEqual(parse_command_complete(complete, OPCODE_LE_SET_SCAN_PARAMETERS), 0)
        self.assertIsNone(parse_command_complete(complete, OPCODE_LE_SET_SCAN_ENABLE))

    def test_smartshunt_report_is_accepted(self) -> None:
        ad = _victron_ad(PUBLIC_ADVERTISEMENT)
        packet = _legacy_report(FIELD_SMARTSHUNT_ADDRESS, ad, -57)
        address, ad_data, rssi = parse_le_advertising_reports(packet)[0]
        self.assertEqual(address, FIELD_SMARTSHUNT_ADDRESS)
        self.assertEqual(rssi, -57)
        self.assertEqual(manufacturer_payload(ad_data), PUBLIC_ADVERTISEMENT)
        report = victron_report(
            address,
            ad_data,
            rssi,
            allowed_addresses={FIELD_SMARTSHUNT_ADDRESS},
            allowed_product_ids={FIELD_SMARTSHUNT_PRODUCT_ID},
        )
        self.assertIsNotNone(report)
        assert report is not None
        self.assertEqual(report.model_id, FIELD_SMARTSHUNT_PRODUCT_ID)
        self.assertEqual(report.record_type, 0x02)

    def test_nearby_charger_is_ignored(self) -> None:
        ad = _victron_ad(_charger_payload())
        packet = _legacy_report("02:00:00:00:00:02", ad, -70)
        address, ad_data, rssi = parse_le_advertising_reports(packet)[0]
        self.assertIsNone(
            victron_report(
                address,
                ad_data,
                rssi,
                allowed_addresses={FIELD_SMARTSHUNT_ADDRESS},
                allowed_product_ids={FIELD_SMARTSHUNT_PRODUCT_ID},
            )
        )
        self.assertIsNone(victron_report(address, ad_data, rssi))


class HciScannerTests(unittest.TestCase):
    def test_start_is_passive_and_stop_runs_on_exit(self) -> None:
        transport = FakeHciTransport()
        observed: list[str] = []
        stops = {"n": 0}

        async def body() -> None:
            scanner = HciPassiveScanner(
                allowed_addresses={FIELD_SMARTSHUNT_ADDRESS},
                allowed_product_ids={FIELD_SMARTSHUNT_PRODUCT_ID},
                transport_factory=lambda: transport,
                stop_hook=lambda: stops.__setitem__("n", stops["n"] + 1),
            )
            ad = _legacy_report(FIELD_SMARTSHUNT_ADDRESS, _victron_ad(PUBLIC_ADVERTISEMENT))
            charger = _legacy_report("02:00:00:00:00:02", _victron_ad(_charger_payload()))

            async def on_ad(observation) -> None:
                observed.append(observation.address)
                raise asyncio.CancelledError

            task = asyncio.create_task(scanner.run(on_ad))
            for _ in range(50):
                if len(transport.sent) >= 2:
                    break
                await asyncio.sleep(0)
            await transport.incoming.put(charger)
            await transport.incoming.put(ad)
            with self.assertRaises(asyncio.CancelledError):
                await task

        asyncio.run(body())
        self.assertEqual(observed, [FIELD_SMARTSHUNT_ADDRESS])
        self.assertTrue(any(pkt == disable_scan_command() for pkt in transport.sent))
        self.assertTrue(transport.closed)
        self.assertEqual(stops["n"], 1)
        self.assertEqual(transport.sent[0][4], 0x00)

    def test_failed_passive_start_fails_closed(self) -> None:
        transport = FakeHciTransport()

        async def fail_start() -> None:
            scanner = HciPassiveScanner(transport_factory=lambda: transport)

            def send_fail(packet: bytes) -> None:
                transport.sent.append(packet)
                complete = bytes([0x04, 0x0E, 4, 1, packet[1], packet[2], 0x12])
                transport.incoming.put_nowait(complete)

            transport.send = send_fail  # type: ignore[method-assign]
            await scanner.run(lambda *_: None)

        with self.assertRaises(PassiveScanUnavailable):
            asyncio.run(fail_start())
        self.assertTrue(any(pkt == disable_scan_command() for pkt in transport.sent))

    def test_open_hci_socket_closes_on_setsockopt_error(self) -> None:
        created: dict[str, object] = {}

        class FakeSock:
            def bind(self, _addr) -> None:
                return None

            def setsockopt(self, *_args) -> None:
                raise OSError(22, "Invalid argument")

            def setblocking(self, _value) -> None:
                return None

            def close(self) -> None:
                created["closed"] = True

        def fake_socket(*_args, **_kwargs):
            sock = FakeSock()
            created["sock"] = sock
            return sock

        with (
            mock.patch.object(socket, "socket", fake_socket),
            mock.patch.object(socket, "AF_BLUETOOTH", 31, create=True),
            mock.patch.object(socket, "BTPROTO_HCI", 1, create=True),
        ):
            with self.assertRaises(PassiveScanUnavailable):
                open_hci_socket("hci0")
        self.assertTrue(created.get("closed"))
