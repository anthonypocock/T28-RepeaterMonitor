"""HCI LE passive scan packets. No D-Bus AdvertisementMonitor, no Bleak.

Passive scan behaviour: `hcitool lescan --passive` is Type Passive
(0x00). Scan stays on after the tool exits until
`hcitool cmd 0x08 0x000C 00 00`. This module emits that disable command.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Iterable, Optional

from . import PRODUCT_ADVERTISEMENT, RECORD_BATTERY_MONITOR, VICTRON_COMPANY_ID

HCI_COMMAND_PKT = 0x01
HCI_EVENT_PKT = 0x04
HCI_LE_META = 0x3E
HCI_COMMAND_COMPLETE = 0x0E
LE_ADVERTISING_REPORT = 0x02
LE_EXT_ADVERTISING_REPORT = 0x0D
OGF_LE = 0x08
OCF_LE_SET_SCAN_PARAMETERS = 0x000B
OCF_LE_SET_SCAN_ENABLE = 0x000C
OPCODE_LE_SET_SCAN_PARAMETERS = (OGF_LE << 10) | OCF_LE_SET_SCAN_PARAMETERS
OPCODE_LE_SET_SCAN_ENABLE = (OGF_LE << 10) | OCF_LE_SET_SCAN_ENABLE
SCAN_TYPE_PASSIVE = 0x00
SCAN_TYPE_ACTIVE = 0x01
AD_MANUFACTURER = 0xFF

# Charger product type. Never decode as a SmartShunt.
CHARGER_PRODUCT_ID = 0xA305
FIELD_SMARTSHUNT_PRODUCT_ID = 0xA389
FIELD_SMARTSHUNT_ADDRESS = "02:00:00:00:00:01"

PASSIVE_UNAVAILABLE = (
    "HCI/mgmt passive LE scan is unavailable. This sidecar will not use BlueZ "
    "AdvertisementMonitor1 (needs Experimental + bluetoothd restart) and will "
    "not use Bleak or StartDiscovery. Do not restart bluetoothd from the installer."
)


@dataclass(frozen=True)
class AdvertisingReport:
    address: str
    payload: bytes
    rssi: Optional[int]
    model_id: int
    record_type: int


def hci_command(opcode: int, payload: bytes = b"") -> bytes:
    return bytes([HCI_COMMAND_PKT, opcode & 0xFF, (opcode >> 8) & 0xFF, len(payload)]) + payload


def le_set_scan_parameters(*, scan_type: int = SCAN_TYPE_PASSIVE, interval: int = 0x0010, window: int = 0x0010) -> bytes:
    if scan_type != SCAN_TYPE_PASSIVE:
        raise ValueError("refusing non-passive LE scan type")
    return hci_command(
        OPCODE_LE_SET_SCAN_PARAMETERS,
        struct.pack("<BHHBB", scan_type, interval, window, 0x00, 0x00),
    )


def le_set_scan_enable(enabled: bool, filter_duplicates: bool = False) -> bytes:
    return hci_command(OPCODE_LE_SET_SCAN_ENABLE, bytes([1 if enabled else 0, 1 if filter_duplicates else 0]))


def disable_scan_command() -> bytes:
    """Same payload as `hcitool cmd 0x08 0x000C 00 00`."""
    return le_set_scan_enable(False)


def hcitool_disable_argv(adapter: str = "hci0") -> list[str]:
    return ["hcitool", "-i", adapter, "cmd", "0x08", "0x000C", "00", "00"]


def parse_command_complete(packet: bytes, expected_opcode: int) -> Optional[int]:
    """Return HCI status byte, or None if this event is not the matching complete."""
    if len(packet) < 7 or packet[0] != HCI_EVENT_PKT or packet[1] != HCI_COMMAND_COMPLETE:
        return None
    opcode = packet[4] | (packet[5] << 8)
    if opcode != expected_opcode:
        return None
    return packet[6]


def iter_ad_structures(data: bytes) -> Iterable[tuple[int, bytes]]:
    index = 0
    while index < len(data):
        length = data[index]
        if length == 0:
            break
        if index + 1 + length > len(data):
            break
        yield data[index + 1], data[index + 2 : index + 1 + length]
        index += 1 + length


def manufacturer_payload(ad_data: bytes, company_id: int = VICTRON_COMPANY_ID) -> Optional[bytes]:
    for ad_type, value in iter_ad_structures(ad_data):
        if ad_type != AD_MANUFACTURER or len(value) < 3:
            continue
        if int.from_bytes(value[:2], "little") != company_id:
            continue
        return bytes(value[2:])
    return None


def parse_bdaddr(raw: bytes) -> str:
    return ":".join(f"{byte:02x}" for byte in reversed(raw))


def parse_le_advertising_reports(packet: bytes) -> list[tuple[str, bytes, Optional[int]]]:
    """Parse one HCI LE Meta Advertising Report into (address, ad_data, rssi)."""
    if len(packet) < 5 or packet[0] != HCI_EVENT_PKT or packet[1] != HCI_LE_META:
        return []
    subevent = packet[3]
    body = packet[4:]
    if subevent == LE_ADVERTISING_REPORT:
        return _parse_legacy_reports(body)
    if subevent == LE_EXT_ADVERTISING_REPORT:
        return _parse_extended_reports(body)
    return []


def _parse_legacy_reports(body: bytes) -> list[tuple[str, bytes, Optional[int]]]:
    if not body:
        return []
    count = body[0]
    index = 1
    reports: list[tuple[str, bytes, Optional[int]]] = []
    for _ in range(count):
        if index + 9 > len(body):
            break
        index += 1  # event type
        index += 1  # addr type
        address = parse_bdaddr(body[index : index + 6])
        index += 6
        data_len = body[index]
        index += 1
        if index + data_len + 1 > len(body):
            break
        ad_data = body[index : index + data_len]
        index += data_len
        rssi_byte = body[index]
        index += 1
        rssi = rssi_byte - 256 if rssi_byte > 127 else rssi_byte
        reports.append((address, bytes(ad_data), rssi))
    return reports


def _parse_extended_reports(body: bytes) -> list[tuple[str, bytes, Optional[int]]]:
    if len(body) < 2:
        return []
    count = body[0]
    index = 1
    reports: list[tuple[str, bytes, Optional[int]]] = []
    for _ in range(count):
        if index + 24 > len(body):
            break
        index += 2  # event type
        index += 1  # addr type
        address = parse_bdaddr(body[index : index + 6])
        index += 6
        index += 1  # primary PHY
        index += 1  # secondary PHY
        index += 1  # SID
        index += 1  # tx power
        rssi_byte = body[index]
        index += 1
        index += 2  # periodic interval
        index += 1  # direct addr type
        index += 6  # direct addr
        data_len = body[index]
        index += 1
        if index + data_len > len(body):
            break
        ad_data = body[index : index + data_len]
        index += data_len
        rssi = rssi_byte - 256 if rssi_byte > 127 else rssi_byte
        reports.append((address, bytes(ad_data), rssi))
    return reports


def victron_report(
    address: str,
    ad_data: bytes,
    rssi: Optional[int],
    *,
    allowed_addresses: Optional[set[str]] = None,
    allowed_product_ids: Optional[set[int]] = None,
) -> Optional[AdvertisingReport]:
    """Accept Battery Monitor 0x02 Instant Readout only. Ignore the nearby charger."""
    payload = manufacturer_payload(ad_data)
    if payload is None or len(payload) < 5:
        return None
    if payload[0] != PRODUCT_ADVERTISEMENT:
        return None
    model_id = int.from_bytes(payload[2:4], "little")
    record_type = payload[4]
    if model_id == CHARGER_PRODUCT_ID or record_type != RECORD_BATTERY_MONITOR:
        return None
    wanted_addr = address.lower()
    if allowed_addresses is not None and wanted_addr not in {item.lower() for item in allowed_addresses}:
        return None
    if allowed_product_ids and model_id not in allowed_product_ids:
        return None
    return AdvertisingReport(
        address=wanted_addr,
        payload=payload,
        rssi=rssi,
        model_id=model_id,
        record_type=record_type,
    )


def hci_filter_events() -> bytes:
    """struct hci_filter: HCI events, Command Complete + LE Meta.

    Kernel sizeof(struct hci_filter) is 16: three u32, one u16, then 2 bytes
    alignment. A 14-byte setsockopt value returns EINVAL on Debian 13 / BlueZ
    5.82 (demo-receiver).
    """
    type_mask = 1 << HCI_EVENT_PKT
    event_mask0 = (1 << HCI_COMMAND_COMPLETE) | (1 << 0x0F)
    event_mask1 = 1 << (HCI_LE_META - 32)
    return struct.pack("<IIIHxx", type_mask, event_mask0, event_mask1, 0)
