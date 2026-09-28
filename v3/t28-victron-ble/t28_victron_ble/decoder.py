"""T28 decoder for Victron Extra Manufacturer Data, Battery Monitor record 0x02.

Layout follows Victron's published Extra Manufacturer Data document
(2022-12-14) and the keshavdv/victron-ble golden vectors. keshavdv/victron-ble
is used only as a source of encrypted test vectors, not as the service contract.

Unavailable sentinels become JSON null. The 22-bit current sentinel 0x3FFFFF must
be detected on the unsigned field; interpreting it as signed first yields -1 and
would leak as -0.001 A.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from . import PRODUCT_ADVERTISEMENT, RECORD_BATTERY_MONITOR, SCHEMA_VERSION
from .aes128 import victron_ctr_decrypt

MIN_ADVERTISEMENT_BYTES = 8
MIN_PLAINTEXT_BYTES = 15
CURRENT_UNAVAILABLE = 0x3FFFFF
VOLTAGE_UNAVAILABLE = 0x7FFF
TTG_UNAVAILABLE = 0xFFFF
CONSUMED_UNAVAILABLE = 0xFFFFF
SOC_UNAVAILABLE = 0x3FF

ALARM_FLAGS = (
    ("lowVoltage", 0x0001),
    ("highVoltage", 0x0002),
    ("lowSoc", 0x0004),
    ("lowStarterVoltage", 0x0008),
    ("highStarterVoltage", 0x0010),
    ("lowTemperature", 0x0020),
    ("highTemperature", 0x0040),
    ("midVoltage", 0x0080),
    ("overload", 0x0100),
    ("dcRipple", 0x0200),
    ("lowVacOut", 0x0400),
    ("highVacOut", 0x0800),
    ("shortCircuit", 0x1000),
    ("bmsLockout", 0x2000),
)

PRODUCT_NAMES = {
    0xA380: "BMV-710 Smart",
    0xA381: "BMV-712 Smart",
    0xA382: "BMV-710H Smart",
    0xA383: "BMV-712 Smart",
    0xA389: "SmartShunt 500A/50mV",
    0xA38A: "SmartShunt 1000A/50mV",
    0xA38B: "SmartShunt 2000A/50mV",
    0xA38C: "SmartShunt IP67 500A/50mV",
    0xA38D: "SmartShunt IP67 1000A/50mV",
    0xA38E: "SmartShunt IP67 2000A/50mV",
    0xC030: "SmartShunt IP65 500A/50mV",
    0xC031: "SmartShunt IP65 1000A/50mV",
    0xC032: "SmartShunt IP65 2000A/50mV",
    0xC035: "SmartShunt IP65 500A/50mV",
    0xC036: "SmartShunt IP65 1000A/50mV",
    0xC037: "SmartShunt IP65 2000A/50mV",
    0xC038: "SmartShunt 300A/50mV",
}


class DecodeError(ValueError):
    """Advertisement was rejected as truncated, malformed, or otherwise unusable."""


class KeyMismatchError(DecodeError):
    """Advertisement key byte 0 does not match the configured AES-128 key."""


class UnknownRecordError(DecodeError):
    """Product advertisement used a record type other than Battery Monitor 0x02."""


@dataclass(frozen=True)
class AdvertisementHeader:
    model_id: int
    record_type: int
    nonce: int
    key_check: int
    ciphertext: bytes
    trailing_bytes: int


@dataclass(frozen=True)
class BatteryMonitorReading:
    schema_version: int = SCHEMA_VERSION
    product_id: int = 0
    product: str = "unknown"
    record_type: int = RECORD_BATTERY_MONITOR
    nonce: int = 0
    battery_voltage_v: Optional[float] = None
    battery_current_a: Optional[float] = None
    time_to_go_minutes: Optional[int] = None
    state_of_charge_percent: Optional[float] = None
    consumed_ah: Optional[float] = None
    alarm_mask: int = 0
    alarms: dict[str, bool] = field(default_factory=dict)


class BitReader:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self._index = 0

    @property
    def remaining_bits(self) -> int:
        return len(self._data) * 8 - self._index

    def read_unsigned(self, width: int) -> int:
        if width < 1 or self.remaining_bits < width:
            raise DecodeError("truncated bit field")
        value = 0
        for position in range(width):
            byte = self._data[self._index >> 3]
            value |= ((byte >> (self._index & 7)) & 1) << position
            self._index += 1
        return value

    @staticmethod
    def to_signed(value: int, width: int) -> int:
        sign = 1 << (width - 1)
        return value - (1 << width) if value & sign else value


def product_name(model_id: int) -> str:
    return PRODUCT_NAMES.get(model_id, f"unknown (0x{model_id:04X})")


def decode_alarm_flags(mask: int) -> dict[str, bool]:
    return {name: bool(mask & bit) for name, bit in ALARM_FLAGS}


def parse_header(payload: bytes) -> AdvertisementHeader:
    if not isinstance(payload, (bytes, bytearray)):
        raise DecodeError("advertisement payload must be bytes")
    data = bytes(payload)
    if len(data) < MIN_ADVERTISEMENT_BYTES:
        raise DecodeError("advertisement shorter than Extra Manufacturer Data header")
    if data[0] != PRODUCT_ADVERTISEMENT:
        raise DecodeError("not a Victron Product Advertisement (0x10)")
    model_id = int.from_bytes(data[2:4], "little")
    record_type = data[4]
    nonce = int.from_bytes(data[5:7], "little")
    key_check = data[7]
    ciphertext = data[8:]
    return AdvertisementHeader(
        model_id=model_id,
        record_type=record_type,
        nonce=nonce,
        key_check=key_check,
        ciphertext=ciphertext,
        trailing_bytes=0,
    )


def _null_if_sentinel(value: int, sentinel: int, scale: float, signed_width: int | None = None):
    if value == sentinel:
        return None
    if signed_width is not None:
        value = BitReader.to_signed(value, signed_width)
    if scale == 1:
        return value
    return value / scale


def parse_battery_monitor(plaintext: bytes, header: AdvertisementHeader) -> BatteryMonitorReading:
    if len(plaintext) < MIN_PLAINTEXT_BYTES:
        raise DecodeError("Battery Monitor record is truncated")
    reader = BitReader(plaintext)
    time_to_go = reader.read_unsigned(16)
    voltage = reader.read_unsigned(16)
    alarm_mask = reader.read_unsigned(16)
    reader.read_unsigned(16)  # aux overlay; Phase 1 does not publish starter/mid/temp
    reader.read_unsigned(2)  # aux mode
    current = reader.read_unsigned(22)
    consumed = reader.read_unsigned(20)
    soc = reader.read_unsigned(10)
    return BatteryMonitorReading(
        product_id=header.model_id,
        product=product_name(header.model_id),
        record_type=header.record_type,
        nonce=header.nonce,
        battery_voltage_v=_null_if_sentinel(voltage, VOLTAGE_UNAVAILABLE, 100.0, signed_width=16),
        battery_current_a=_null_if_sentinel(current, CURRENT_UNAVAILABLE, 1000.0, signed_width=22),
        time_to_go_minutes=_null_if_sentinel(time_to_go, TTG_UNAVAILABLE, 1),
        state_of_charge_percent=_null_if_sentinel(soc, SOC_UNAVAILABLE, 10.0),
        consumed_ah=(
            None if consumed == CONSUMED_UNAVAILABLE else -consumed / 10.0
        ),
        alarm_mask=alarm_mask,
        alarms=decode_alarm_flags(alarm_mask),
    )


def decrypt_payload(header: AdvertisementHeader, key: bytes) -> bytes:
    if len(key) != 16:
        raise DecodeError("AES-128 key must be 16 bytes")
    if header.key_check != key[0]:
        raise KeyMismatchError("advertisement key does not match configured device key")
    if not header.ciphertext:
        raise DecodeError("encrypted Extra Manufacturer Data is missing")
    return victron_ctr_decrypt(key, header.nonce, header.ciphertext)


def decode_battery_monitor(payload: bytes, key: bytes) -> BatteryMonitorReading:
    header = parse_header(payload)
    if header.record_type != RECORD_BATTERY_MONITOR:
        raise UnknownRecordError(f"unsupported record type 0x{header.record_type:02X}")
    plaintext = decrypt_payload(header, key)
    return parse_battery_monitor(plaintext, header)


def parse_hex_key(value: str) -> bytes:
    """32 hex chars → 16 bytes. Strips whitespace, colons, and dashes. Never log this."""
    text = "".join(char for char in str(value).lower() if char not in " \t\r\n:-")
    if len(text) != 32 or any(char not in "0123456789abcdef" for char in text):
        raise DecodeError("AES-128 key must be 32 hexadecimal characters")
    return bytes.fromhex(text)
