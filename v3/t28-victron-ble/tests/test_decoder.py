from __future__ import annotations

import random
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from t28_victron_ble.decoder import (
    CURRENT_UNAVAILABLE,
    DecodeError,
    KeyMismatchError,
    UnknownRecordError,
    decode_battery_monitor,
    parse_battery_monitor,
    parse_header,
    parse_hex_key,
)

from support import (
    PUBLIC_ADVERTISEMENT,
    PUBLIC_KEY,
    battery_plaintext,
    encode_advertisement,
)


class DecoderGoldenVectorTests(unittest.TestCase):
    def test_public_encrypted_smartshunt_vector(self) -> None:
        reading = decode_battery_monitor(PUBLIC_ADVERTISEMENT, PUBLIC_KEY)
        self.assertEqual(reading.product, "SmartShunt 500A/50mV")
        self.assertEqual(reading.product_id, 0xA389)
        self.assertEqual(reading.battery_voltage_v, 12.53)
        self.assertEqual(reading.battery_current_a, 0)
        self.assertIsNone(reading.time_to_go_minutes)
        self.assertEqual(reading.state_of_charge_percent, 50.0)
        self.assertEqual(reading.consumed_ah, -50.0)
        self.assertEqual(reading.alarm_mask, 0)
        self.assertFalse(any(reading.alarms.values()))

    def test_published_decrypted_payload(self) -> None:
        header = parse_header(PUBLIC_ADVERTISEMENT)
        reading = parse_battery_monitor(bytes.fromhex("ffffe50400000000030000f40140df03"), header)
        self.assertEqual(reading.battery_voltage_v, 12.53)
        self.assertIsNone(reading.time_to_go_minutes)


class CurrentAndSentinelTests(unittest.TestCase):
    def test_positive_and_negative_current(self) -> None:
        key = bytes(range(16))
        charging = encode_advertisement(key, battery_plaintext(current=1500, ttg=90, voltage=1340))
        discharging = encode_advertisement(key, battery_plaintext(current=BitWriterSigned(-2500), ttg=40))
        self.assertEqual(decode_battery_monitor(charging, key).battery_current_a, 1.5)
        self.assertEqual(decode_battery_monitor(discharging, key).battery_current_a, -2.5)

    def test_unavailable_current_is_null_not_minus_one_milliamp(self) -> None:
        key = bytes(range(16))
        packet = encode_advertisement(key, battery_plaintext(current=CURRENT_UNAVAILABLE))
        reading = decode_battery_monitor(packet, key)
        self.assertIsNone(reading.battery_current_a)
        # The signed mis-parse of 0x3FFFFF is -1 -> -0.001 A. That must never leak.
        self.assertNotEqual(reading.battery_current_a, -0.001)

    def test_every_unavailable_sentinel_is_json_null(self) -> None:
        key = bytes(range(16))
        packet = encode_advertisement(
            key,
            battery_plaintext(
                ttg=0xFFFF,
                voltage=0x7FFF,
                current=CURRENT_UNAVAILABLE,
                consumed=0xFFFFF,
                soc=0x3FF,
            ),
        )
        reading = decode_battery_monitor(packet, key)
        self.assertIsNone(reading.time_to_go_minutes)
        self.assertIsNone(reading.battery_voltage_v)
        self.assertIsNone(reading.battery_current_a)
        self.assertIsNone(reading.consumed_ah)
        self.assertIsNone(reading.state_of_charge_percent)

    def test_zero_values_are_real_zeros_not_sentinels(self) -> None:
        key = bytes(range(16))
        packet = encode_advertisement(
            key, battery_plaintext(ttg=0, voltage=0, current=0, consumed=0, soc=0)
        )
        reading = decode_battery_monitor(packet, key)
        self.assertEqual(reading.time_to_go_minutes, 0)
        self.assertEqual(reading.battery_voltage_v, 0.0)
        self.assertEqual(reading.battery_current_a, 0.0)
        self.assertEqual(reading.consumed_ah, 0.0)
        self.assertEqual(reading.state_of_charge_percent, 0.0)


def BitWriterSigned(value: int) -> int:
    return value & 0x3FFFFF


class AlarmAndRecordTests(unittest.TestCase):
    def test_combined_alarms(self) -> None:
        key = bytes(range(16))
        mask = 0x0001 | 0x0004 | 0x0040 | 0x2000
        reading = decode_battery_monitor(encode_advertisement(key, battery_plaintext(alarm=mask)), key)
        self.assertEqual(reading.alarm_mask, mask)
        self.assertTrue(reading.alarms["lowVoltage"])
        self.assertTrue(reading.alarms["lowSoc"])
        self.assertTrue(reading.alarms["highTemperature"])
        self.assertTrue(reading.alarms["bmsLockout"])
        self.assertFalse(reading.alarms["highVoltage"])

    def test_wrong_key_is_key_mismatch(self) -> None:
        with self.assertRaises(KeyMismatchError):
            decode_battery_monitor(PUBLIC_ADVERTISEMENT, bytes.fromhex("ff" * 16))

    def test_matching_prefix_wrong_remainder_does_not_crash(self) -> None:
        wrong = bytes([PUBLIC_KEY[0]]) + bytes(15)
        reading = decode_battery_monitor(PUBLIC_ADVERTISEMENT, wrong)
        self.assertNotEqual(reading.battery_voltage_v, 12.53)

    def test_unknown_solar_record_is_rejected(self) -> None:
        key = bytes(range(16))
        packet = encode_advertisement(key, battery_plaintext(), record_type=0x01)
        with self.assertRaises(UnknownRecordError):
            decode_battery_monitor(packet, key)

    def test_truncated_header_and_payload(self) -> None:
        with self.assertRaises(DecodeError):
            decode_battery_monitor(b"\x10\x02", PUBLIC_KEY)
        key = bytes(range(16))
        header = encode_advertisement(key, battery_plaintext())[:8]
        with self.assertRaises(DecodeError):
            decode_battery_monitor(header, key)

    def test_extended_trailing_bytes_are_tolerated(self) -> None:
        key = bytes(range(16))
        packet = encode_advertisement(key, battery_plaintext(voltage=1280), extra=b"\xaa\xbb\xcc\xdd")
        self.assertEqual(decode_battery_monitor(packet, key).battery_voltage_v, 12.80)

    def test_not_product_advertisement(self) -> None:
        with self.assertRaises(DecodeError):
            decode_battery_monitor(b"\x01" + PUBLIC_ADVERTISEMENT[1:], PUBLIC_KEY)

    def test_parse_hex_key(self) -> None:
        self.assertEqual(parse_hex_key(" 0011 " + "2233445566778899aabbccddeeff"), PUBLIC_KEY)
        self.assertEqual(parse_hex_key("0011:2233:4455:6677:8899:aabb:ccdd:eeff"), PUBLIC_KEY)
        self.assertEqual(parse_hex_key("00112233-4455-6677-8899-aabb-ccdd-eeff"), PUBLIC_KEY)
        with self.assertRaises(DecodeError):
            parse_hex_key("abcd")


class FuzzTests(unittest.TestCase):
    def test_random_packets_do_not_crash_the_decoder(self) -> None:
        rng = random.Random(42)
        key = bytes(range(16))
        for _ in range(200):
            blob = bytes(rng.getrandbits(8) for _ in range(rng.randint(0, 40)))
            try:
                decode_battery_monitor(blob, key)
            except (DecodeError, KeyMismatchError, UnknownRecordError):
                continue
