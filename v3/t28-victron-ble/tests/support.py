from __future__ import annotations

from t28_victron_ble.aes128 import victron_ctr_decrypt
from t28_victron_ble.decoder import CURRENT_UNAVAILABLE


class BitWriter:
    def __init__(self) -> None:
        self._bits: list[int] = []

    def write_unsigned(self, value: int, width: int) -> None:
        for position in range(width):
            self._bits.append((value >> position) & 1)

    def to_bytes(self) -> bytes:
        data = bytearray((len(self._bits) + 7) // 8)
        for index, bit in enumerate(self._bits):
            data[index >> 3] |= bit << (index & 7)
        return bytes(data)


def battery_plaintext(
    *,
    ttg: int = 0xFFFF,
    voltage: int = 1253,
    alarm: int = 0,
    aux: int = 0,
    aux_mode: int = 3,
    current: int = 0,
    consumed: int = 500,
    soc: int = 500,
) -> bytes:
    writer = BitWriter()
    writer.write_unsigned(ttg, 16)
    writer.write_unsigned(voltage & 0xFFFF, 16)
    writer.write_unsigned(alarm, 16)
    writer.write_unsigned(aux, 16)
    writer.write_unsigned(aux_mode, 2)
    writer.write_unsigned(current & 0x3FFFFF, 22)
    writer.write_unsigned(consumed, 20)
    writer.write_unsigned(soc, 10)
    return writer.to_bytes()


def encode_advertisement(
    key: bytes,
    plaintext: bytes,
    *,
    model_id: int = 0xA389,
    nonce: int = 0x40B0,
    record_type: int = 0x02,
    extra: bytes = b"",
) -> bytes:
    ciphertext = victron_ctr_decrypt(key, nonce, plaintext)
    return (
        bytes([0x10, 0x02])
        + model_id.to_bytes(2, "little")
        + bytes([record_type])
        + nonce.to_bytes(2, "little")
        + bytes([key[0]])
        + ciphertext
        + extra
    )


# Synthetic public fixture: sequential-byte key; generated AES-CTR packet, not device data.
PUBLIC_KEY = bytes.fromhex("00112233445566778899aabbccddeeff")
PUBLIC_ADVERTISEMENT = bytes.fromhex("100289a302b04000d7f8940e7cf73193ebab1ec2f46596")
UNAVAILABLE_CURRENT = CURRENT_UNAVAILABLE
