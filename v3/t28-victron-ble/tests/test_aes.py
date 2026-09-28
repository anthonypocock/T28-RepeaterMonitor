import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from t28_victron_ble.aes128 import aes128_ctr, encrypt_block, victron_ctr_decrypt


class Aes128Tests(unittest.TestCase):
    def test_fips197_aes128_block(self) -> None:
        key = bytes.fromhex("000102030405060708090a0b0c0d0e0f")
        plain = bytes.fromhex("00112233445566778899aabbccddeeff")
        expect = bytes.fromhex("69c4e0d86a7b0430d8cdb78070b4c55a")
        self.assertEqual(encrypt_block(key, plain), expect)

    def test_nist_sp800_38a_ctr(self) -> None:
        key = bytes.fromhex("2b7e151628aed2a6abf7158809cf4f3c")
        counter = bytes.fromhex("f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff")
        plain = bytes.fromhex(
            "6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e51"
            "30c81c46a35ce411e5fbc1191a0a52eff69f2445df4f9b17ad2b417be66c3710"
        )
        expect = bytes.fromhex(
            "874d6191b620e3261bef6864990db6ce9806f66b7970fdff8617187bb9fffdff"
            "5ae4df3edbd5d35e5b4f09020db03eab1e031dda2fbe03d1792170a0f3009cee"
        )
        self.assertEqual(aes128_ctr(key, counter, plain, little_endian=False), expect)

    def test_victron_public_vector_keystream(self) -> None:
        key = bytes.fromhex("00112233445566778899aabbccddeeff")
        nonce = 0x40B0
        ciphertext = bytes.fromhex("d7f8940e7cf73193ebab1ec2f46596")
        plaintext = victron_ctr_decrypt(key, nonce, ciphertext)
        self.assertTrue(plaintext.startswith(bytes.fromhex("ffffe50400000000030000f40140df")))
