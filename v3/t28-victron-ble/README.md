# Optional Victron Bluetooth telemetry

Passive Battery Monitor (record 0x02) advertisement decoding, latest-value storage, freshness/error reporting and optional MQTT transport. AES-128-CTR keys belong in local device configuration, never in this repository. The scanner and network publisher are separate components so Bluetooth failures do not control the radio service.

Run `python3 -m unittest discover -s tests` from this directory. Deployment installers and real device records are intentionally omitted from this public source snapshot. Physical-device acceptance and long-running field soak remain separate from unit testing.
