# T28 Repeater Monitor

Remote radio monitoring and optional Bluetooth battery telemetry, developed with AI assistance to address a practical organisational capability gap.

The application connects a GNU Radio receiver to a .NET fleet hub, providing authenticated receiver sessions, bounded audio queues, receiver lifecycle management and browser listening. The separate Victron BLE component reads battery-monitor advertisements and reports voltage, current, state of charge and freshness without making the radio pipeline depend on Bluetooth.

## Source map

- `v2/T28.V2.Receiver`, `v2/T28.V2.Server`, `v2/T28.V2.Contracts`: receiver, hub and protocol.
- `v2/gnuradio`: receive pipeline and signal-processing tests.
- `v2/T28.V2.Tests`: protocol, lifecycle and fleet checks.
- `v3/t28-victron-ble`: optional battery telemetry, decoder, freshness handling and MQTT integration.

## Public snapshot boundaries

Deployment installers, operational baselines, field notes, recordings and legacy IoT implementations are excluded. Channel/location fixtures have been replaced with demonstration values; they are not an approved frequency plan. Documentation addresses are examples only. Supply your own authorised receiver configuration.

This is source for technical review, not a ready-to-install operational distribution. Hardware acceptance, RF reception and SmartShunt field soak are not established by these software tests.

## Local verification

Requires .NET 8 and Python 3. GNU Radio and receiver hardware are required for actual reception.

```sh
dotnet build v2/T28.V2.sln
python3 v2/gnuradio/test_t28_v2_receive.py
cd v3/t28-victron-ble
python3 -m unittest discover -s tests
```

Core BLE scanning uses the Python standard library. Optional MQTT publishing uses the pinned dependency in `requirements.txt`.

See [publication boundaries](PUBLICATION.md) and preserved notices under `v2/T28.V2.Server/vendor/libopus-wasm/`.
