# Snapshot verification — 28 September 2026

- Secret scan of the exact tracked export: no findings (Gitleaks 8.30.1).
- BLE unit suite: 82 tests passed, including decoding, key handling, freshness, fleet configuration and telemetry publishing.
- DSP suite: four tests passed; the real GNU Radio flowgraph test could not run because GNU Radio is unavailable on the verification host.
- .NET build and fleet integration tests: not run; the verification host has no .NET SDK.
- No physical receiver, battery monitor, browser layout or live service acceptance is claimed by this publication.

Demonstration channel/frequency data and synthetic device identifiers replace operational fixtures. Operational installers, original history, field baselines and audio/IQ captures are excluded. Service unit examples and browser assets required by the BLE tests are retained.

## Second publication review

Reviewed tracked files and public branch/tag history again. Removed residual organisational labels and example location references. Replaced the inherited, unverified-provenance BLE test key and ciphertext with a sequential-byte synthetic key and generated ciphertext; all 82 BLE tests pass. The original fixture was labelled public but its provenance was not independently established. The original public snapshot was briefly accessible before this correction; rewriting the branch cannot guarantee removal of third-party copies or cached objects.
