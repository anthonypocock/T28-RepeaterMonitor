"""t28-victron-ble service entry. Isolated from t28-v2-receiver."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

from .config import ConfigError, load_service_config
from .engine import VictronEngine
from .scanner import BoundedBackoff, PassiveScanUnavailable, run_with_backoff
from .store import HistoryStore, LatestStore

LOGGER = logging.getLogger("t28_victron_ble")


class _FlushingStreamHandler(logging.StreamHandler):
    """Journal/stderr must see the line immediately. systemd was silent on the field bug."""

    def emit(self, record: logging.LogRecord) -> None:
        super().emit(record)
        self.flush()


def configure_logging() -> None:
    handler = _FlushingStreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s t28-victron-ble %(levelname)s %(message)s"))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(os.environ.get("T28_VICTRON_LOG", "INFO"))


def load_runtime_config(args: argparse.Namespace):
    devices = Path(args.devices)
    keys = Path(args.keys) if args.keys else Path("/etc/t28-victron/keys.json")
    return load_service_config(devices, keys, run_dir=Path(args.run_dir) if args.run_dir else None)


async def serve(config, source) -> None:
    store = LatestStore(config.latest_path)
    history = None
    if config.history_enabled and config.history_path is not None:
        history = HistoryStore(
            config.history_path,
            interval_seconds=config.history_interval_seconds,
            max_bytes=config.history_max_bytes,
        )
    engine = VictronEngine(config, store, history=history)
    stop = asyncio.Event()

    def request_stop() -> None:
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, request_stop)
        except NotImplementedError:
            pass

    async def on_ad(observation) -> None:
        engine.handle_advertisement(observation)

    async def on_lost(address: str) -> None:
        engine.mark_device_lost(address)

    async def stale_loop() -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=5)
            except asyncio.TimeoutError:
                engine.refresh_stale()

    async def scan_once() -> None:
        await source.run(on_ad, on_lost)

    async def scan_loop() -> None:
        await run_with_backoff(
            scan_once,
            backoff=BoundedBackoff(1.0, 30.0, 2.0),
            should_continue=lambda: not stop.is_set(),
        )

    stale_task = asyncio.create_task(stale_loop())
    scan_task = asyncio.create_task(scan_loop())
    stopper = asyncio.create_task(stop.wait())
    await asyncio.wait({scan_task, stopper}, return_when=asyncio.FIRST_COMPLETED)
    scan_error: BaseException | None = None
    if scan_task.done() and not scan_task.cancelled():
        scan_error = scan_task.exception()
        if scan_error is not None:
            LOGGER.error("scan task failed: %s", scan_error)
    stop.set()
    for task in (scan_task, stale_task, stopper):
        task.cancel()
    await asyncio.gather(scan_task, stale_task, stopper, return_exceptions=True)
    LOGGER.info("t28-victron-ble stopped")
    if scan_error is not None:
        raise scan_error


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="t28-victron-ble")
    parser.add_argument("--devices", default="/etc/t28-victron/devices.json")
    parser.add_argument("--keys", default="")
    parser.add_argument("--run-dir", default="")
    parser.add_argument("--adapter", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = build_parser().parse_args(argv)
    try:
        config = load_runtime_config(args)
    except ConfigError as exc:
        LOGGER.error("refusing to start without valid config: %s", exc)
        return 2
    try:
        from .hci_scan import HciPassiveScanner

        source = HciPassiveScanner(
            adapter=args.adapter or "hci0",
            allowed_addresses={device.address for device in config.devices},
            allowed_product_ids={device.product_id for device in config.devices if device.product_id is not None},
        )
        asyncio.run(serve(config, source))
    except PassiveScanUnavailable as exc:
        LOGGER.error("%s", exc)
        return 3
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        LOGGER.error("sidecar failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
