"""Production scanner: HCI/mgmt passive LE scan.

demo-receiver has no org.bluez.AdvertisementMonitorManager1 unless Experimental
is enabled and bluetoothd is restarted. This sidecar must not do that.
btmgmt find has no -p. Use LE Set Scan Parameters type=0x00 (same as
`hcitool lescan --passive`) and always disable scan on exit.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import subprocess
from datetime import datetime, timezone
from typing import Callable, Optional

from .engine import AdvertisementObservation
from .hci import (
    OPCODE_LE_SET_SCAN_ENABLE,
    OPCODE_LE_SET_SCAN_PARAMETERS,
    PASSIVE_UNAVAILABLE,
    disable_scan_command,
    hci_filter_events,
    hcitool_disable_argv,
    le_set_scan_enable,
    le_set_scan_parameters,
    parse_command_complete,
    parse_le_advertising_reports,
    victron_report,
)
from .scanner import LostCallback, ObservationCallback, PassiveScanUnavailable

LOGGER = logging.getLogger("t28_victron_ble.hci")

SOL_HCI = getattr(socket, "SOL_HCI", 0)
HCI_FILTER = getattr(socket, "HCI_FILTER", 2)


class HciTransport:
    def send(self, packet: bytes) -> None:
        raise NotImplementedError

    def recv(self, n: int = 512) -> bytes:
        raise NotImplementedError

    def fileno(self) -> int:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


class SocketHciTransport(HciTransport):
    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock

    def send(self, packet: bytes) -> None:
        self._sock.send(packet)

    def recv(self, n: int = 512) -> bytes:
        return self._sock.recv(n)

    def fileno(self) -> int:
        return self._sock.fileno()

    def close(self) -> None:
        try:
            self._sock.close()
        except OSError:
            pass


def open_hci_socket(adapter: str = "hci0") -> SocketHciTransport:
    if not hasattr(socket, "AF_BLUETOOTH") or not hasattr(socket, "BTPROTO_HCI"):
        raise PassiveScanUnavailable(PASSIVE_UNAVAILABLE)
    index = 0
    if adapter.startswith("hci"):
        try:
            index = int(adapter[3:] or "0")
        except ValueError as exc:
            raise PassiveScanUnavailable(f"invalid adapter {adapter}") from exc
    sock = None
    try:
        sock = socket.socket(socket.AF_BLUETOOTH, socket.SOCK_RAW, socket.BTPROTO_HCI)
        sock.bind((index,))
        sock.setsockopt(SOL_HCI, HCI_FILTER, hci_filter_events())
        sock.setblocking(False)
    except OSError as exc:
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
        raise PassiveScanUnavailable(PASSIVE_UNAVAILABLE) from exc
    return SocketHciTransport(sock)


class HciPassiveScanner:
    """Passive-only LE scanner. Always sends LE Set Scan Enable 00 00 on exit."""

    def __init__(
        self,
        *,
        adapter: str = "hci0",
        allowed_addresses: Optional[set[str]] = None,
        allowed_product_ids: Optional[set[int]] = None,
        transport_factory: Optional[Callable[[], HciTransport]] = None,
        stop_hook: Optional[Callable[[], None]] = None,
    ) -> None:
        self.adapter = adapter or "hci0"
        self.allowed_addresses = {item.lower() for item in allowed_addresses} if allowed_addresses else None
        self.allowed_product_ids = set(allowed_product_ids) if allowed_product_ids else None
        self._transport_factory = transport_factory or (lambda: open_hci_socket(self.adapter))
        self._stop_hook = stop_hook
        self._transport: Optional[HciTransport] = None

    async def run(
        self,
        on_advertisement: ObservationCallback,
        on_device_lost: Optional[LostCallback] = None,
    ) -> None:
        del on_device_lost
        transport = self._transport_factory()
        self._transport = transport
        loop = asyncio.get_running_loop()
        try:
            await self._require_passive_start(loop, transport)
            while True:
                packet = await self._recv(loop, transport)
                if not packet:
                    continue
                for address, ad_data, rssi in parse_le_advertising_reports(packet):
                    report = victron_report(
                        address,
                        ad_data,
                        rssi,
                        allowed_addresses=self.allowed_addresses,
                        allowed_product_ids=self.allowed_product_ids,
                    )
                    if report is None:
                        continue
                    observation = AdvertisementObservation(
                        report.address,
                        report.payload,
                        report.rssi,
                        datetime.now(timezone.utc),
                    )
                    result = on_advertisement(observation)
                    if asyncio.iscoroutine(result):
                        await result
        finally:
            self.stop_scan(transport)

    async def _require_passive_start(self, loop: asyncio.AbstractEventLoop, transport: HciTransport) -> None:
        try:
            params = le_set_scan_parameters()
        except ValueError as exc:
            raise PassiveScanUnavailable(PASSIVE_UNAVAILABLE) from exc
        transport.send(params)
        status = await self._wait_complete(loop, transport, OPCODE_LE_SET_SCAN_PARAMETERS)
        if status != 0:
            raise PassiveScanUnavailable(PASSIVE_UNAVAILABLE)
        transport.send(le_set_scan_enable(True, filter_duplicates=False))
        status = await self._wait_complete(loop, transport, OPCODE_LE_SET_SCAN_ENABLE)
        if status != 0:
            raise PassiveScanUnavailable(PASSIVE_UNAVAILABLE)
        LOGGER.info("HCI passive LE scan enabled on %s (type 0x00)", self.adapter)

    async def _wait_complete(
        self,
        loop: asyncio.AbstractEventLoop,
        transport: HciTransport,
        opcode: int,
        timeout: float = 2.0,
    ) -> int:
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            remaining = deadline - loop.time()
            try:
                packet = await asyncio.wait_for(self._recv(loop, transport), timeout=remaining)
            except asyncio.TimeoutError as exc:
                raise PassiveScanUnavailable(PASSIVE_UNAVAILABLE) from exc
            status = parse_command_complete(packet, opcode)
            if status is not None:
                return status
        raise PassiveScanUnavailable(PASSIVE_UNAVAILABLE)

    async def _recv(self, loop: asyncio.AbstractEventLoop, transport: HciTransport) -> bytes:
        async_recv = getattr(transport, "async_recv", None)
        if async_recv is not None:
            return await async_recv()
        sock = getattr(transport, "_sock", None)
        if sock is None:
            raise PassiveScanUnavailable(PASSIVE_UNAVAILABLE)
        return await loop.sock_recv(sock, 512)

    def stop_scan(self, transport: Optional[HciTransport] = None) -> None:
        """Must run on every exit. Scan stays on if this is skipped."""
        active = transport or self._transport
        if active is not None:
            try:
                active.send(disable_scan_command())
            except OSError:
                LOGGER.warning("HCI disable-scan send failed; trying hcitool")
            try:
                active.close()
            except OSError:
                pass
        self._transport = None
        argv = hcitool_disable_argv(self.adapter)
        try:
            subprocess.run(argv, check=False, capture_output=True, timeout=2)
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
            pass
        if self._stop_hook is not None:
            self._stop_hook()
        LOGGER.info("HCI LE scan disabled (%s)", " ".join(argv))
