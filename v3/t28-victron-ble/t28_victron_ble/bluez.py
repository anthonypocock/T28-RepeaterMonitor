"""AdvertisementMonitor is not the production scanner on demo-receiver.

hci0 has no org.bluez.AdvertisementMonitorManager1 unless Experimental is
enabled and bluetoothd is restarted. The installer must not do that. Use
`hci_scan.HciPassiveScanner` (`hcitool lescan --passive` equivalent).
"""

from .hci import PASSIVE_UNAVAILABLE as EXPERIMENTAL_HINT
from .hci_scan import HciPassiveScanner as BlueZPassiveMonitor

__all__ = ["BlueZPassiveMonitor", "EXPERIMENTAL_HINT"]
