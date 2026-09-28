"""Strip key material and credentials from JSON that may leave the Pi."""

from __future__ import annotations

from typing import Any

_FORBIDDEN_FIELDS = frozenset(
    {
        "advertisementkey",
        "rawadvertisement",
        "encryptionkey",
        "aeskey",
        "devicekey",
        "instantreadoutkey",
        "password",
        "passwd",
        "secret",
        "token",
        "keys",
        "key",
    }
)


def _folded_field(name: str) -> str:
    return "".join(char for char in str(name).lower() if char.isalnum())


def is_forbidden_field(name: str) -> bool:
    folded = _folded_field(name)
    if folded in _FORBIDDEN_FIELDS:
        return True
    if "password" in folded or "secret" in folded or "token" in folded:
        return True
    if folded.endswith("key") and folded != "keymismatch":
        return True
    return False


def sanitize_payload(value: Any) -> Any:
    """Drop key material and raw advertisements even if latest.json is hostile."""
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for key, item in value.items():
            if is_forbidden_field(str(key)):
                continue
            cleaned[str(key)] = sanitize_payload(item)
        return cleaned
    if isinstance(value, list):
        return [sanitize_payload(item) for item in value]
    return value
