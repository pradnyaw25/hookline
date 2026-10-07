"""Standard Webhooks signature verification (https://www.standardwebhooks.com).

The service (your code, stage 9) signs. This verifier is kept independent on purpose:
it is the receiver's view of the world, so a bug in your signer can't hide in a shared helper.

Signed content is ``f"{msg_id}.{timestamp}.{body}"``; the signature header carries one or
more space-separated ``v1,<base64 hmac-sha256>`` entries so secret rotation can overlap.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import time

TOLERANCE_SECONDS = 5 * 60


def _secret_bytes(secret: str) -> bytes:
    if secret.startswith("whsec_"):
        return base64.b64decode(secret[len("whsec_") :])
    return secret.encode()


def sign(secret: str, msg_id: str, timestamp: int, body: bytes) -> str:
    """Return the ``v1,...`` signature for a payload. Used by tests; your service writes its own."""
    to_sign = f"{msg_id}.{timestamp}.".encode() + body
    digest = hmac.new(_secret_bytes(secret), to_sign, hashlib.sha256).digest()
    return "v1," + base64.b64encode(digest).decode()


def verify(
    secret: str, headers: dict[str, str], body: bytes, *, now: float | None = None
) -> str | None:
    """Return None when valid, otherwise a short reason string."""
    msg_id = headers.get("webhook-id")
    ts_raw = headers.get("webhook-timestamp")
    sig_header = headers.get("webhook-signature")
    if not (msg_id and ts_raw and sig_header):
        return "missing headers"
    try:
        ts = int(ts_raw)
    except ValueError:
        return "bad timestamp"
    if abs((now if now is not None else time.time()) - ts) > TOLERANCE_SECONDS:
        return "timestamp outside tolerance"
    expected = sign(secret, msg_id, ts, body)
    for candidate in sig_header.split():
        if hmac.compare_digest(candidate, expected):
            return None
    return "no matching signature"
