from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import bcrypt
import jwt
import ow_config as config

TRANSFER_JWT_ALG = "HS256"


def check_token(token_name: str, token: str) -> bool:
    stored_token_hash = getattr(config, token_name, None)
    if stored_token_hash is None:
        return False
    return bcrypt.checkpw(token.encode(), str(stored_token_hash).encode())


def is_safe_job_id(job_id: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9_-]{8,128}", job_id or ""))


def decode_transfer_jwt(token: str, audience: str) -> Optional[dict[str, Any]]:
    secret = getattr(config, "TRANSFER_JWT_SECRET", None)
    if not secret:
        return None
    try:
        return jwt.decode(
            token,
            secret,
            algorithms=[TRANSFER_JWT_ALG],
            audience=audience,
        )
    except jwt.PyJWTError:
        return None


def encode_transfer_jwt(payload: dict[str, Any], audience: str, ttl_seconds: int) -> Optional[str]:
    secret = getattr(config, "TRANSFER_JWT_SECRET", None)
    if not secret:
        return None
    now = datetime.now(timezone.utc)
    token_payload = dict(payload)
    token_payload.update(
        {
            "aud": audience,
            "iss": "storage",
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(seconds=ttl_seconds)).timestamp()),
        }
    )
    return jwt.encode(token_payload, secret, algorithm=TRANSFER_JWT_ALG)
