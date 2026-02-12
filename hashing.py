"""Hashing utilities for Enhanced Conversions.

All PII is normalized and SHA-256 hashed before being sent to ad platforms.
No plaintext PII ever leaves our system.
"""

import hashlib
from typing import Optional

_GMAIL_DOMAINS = {'gmail.com', 'googlemail.com'}


def _sha256_hex(value: str) -> str:
    """Return the SHA-256 hex digest of a UTF-8 string."""
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def normalize_and_hash_email(email: Optional[str]) -> Optional[str]:
    """Normalize and SHA-256 hash an email address.

    Normalization:
    - Strip whitespace, lowercase
    - For Gmail/Googlemail: remove dots and plus-suffixes from the username

    Returns None if email is None or empty.
    """
    if not email or not str(email).strip():
        return None

    email = str(email).strip().lower()
    local, sep, domain = email.partition('@')
    if not sep or not domain:
        return None  # Invalid email format

    if domain in _GMAIL_DOMAINS:
        local = local.split('+')[0]   # Remove plus-suffix
        local = local.replace('.', '')  # Remove dots

    return _sha256_hex(f"{local}@{domain}")


def normalize_and_hash_name(name: Optional[str]) -> Optional[str]:
    """Normalize and SHA-256 hash a name (first or last).

    Normalization: strip whitespace, lowercase.
    Returns None if name is None or empty.
    """
    if not name or not str(name).strip():
        return None
    return _sha256_hex(str(name).strip().lower())
