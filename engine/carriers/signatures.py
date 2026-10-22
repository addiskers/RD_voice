"""Carrier callback authentication.

Plivo V3 (port of ``plivo-python`` ``signature_v3.py``, MIT): the signed string is the callback URL as Plivo
called it (so build it from PUBLIC_HTTP_URL, never from the proxied request host), with sorted query
parameters, and for POST the sorted ``namevalue`` pairs appended after a ``.``; then ``.<nonce>`` is appended
and the whole thing is HMAC-SHA256'd with the auth token and base64-encoded. V2 (deprecated) signs the URL
without its query string followed directly by the nonce.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import ipaddress
from typing import Any, Mapping
from urllib.parse import parse_qs, urlparse, urlunparse


def _fmt(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("latin-1")
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, list):
        return [_fmt(v) for v in value]
    return value


def _query_map(query: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in parse_qs(query, keep_blank_values=True).items():
        out[_fmt(k)] = _fmt(v)
    return out


def _sorted_query_string(params: Mapping[str, Any]) -> str:
    parts = []
    for key in sorted(params):
        value = params[key]
        if isinstance(value, list):
            parts.append("&".join(f"{_fmt(key)}={v}" for v in sorted(_fmt(value))))
        else:
            parts.append(f"{_fmt(key)}={_fmt(value)}")
    return "&".join(parts)


def _sorted_params_string(params: Mapping[str, Any]) -> str:
    parts = []
    for key in sorted(params):
        value = params[key]
        if isinstance(value, list):
            parts.append("".join(f"{_fmt(key)}{v}" for v in sorted(_fmt(value))))
        elif isinstance(value, dict):
            parts.append(f"{_fmt(key)}{_sorted_params_string(value)}")
        else:
            parts.append(f"{_fmt(key)}{_fmt(value)}")
    return "".join(parts)


def _get_url(uri: str, params: Mapping[str, Any], empty_post_params: bool = True) -> str:
    parsed = urlparse(uri)
    base = urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))
    merged = dict(params)
    merged.update(_query_map(parsed.query))
    query = _sorted_query_string(merged)
    if query or not empty_post_params:
        base += "?" + query
    if query and not empty_post_params:
        base += "."
    return base


def plivo_v3_base_string(method: str, url: str, params: Mapping[str, Any]) -> str:
    """The string Plivo signs for a callback: GET merges ``params`` into the sorted query; POST appends the
    sorted form parameters after the query."""
    if method.upper() == "GET":
        return _get_url(url, params)
    return _get_url(url, {}, empty_post_params=len(params) == 0) + _sorted_params_string(params)


def plivo_v3_signature(auth_token: str, base_string: str, nonce: str) -> str:
    mac = hmac.new(auth_token.encode("utf-8"), f"{base_string}.{nonce}".encode("utf-8"), hashlib.sha256)
    return base64.b64encode(mac.digest()).decode("ascii").strip()


def verify_plivo_v3(auth_token: str, method: str, url: str, nonce: str, signature_header: str,
                    params: Mapping[str, Any]) -> bool:
    if not auth_token or not nonce or not signature_header:
        return False
    expected = plivo_v3_signature(auth_token, plivo_v3_base_string(method, url, params), nonce)
    return any(hmac.compare_digest(expected, candidate.strip()) for candidate in signature_header.split(","))


def verify_plivo_v2(auth_token: str, url: str, nonce: str, signature_header: str) -> bool:
    if not auth_token or not nonce or not signature_header:
        return False
    parsed = urlparse(url)
    base = urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))
    mac = hmac.new(auth_token.encode("utf-8"), (base + nonce).encode("utf-8"), hashlib.sha256)
    expected = base64.b64encode(mac.digest()).decode("ascii").strip()
    return hmac.compare_digest(expected, signature_header.strip())


def ip_in_allowlist(ip: str, allowlist: str) -> bool:
    """``allowlist``: comma-separated IPs or CIDRs. Empty allowlist allows nothing."""
    try:
        addr = ipaddress.ip_address(ip.strip())
    except ValueError:
        return False
    for entry in (allowlist or "").split(","):
        entry = entry.strip()
        if not entry:
            continue
        try:
            if "/" in entry:
                if addr in ipaddress.ip_network(entry, strict=False):
                    return True
            elif addr == ipaddress.ip_address(entry):
                return True
        except ValueError:
            continue
    return False
