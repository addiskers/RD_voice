"""Secrets at rest: bring-your-own provider keys inside agent configs and webhook secrets are stored
Fernet-encrypted (``enc:v1:<token>``) when ``ENGINE_ENCRYPTION_KEY`` is set. Reads are tolerant: a value
without the prefix is returned as-is, so a database written before encryption keeps working and
``gvox-api encrypt-existing`` can migrate it."""

from __future__ import annotations

import copy
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

PREFIX = "enc:v1:"
KEY_FIELDS = ("stt", "llm", "tts", "realtime")       # AgentConfig sections carrying an ``api_key``


class SecretBox:
    def __init__(self, key: str):
        self._fernet = None
        if key:
            from cryptography.fernet import Fernet
            self._fernet = Fernet(key.encode("utf-8") if isinstance(key, str) else key)
        else:
            logger.warning("ENGINE_ENCRYPTION_KEY is not set: provider keys and webhook secrets are stored in "
                           "plaintext (development only)")

    @property
    def enabled(self) -> bool:
        return self._fernet is not None

    @staticmethod
    def is_encrypted(value: Optional[str]) -> bool:
        return isinstance(value, str) and value.startswith(PREFIX)

    def encrypt(self, value: str) -> str:
        if self._fernet is None or not value or self.is_encrypted(value):
            return value
        return PREFIX + self._fernet.encrypt(value.encode("utf-8")).decode("ascii")

    def decrypt(self, value: Optional[str]) -> str:
        if not value or not self.is_encrypted(value):
            return value or ""
        if self._fernet is None:
            raise ValueError("encrypted value found but ENGINE_ENCRYPTION_KEY is not set")
        return self._fernet.decrypt(value[len(PREFIX):].encode("ascii")).decode("utf-8")

    @staticmethod
    def mask(value: Optional[str]) -> str:
        if not value:
            return ""
        return "****" + value[-4:] if len(value) >= 8 else "****"


def _map_keys(cfg: dict[str, Any], fn) -> dict[str, Any]:
    out = copy.deepcopy(cfg)
    for section in KEY_FIELDS:
        block = out.get(section)
        if isinstance(block, dict) and block.get("api_key"):
            block["api_key"] = fn(block["api_key"])
    return out


def encrypt_agent_config(cfg: dict[str, Any], box: SecretBox) -> dict[str, Any]:
    return _map_keys(cfg, box.encrypt)


def decrypt_agent_config(cfg: dict[str, Any], box: SecretBox) -> dict[str, Any]:
    return _map_keys(cfg, box.decrypt)


def mask_agent_config(cfg: dict[str, Any], box: SecretBox) -> dict[str, Any]:
    return _map_keys(cfg, lambda v: SecretBox.mask(box.decrypt(v)) if box.is_encrypted(v) else SecretBox.mask(v))
