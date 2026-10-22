"""One-off maintenance commands behind ``gvox-api``: encrypt pre-existing secrets, copy a Phase-0 SQLite file."""

from __future__ import annotations

from sqlalchemy import select

from engine.api.crypto import SecretBox
from engine.api.db import AgentVersion, Call, Database, PhoneNumber


async def encrypt_existing_rows(database_url: str, box: SecretBox) -> int:
    """Encrypt provider keys in agent versions and frozen call configs, and webhook secrets on calls and
    numbers, wherever they are still plaintext; re-encrypt already-encrypted values with the primary key
    (rotation). Idempotent."""
    from engine.api.crypto import _map_keys

    def _settle(value):
        return box.rotate(value) if box.is_encrypted(value) else box.encrypt(value)

    db = Database(database_url)
    await db.init()
    changed = 0
    try:
        async with db.session() as s:
            for row in await s.scalars(select(AgentVersion)):
                enc = _map_keys(row.config or {}, _settle)
                if enc != row.config:
                    row.config = enc
                    changed += 1
            for call in await s.scalars(select(Call)):
                enc = _map_keys(call.agent_config or {}, _settle)
                if enc != call.agent_config:
                    call.agent_config = enc
                    changed += 1
                if call.webhook_secret and _settle(call.webhook_secret) != call.webhook_secret:
                    call.webhook_secret = _settle(call.webhook_secret)
                    changed += 1
            for num in await s.scalars(select(PhoneNumber)):
                if num.webhook_secret and _settle(num.webhook_secret) != num.webhook_secret:
                    num.webhook_secret = _settle(num.webhook_secret)
                    changed += 1
            await s.commit()
    finally:
        await db.close()
    return changed


async def copy_sqlite(source_url: str, target_url: str) -> int:
    """Copy agent versions, numbers and calls from one database to another (rows that already exist in the
    target are skipped). Recordings on local disk are not moved."""
    src, dst = Database(source_url), Database(target_url)
    await src.init()
    await dst.init()
    copied = 0
    try:
        async with src.session() as s, dst.session() as d:
            for model, key in ((AgentVersion, lambda r: (r.agent_id, r.version)), (PhoneNumber, lambda r: r.e164),
                               (Call, lambda r: r.id)):
                for row in await s.scalars(select(model)):
                    if await d.get(model, key(row)) is not None:
                        continue
                    data = {c.key: getattr(row, c.key) for c in model.__table__.columns}
                    d.add(model(**data))
                    copied += 1
            await d.commit()
    finally:
        await src.close()
        await dst.close()
    return copied
