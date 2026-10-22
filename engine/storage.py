"""Object storage for call recordings: a local directory (development, single box) or an S3-compatible
bucket (GCS interoperability, MinIO, AWS). The worker uploads after the call; the API streams on request
and deletes after the retention window. Keys look like ``recordings/YYYY/MM/DD/<call_id>.wav``."""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
from datetime import datetime
from typing import Optional, Protocol

from engine.config import Settings

logger = logging.getLogger(__name__)

KEY_PREFIX = "recordings/"


def recording_key(call_id: str, when: datetime) -> str:
    return f"{KEY_PREFIX}{when:%Y/%m/%d}/{call_id}.wav"


def is_storage_key(path: Optional[str]) -> bool:
    return bool(path) and path.startswith(KEY_PREFIX)


class Storage(Protocol):
    async def put_file(self, local_path: str, key: str) -> None: ...
    async def get_bytes(self, key: str) -> bytes: ...
    async def exists(self, key: str) -> bool: ...
    async def delete(self, key: str) -> None: ...
    def signed_url(self, key: str, expires_s: int = 900) -> Optional[str]: ...


class LocalStorage:
    def __init__(self, root: str):
        self.root = root

    def _path(self, key: str) -> str:
        return os.path.join(self.root, *key.split("/"))

    async def put_file(self, local_path: str, key: str) -> None:
        dst = self._path(key)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        await asyncio.to_thread(shutil.copyfile, local_path, dst)

    async def get_bytes(self, key: str) -> bytes:
        def _read() -> bytes:
            with open(self._path(key), "rb") as f:
                return f.read()
        return await asyncio.to_thread(_read)

    async def exists(self, key: str) -> bool:
        return os.path.isfile(self._path(key))

    async def delete(self, key: str) -> None:
        try:
            os.remove(self._path(key))
        except FileNotFoundError:
            pass

    def signed_url(self, key: str, expires_s: int = 900) -> Optional[str]:
        return None


class S3Storage:
    """boto3 in a worker thread; works with any S3-compatible endpoint (GCS interop, MinIO, AWS)."""

    def __init__(self, bucket: str, *, endpoint: str = "", region: str = "", access_key: str = "", secret_key: str = ""):
        import boto3   # optional dependency: pip install "gvox-voice[s3]"
        self.bucket = bucket
        kwargs = {"region_name": region or None, "endpoint_url": endpoint or None}
        if access_key and secret_key:
            kwargs.update(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        self._client = boto3.client("s3", **{k: v for k, v in kwargs.items() if v is not None})

    async def put_file(self, local_path: str, key: str) -> None:
        await asyncio.to_thread(self._client.upload_file, local_path, self.bucket, key,
                                ExtraArgs={"ContentType": "audio/wav"})

    async def get_bytes(self, key: str) -> bytes:
        def _get() -> bytes:
            return self._client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        return await asyncio.to_thread(_get)

    async def exists(self, key: str) -> bool:
        def _head() -> bool:
            try:
                self._client.head_object(Bucket=self.bucket, Key=key)
                return True
            except Exception:
                return False
        return await asyncio.to_thread(_head)

    async def delete(self, key: str) -> None:
        await asyncio.to_thread(self._client.delete_object, Bucket=self.bucket, Key=key)

    def signed_url(self, key: str, expires_s: int = 900) -> Optional[str]:
        try:
            return self._client.generate_presigned_url("get_object", Params={"Bucket": self.bucket, "Key": key},
                                                       ExpiresIn=expires_s)
        except Exception as e:
            logger.warning("presigned url failed for %s: %s", key, e)
            return None


def build_storage(settings: Settings) -> Storage:
    kind = (settings.recording_storage or "local").lower()
    if kind == "s3":
        if not settings.recording_s3_bucket:
            raise ValueError("RECORDING_STORAGE=s3 needs RECORDING_S3_BUCKET")
        return S3Storage(settings.recording_s3_bucket, endpoint=settings.recording_s3_endpoint,
                         region=settings.recording_s3_region, access_key=settings.recording_s3_access_key,
                         secret_key=settings.recording_s3_secret_key)
    return LocalStorage(settings.recordings_dir)
