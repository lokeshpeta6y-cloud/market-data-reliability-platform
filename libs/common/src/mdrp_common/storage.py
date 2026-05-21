"""
S3/MinIO storage client for the Bronze layer.

Uses boto3 with path-style addressing when pointing at MinIO locally.
The same code runs against real AWS S3 in production — only the endpoint
URL and credentials differ, controlled via environment variables.

Bronze partition scheme:
  s3://{bucket}/bronze/{provider}/{YYYY-MM-DD}/{HH}/events_{batch_id}.parquet
"""

from __future__ import annotations

import io
import json
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config

logger = logging.getLogger(__name__)


class BronzeStorageClient:
    def __init__(
        self,
        bucket: str,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
    ) -> None:
        self._bucket = bucket
        self._endpoint_url = endpoint_url

        session_kwargs: dict[str, Any] = {}
        if aws_access_key_id:
            session_kwargs["aws_access_key_id"] = aws_access_key_id
        if aws_secret_access_key:
            session_kwargs["aws_secret_access_key"] = aws_secret_access_key

        # path_style_access is required for MinIO compatibility
        self._s3 = boto3.client(
            "s3",
            region_name=region,
            endpoint_url=endpoint_url,
            config=Config(s3={"addressing_style": "path"}) if endpoint_url else None,
            **session_kwargs,
        )

    def write_parquet_batch(
        self,
        records: list[dict[str, Any]],
        provider: str,
        timestamp: datetime | None = None,
    ) -> str:
        """
        Serialise a list of event dicts to Parquet and write to S3.

        Returns the full S3 key of the written object.
        """
        if not records:
            raise ValueError("cannot write empty batch")

        ts = timestamp or datetime.now(timezone.utc)
        batch_id = str(uuid4())
        key = _partition_key(provider, ts, batch_id)

        df = pd.DataFrame(_sanitise_records(records))
        try:
            table = pa.Table.from_pandas(df, preserve_index=False)
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
            # Fallback: stringify all columns so mixed-type batches (e.g. fault-
            # injected ~~CORRUPTED~~ strings alongside numeric prices) never block
            # Bronze writes.  Raw JSON is preserved in the payload column.
            df = df.astype(str)
            table = pa.Table.from_pandas(df, preserve_index=False)

        buf = io.BytesIO()
        pq.write_table(
            table,
            buf,
            compression="snappy",
            # Store schema metadata so consumers can introspect without a registry
            write_batch_size=1000,
        )
        buf.seek(0)
        size_bytes = buf.getbuffer().nbytes

        self._s3.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
            Metadata={
                "provider": provider,
                "partition_date": ts.strftime("%Y-%m-%d"),
                "partition_hour": str(ts.hour),
                "record_count": str(len(records)),
                "batch_id": batch_id,
            },
        )

        logger.info(
            "bronze_batch_written",
            extra={
                "key": key,
                "record_count": len(records),
                "size_bytes": size_bytes,
                "provider": provider,
            },
        )
        return key

    def list_partitions(
        self,
        provider: str,
        start: datetime,
        end: datetime,
    ) -> list[str]:
        """
        List all Parquet object keys for a provider within a time range.
        Used by the replay engine to enumerate files for Bronze replay.
        """
        keys: list[str] = []
        prefix = f"bronze/{provider}/"
        paginator = self._s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                file_ts = _extract_timestamp_from_key(key)
                if file_ts and start <= file_ts <= end:
                    keys.append(key)

        return sorted(keys)

    def read_parquet_batch(self, key: str) -> list[dict[str, Any]]:
        """Read a Parquet file from S3 and return as a list of dicts."""
        response = self._s3.get_object(Bucket=self._bucket, Key=key)
        buf = io.BytesIO(response["Body"].read())
        table = pq.read_table(buf)
        return table.to_pydict_list() if hasattr(table, "to_pydict_list") else table.to_pandas().to_dict(orient="records")

    def ensure_bucket(self) -> None:
        """Create the bucket if it does not exist. Used for MinIO local setup."""
        try:
            self._s3.head_bucket(Bucket=self._bucket)
        except self._s3.exceptions.NoSuchBucket:
            self._s3.create_bucket(Bucket=self._bucket)
            logger.info("bucket_created", extra={"bucket": self._bucket})
        except Exception:
            # head_bucket raises ClientError with 404 for MinIO
            try:
                self._s3.create_bucket(Bucket=self._bucket)
                logger.info("bucket_created", extra={"bucket": self._bucket})
            except Exception:
                pass


def _partition_key(provider: str, ts: datetime, batch_id: str) -> str:
    return (
        f"bronze/{provider}/"
        f"{ts.strftime('%Y-%m-%d')}/"
        f"{ts.strftime('%H')}/"
        f"events_{batch_id}.parquet"
    )


def _sanitise_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Serialise dict/list values to JSON strings so PyArrow can build a
    consistent column schema even when fault injection has injected
    strings (e.g. '~~CORRUPTED~~') into otherwise-numeric nested fields.
    """
    out = []
    for record in records:
        sanitised: dict[str, Any] = {}
        for k, v in record.items():
            if isinstance(v, (dict, list)):
                sanitised[k] = json.dumps(v, default=str)
            else:
                sanitised[k] = v
        out.append(sanitised)
    return out


def _extract_timestamp_from_key(key: str) -> datetime | None:
    """
    Parse the date/hour from a Bronze partition key.
    Expected format: bronze/{provider}/YYYY-MM-DD/HH/events_{id}.parquet
    """
    try:
        parts = key.split("/")
        date_str = parts[2]
        hour_str = parts[3]
        return datetime(
            *[int(x) for x in date_str.split("-")],
            int(hour_str),
            tzinfo=timezone.utc,
        )
    except (IndexError, ValueError):
        return None
