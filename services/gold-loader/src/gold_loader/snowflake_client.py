"""
Snowflake client for the gold-loader service.

Loads ForwardCurveSnapshot records into GOLD_CURVES.FORWARD_CURVE_SNAPSHOTS via
a MERGE statement (upsert on ``curve_name, as_of``) so that re-assembling the
same window is idempotent.

Target DDL (for reference — apply once to your Snowflake account):

    CREATE TABLE IF NOT EXISTS GOLD_CURVES.FORWARD_CURVE_SNAPSHOTS (
        snapshot_id     VARCHAR(36)   NOT NULL,
        curve_name      VARCHAR(100),
        instrument      VARCHAR(50),
        as_of           TIMESTAMP_TZ,
        tenors          VARIANT,      -- JSON: {"2024-03": {"price": 28.45, ...}}
        completeness    NUMBER(5, 4),
        is_authoritative BOOLEAN,
        version         INTEGER,
        provider        VARCHAR(50),
        created_at      TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (snapshot_id),
        UNIQUE (curve_name, as_of)
    );
"""

from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path
from types import TracebackType
from typing import Any

from mdrp_common.logging import get_logger
from mdrp_common.metrics import (
    SNOWFLAKE_LOAD_DURATION_SECONDS,
    SNOWFLAKE_LOADS_TOTAL,
    SNOWFLAKE_ROWS_LOADED_TOTAL,
)
from mdrp_common.models import ForwardCurveSnapshot

logger = get_logger(__name__)

_LAYER = "gold"


class SnowflakeLoadError(Exception):
    """Raised when a Snowflake MERGE operation fails unrecoverably."""


class SnowflakeClient:
    """
    Manages a single Snowflake connection for the gold-loader.

    Snapshots are upserted via a two-step PUT → MERGE pattern so that
    re-publishing the same ``(curve_name, as_of)`` overwrites the previous
    row rather than creating a duplicate.

    Context manager usage::

        with SnowflakeClient(...) as client:
            rows = client.load_batch(snapshots)
    """

    def __init__(
        self,
        account: str,
        user: str,
        database: str,
        schema: str,
        warehouse: str,
        stage_name: str,
        password: str | None = None,
        pat_token: str | None = None,
        max_reconnect_attempts: int = 3,
        reconnect_delay_seconds: float = 5.0,
    ) -> None:
        self._account = account
        self._user = user
        self._password = password
        self._pat_token = pat_token
        self._database = database
        self._schema = schema
        self._warehouse = warehouse
        self._stage_name = stage_name
        self._max_reconnect = max_reconnect_attempts
        self._reconnect_delay = reconnect_delay_seconds
        self._conn: Any = None  # snowflake.connector.SnowflakeConnection | None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open (or re-open) the Snowflake connection."""
        import snowflake.connector  # type: ignore[import-untyped]

        logger.info(
            "snowflake_connecting",
            account=self._account,
            database=self._database,
            schema=self._schema,
            warehouse=self._warehouse,
        )
        if self._pat_token:
            self._conn = snowflake.connector.connect(
                account=self._account,
                user=self._user,
                authenticator="programmatic_access_token",
                token=self._pat_token,
                database=self._database,
                schema=self._schema,
                warehouse=self._warehouse,
                timezone="UTC",
            )
        else:
            self._conn = snowflake.connector.connect(
                account=self._account,
                user=self._user,
                password=self._password,
                database=self._database,
                schema=self._schema,
                warehouse=self._warehouse,
                timezone="UTC",
            )
        logger.info("snowflake_connected", account=self._account)

    def close(self) -> None:
        """Close the Snowflake connection if open."""
        if self._conn is not None:
            try:
                self._conn.close()
                logger.info("snowflake_connection_closed")
            except Exception as exc:
                logger.warning("snowflake_close_error", error=str(exc))
            finally:
                self._conn = None

    def _ensure_connected(self) -> None:
        """Connect (or reconnect) with linear back-off."""
        if self._conn is not None:
            return
        last_exc: Exception | None = None
        for attempt in range(1, self._max_reconnect + 1):
            try:
                self.connect()
                return
            except Exception as exc:
                last_exc = exc
                wait = self._reconnect_delay * attempt
                logger.warning(
                    "snowflake_connect_retry",
                    attempt=attempt,
                    max_attempts=self._max_reconnect,
                    wait_seconds=wait,
                    error=str(exc),
                )
                time.sleep(wait)
        raise SnowflakeLoadError(
            f"Failed to connect to Snowflake after {self._max_reconnect} attempts"
        ) from last_exc

    # ------------------------------------------------------------------
    # Batch loading
    # ------------------------------------------------------------------

    def load_batch(self, snapshots: list[ForwardCurveSnapshot]) -> int:
        """
        Upsert *snapshots* into GOLD_CURVES.FORWARD_CURVE_SNAPSHOTS.

        Strategy:
        - Serialise snapshots to JSON-lines in a temp file.
        - PUT to the Snowflake internal stage.
        - Use a MERGE INTO on ``(curve_name, as_of)`` to insert or update.
        - Return the number of rows inserted + updated.

        On connection failure the method reconnects once before re-raising.
        """
        if not snapshots:
            return 0

        self._ensure_connected()
        start = time.monotonic()

        try:
            rows_affected = self._do_merge(snapshots)
        except Exception as exc:
            logger.warning(
                "snowflake_load_failed_reconnecting",
                error=str(exc),
                snapshot_count=len(snapshots),
            )
            self._conn = None
            try:
                self._ensure_connected()
                rows_affected = self._do_merge(snapshots)
            except Exception as retry_exc:
                SNOWFLAKE_LOADS_TOTAL.labels(layer=_LAYER, outcome="failed").inc()
                raise SnowflakeLoadError(
                    f"Snowflake MERGE failed after reconnect: {retry_exc}"
                ) from retry_exc

        elapsed = time.monotonic() - start
        SNOWFLAKE_LOADS_TOTAL.labels(layer=_LAYER, outcome="success").inc()
        SNOWFLAKE_LOAD_DURATION_SECONDS.labels(layer=_LAYER).observe(elapsed)
        SNOWFLAKE_ROWS_LOADED_TOTAL.labels(layer=_LAYER).inc(rows_affected)

        logger.info(
            "snowflake_batch_loaded",
            rows_affected=rows_affected,
            batch_size=len(snapshots),
            elapsed_seconds=round(elapsed, 3),
            layer=_LAYER,
        )
        return rows_affected

    def _do_merge(self, snapshots: list[ForwardCurveSnapshot]) -> int:
        """Write snapshots to a temp file and execute PUT + MERGE INTO."""
        assert self._conn is not None

        records = [_snapshot_to_dict(s) for s in snapshots]

        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".jsonl",
            prefix="gold_batch_",
            delete=False,
        ) as tmp:
            tmp_path = Path(tmp.name)
            for record in records:
                tmp.write(json.dumps(record, default=str) + "\n")

        try:
            cursor = self._conn.cursor()
            try:
                # Upload to stage
                put_sql = (
                    f"PUT file://{tmp_path.as_posix()} "
                    f"@{self._schema}.{self._stage_name} "
                    f"AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
                )
                cursor.execute(put_sql)
                logger.debug(
                    "snowflake_put_complete",
                    stage=self._stage_name,
                    file=tmp_path.name,
                )

                staged_file = tmp_path.name

                # MERGE INTO (upsert on curve_name + as_of)
                merge_sql = f"""
                    MERGE INTO {self._schema}.FORWARD_CURVE_SNAPSHOTS AS tgt
                    USING (
                        SELECT
                            $1:snapshot_id::STRING    AS snapshot_id,
                            $1:curve_name::STRING     AS curve_name,
                            $1:instrument::STRING     AS instrument,
                            $1:as_of::TIMESTAMP_TZ    AS as_of,
                            PARSE_JSON($1:tenors)     AS tenors,
                            $1:completeness::FLOAT    AS completeness,
                            $1:is_authoritative::BOOLEAN AS is_authoritative,
                            $1:version::INTEGER       AS version,
                            $1:provider::STRING       AS provider
                        FROM @{self._schema}.{self._stage_name}/{staged_file}
                        (FILE_FORMAT => '{self._schema}.MDRP_JSON')
                    ) AS src
                    ON tgt.curve_name = src.curve_name
                       AND tgt.as_of = src.as_of
                    WHEN MATCHED THEN UPDATE SET
                        tgt.snapshot_id      = src.snapshot_id,
                        tgt.instrument       = src.instrument,
                        tgt.tenors           = src.tenors,
                        tgt.completeness     = src.completeness,
                        tgt.is_authoritative = src.is_authoritative,
                        tgt.version          = src.version,
                        tgt.provider         = src.provider,
                        tgt.created_at       = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT (
                        snapshot_id, curve_name, instrument, as_of,
                        tenors, completeness, is_authoritative, version, provider
                    ) VALUES (
                        src.snapshot_id, src.curve_name, src.instrument, src.as_of,
                        src.tenors, src.completeness, src.is_authoritative,
                        src.version, src.provider
                    )
                """
                cursor.execute(merge_sql)

                # Snowflake MERGE result: (number of rows inserted, number updated)
                row = cursor.fetchone()
                rows_inserted = int(row[0]) if row and row[0] is not None else 0
                rows_updated = int(row[1]) if row and len(row) > 1 and row[1] is not None else 0

                # Clean up staged file
                cursor.execute(
                    f"REMOVE @{self._schema}.{self._stage_name}/{staged_file}"
                )
                return rows_inserted + rows_updated
            finally:
                cursor.close()
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError as exc:
                logger.warning("temp_file_cleanup_failed", path=str(tmp_path), error=str(exc))

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> SnowflakeClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()


# ------------------------------------------------------------------
# Serialisation helper
# ------------------------------------------------------------------


def _snapshot_to_dict(snapshot: ForwardCurveSnapshot) -> dict[str, Any]:
    """Convert a ForwardCurveSnapshot to a plain dict for JSON serialisation."""
    tenors_json: dict[str, Any] = {}
    for tenor_key, tp in snapshot.tenors.items():
        tenors_json[tenor_key] = {
            "price": str(tp.price),  # Decimal → string
            "quality_score": tp.quality_score,
            "last_updated": tp.last_updated.isoformat(),
        }

    return {
        "snapshot_id": snapshot.snapshot_id,
        "curve_name": snapshot.curve_name,
        "instrument": snapshot.instrument,
        "as_of": snapshot.as_of.isoformat(),
        "tenors": json.dumps(tenors_json),  # nested JSON string for PARSE_JSON
        "completeness": snapshot.completeness,
        "is_authoritative": snapshot.is_authoritative,
        "version": snapshot.version,
        "provider": snapshot.provider,
    }
