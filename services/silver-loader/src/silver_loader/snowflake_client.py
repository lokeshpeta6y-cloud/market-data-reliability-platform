"""
Snowflake client for the silver-loader service.

Handles connection lifecycle, reconnection on failure, and bulk loading of
CurveEvent records into SILVER_EVENTS.CURVE_EVENTS via a staged COPY INTO.

Target DDL (for reference — apply once to your Snowflake account):

    CREATE TABLE IF NOT EXISTS SILVER_EVENTS.CURVE_EVENTS (
        event_id VARCHAR(36) NOT NULL,
        source_event_id VARCHAR(36),
        curve_name VARCHAR(100),
        instrument VARCHAR(50),
        tenor VARCHAR(20),
        delivery_period VARCHAR(20),
        price NUMBER(20, 6),
        currency VARCHAR(10),
        unit VARCHAR(20),
        provider VARCHAR(50),
        version INTEGER,
        event_timestamp TIMESTAMP_TZ,
        ingestion_timestamp TIMESTAMP_TZ,
        quality_score NUMBER(5, 4),
        is_replay BOOLEAN DEFAULT FALSE,
        replay_source VARCHAR(50),
        trace_id VARCHAR(36),
        loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (event_id)
    );
"""

from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Any

from mdrp_common.logging import get_logger
from mdrp_common.metrics import (
    SNOWFLAKE_LOAD_DURATION_SECONDS,
    SNOWFLAKE_LOADS_TOTAL,
    SNOWFLAKE_ROWS_LOADED_TOTAL,
)

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)

# Layer label used in Prometheus metrics
_LAYER = "silver"

# Columns in insertion order — must match the DDL exactly (excluding loaded_at which
# has a DEFAULT and is set server-side)
_COLUMNS = (
    "event_id",
    "source_event_id",
    "curve_name",
    "instrument",
    "tenor",
    "delivery_period",
    "price",
    "currency",
    "unit",
    "provider",
    "version",
    "event_timestamp",
    "ingestion_timestamp",
    "quality_score",
    "is_replay",
    "replay_source",
    "trace_id",
)


class SnowflakeLoadError(Exception):
    """Raised when a Snowflake COPY INTO operation fails unrecoverably."""


class SnowflakeClient:
    """
    Manages a single Snowflake connection for the silver-loader.

    The client is lazily connected on the first ``load_batch`` call so that
    the service starts even when Snowflake credentials are absent.  A lost
    connection is transparently re-established up to ``max_reconnect_attempts``
    times before raising ``SnowflakeLoadError``.

    Context manager usage::

        with SnowflakeClient(...) as client:
            rows = client.load_batch(records)
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
        # Import here so the service starts even without snowflake-connector-python
        import snowflake.connector  # type: ignore[import-untyped]

        logger.info(
            "snowflake_connecting",
            account=self._account,
            database=self._database,
            schema=self._schema,
            warehouse=self._warehouse,
        )
        if self._pat_token:
            # Programmatic Access Token — bypasses MFA
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
        """Connect (or reconnect) with exponential back-off."""
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

    def load_batch(self, records: list[dict[str, Any]]) -> int:
        """
        Bulk-load *records* into SILVER_EVENTS.CURVE_EVENTS.

        Strategy:
        - Write all records as newline-delimited JSON to a temporary file.
        - PUT the file onto the Snowflake internal stage.
        - COPY INTO the target table using ON_ERROR=CONTINUE for idempotency.
        - Return the number of rows actually loaded (skipping duplicates).

        The ``event_id`` is the PRIMARY KEY / dedup key — Snowflake will skip
        duplicate rows when ON_ERROR=CONTINUE is used with a NOT NULL PK
        violation.

        On connection failure the method reconnects once before re-raising.
        """
        if not records:
            return 0

        self._ensure_connected()
        start = time.monotonic()

        try:
            rows_loaded = self._do_copy_into(records)
        except Exception as exc:
            # Attempt a single reconnect before giving up
            logger.warning(
                "snowflake_load_failed_reconnecting",
                error=str(exc),
                record_count=len(records),
            )
            self._conn = None
            try:
                self._ensure_connected()
                rows_loaded = self._do_copy_into(records)
            except Exception as retry_exc:
                SNOWFLAKE_LOADS_TOTAL.labels(layer=_LAYER, outcome="failed").inc()
                raise SnowflakeLoadError(
                    f"Snowflake COPY INTO failed after reconnect: {retry_exc}"
                ) from retry_exc

        elapsed = time.monotonic() - start
        SNOWFLAKE_LOADS_TOTAL.labels(layer=_LAYER, outcome="success").inc()
        SNOWFLAKE_LOAD_DURATION_SECONDS.labels(layer=_LAYER).observe(elapsed)
        SNOWFLAKE_ROWS_LOADED_TOTAL.labels(layer=_LAYER).inc(rows_loaded)

        logger.info(
            "snowflake_batch_loaded",
            rows_loaded=rows_loaded,
            batch_size=len(records),
            elapsed_seconds=round(elapsed, 3),
            layer=_LAYER,
        )
        return rows_loaded

    def _do_copy_into(self, records: list[dict[str, Any]]) -> int:
        """Write records to a temp file and execute PUT + COPY INTO."""
        assert self._conn is not None

        # Serialise to JSON-lines in a temporary file
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".jsonl",
            prefix="silver_batch_",
            delete=False,
        ) as tmp:
            tmp_path = Path(tmp.name)
            for record in records:
                tmp.write(json.dumps(record, default=str) + "\n")

        try:
            cursor = self._conn.cursor()
            try:
                # Upload file to the Snowflake internal stage
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

                # Build column list for COPY INTO
                col_list = ", ".join(_COLUMNS)
                # JSON parse expressions for each column (Snowflake VARIANT path)
                parse_exprs = ", ".join(
                    f"$1:{col}::{'BOOLEAN' if col == 'is_replay' else 'STRING'}"
                    if col in ("is_replay",)
                    else f"$1:{col}"
                    for col in _COLUMNS
                )

                staged_file = tmp_path.name
                copy_sql = f"""
                    COPY INTO {self._schema}.CURVE_EVENTS ({col_list})
                    FROM (
                        SELECT {parse_exprs}
                        FROM @{self._schema}.{self._stage_name}/{staged_file}
                        (FILE_FORMAT => '{self._schema}.MDRP_JSON')
                    )
                    ON_ERROR = CONTINUE
                    PURGE = TRUE
                """
                cursor.execute(copy_sql)

                # Sum rows_loaded across all file results
                rows_loaded: int = 0
                for row in cursor.fetchall():
                    # Snowflake COPY INTO result: (file, status, rows_parsed,
                    #   rows_loaded, error_limit, errors_seen, first_error, ...)
                    try:
                        rows_loaded += int(row[3])
                    except (IndexError, TypeError, ValueError):
                        pass

                return rows_loaded
            finally:
                cursor.close()
        finally:
            # Always clean up the temp file
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError as exc:
                logger.warning("temp_file_cleanup_failed", path=str(tmp_path), error=str(exc))

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "SnowflakeClient":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
