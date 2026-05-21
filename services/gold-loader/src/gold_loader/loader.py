"""
GoldLoader — coordinates snapshot assembly and Snowflake Gold loading.

Responsibilities
----------------
1. Accept incoming CurveEvents and pass them to the SnapshotAssembler.
2. Periodically call ``assembler.get_ready_snapshots()`` to retrieve expired windows.
3. Filter to only authoritative snapshots (unless ``write_non_authoritative`` is True).
4. Load the filtered snapshots to Snowflake Gold via SnowflakeClient.
5. Return load counts to the caller for metrics and offset commit decisions.

Thread safety
-------------
GoldLoader is NOT internally thread-safe for ``add`` vs ``flush`` calls; the
design expects ``add()`` to be called from the main consume thread and
``flush_ready()`` to be called from either the same thread or a dedicated
polling thread with appropriate external synchronisation (see main.py).
"""

from __future__ import annotations

from mdrp_common.logging import get_logger
from mdrp_common.models import CurveEvent, ForwardCurveSnapshot

from .snapshot_assembler import SnapshotAssembler
from .snowflake_client import SnowflakeClient

logger = get_logger(__name__)


class GoldLoader:
    """
    Orchestrates snapshot assembly and Snowflake Gold loading.

    Parameters
    ----------
    assembler:
        A configured SnapshotAssembler instance.
    snowflake_client:
        A configured SnowflakeClient, or ``None`` if Snowflake is not available.
    write_non_authoritative:
        If True, write all snapshots that meet ``min_completeness`` regardless
        of ``is_authoritative``.  Defaults to False (Gold only gets authoritative
        snapshots).
    """

    def __init__(
        self,
        assembler: SnapshotAssembler,
        snowflake_client: SnowflakeClient | None,
        write_non_authoritative: bool = False,
    ) -> None:
        self._assembler = assembler
        self._client = snowflake_client
        self._write_non_authoritative = write_non_authoritative
        self._total_loaded: int = 0
        self._total_snapshots_assembled: int = 0
        self._total_snapshots_skipped: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add(self, event: CurveEvent) -> None:
        """Buffer *event* into the snapshot assembler."""
        self._assembler.add(event)

    def flush_ready(self) -> int:
        """
        Retrieve all expired windows from the assembler, filter for
        authoritative snapshots, and load them to Snowflake.

        Returns the number of rows loaded (0 if nothing was ready or Snowflake
        is not configured).

        Raises SnowflakeLoadError if the Snowflake write fails.
        """
        ready = self._assembler.get_ready_snapshots()
        if not ready:
            return 0

        self._total_snapshots_assembled += len(ready)

        # Filter: only write authoritative snapshots unless configured otherwise
        to_load: list[ForwardCurveSnapshot] = []
        for snapshot in ready:
            if self._write_non_authoritative or snapshot.is_authoritative:
                to_load.append(snapshot)
            else:
                self._total_snapshots_skipped += 1
                logger.info(
                    "snapshot_not_authoritative_skipped",
                    curve_name=snapshot.curve_name,
                    as_of=snapshot.as_of.isoformat(),
                    completeness=round(snapshot.completeness, 4),
                    is_authoritative=snapshot.is_authoritative,
                )

        if not to_load:
            return 0

        if self._client is None:
            logger.warning(
                "snowflake_not_configured_skipping_flush",
                snapshot_count=len(to_load),
                layer="gold",
            )
            return 0

        rows_loaded = self._client.load_batch(to_load)
        self._total_loaded += rows_loaded

        logger.info(
            "gold_flush_complete",
            snapshots_ready=len(ready),
            snapshots_loaded=len(to_load),
            rows_loaded=rows_loaded,
            total_loaded=self._total_loaded,
            total_skipped=self._total_snapshots_skipped,
        )
        return rows_loaded

    def close(self) -> None:
        """Close the Snowflake connection (if open)."""
        if self._client is not None:
            self._client.close()

    @property
    def pending_windows(self) -> int:
        """Number of open (not-yet-expired) windows in the assembler."""
        return self._assembler.pending_window_count()

    @property
    def total_loaded(self) -> int:
        return self._total_loaded

    @property
    def total_snapshots_assembled(self) -> int:
        return self._total_snapshots_assembled
