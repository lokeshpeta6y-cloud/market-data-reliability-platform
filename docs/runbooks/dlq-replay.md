# Runbook: DLQ Replay

**Audience:** On-call engineer / data operations  
**Severity:** P2 (data quality gap, not a live outage)  
**Last updated:** 2026-05-20

---

## 1. When to Replay the DLQ

Trigger a DLQ replay when **all three** of the following are true:

1. **Events are accumulating on `market.events.dlq`** — the Grafana alert `DLQDepthHigh` has fired, or `mdrp_dlq_events_total` has risen sharply in a short window.
2. **The root cause has been resolved** — replaying before fixing the underlying issue will simply re-DLQ the same events. Common root causes and fixes:

   | Failure category | Typical cause | Fix before replaying |
   |---|---|---|
   | `SCHEMA_VIOLATION` | Provider changed field names | Update normaliser mapping or schema registry |
   | `MALFORMED` | Provider bug sending null prices | Confirm fix deployed; set fault rate back to normal |
   | `STALE` | Clock skew on provider host | Confirm NTP sync on emulator or provider side |
   | `OUT_OF_ORDER` | Burst of future-dated events | Confirm emulator clock fixed |
   | `MISSING_REQUIRED_FIELD` | Provider omitting required fields | Confirm provider fix or update validation rules |

3. **The data gap matters for downstream consumers** — DLQ events that were already covered by a Bronze S3 replay do not need a DLQ replay (they would produce duplicates, which Snowflake COPY INTO deduplicates but wastes quota).

---

## 2. Inspect DLQ Contents

### 2a. Check overall DLQ statistics

```bash
curl -s http://localhost:8000/api/v1/dlq | python -m json.tool
```

Example response:

```json
{
  "depth_estimate": 1452,
  "top_failure_categories": [
    {"category": "schema_violation", "count": 1100},
    {"category": "malformed", "count": 320},
    {"category": "stale", "count": 32}
  ],
  "recent_entries": [...],
  "as_of": "2026-05-20T09:00:00Z"
}
```

### 2b. Sample recent DLQ entries

```bash
curl -s "http://localhost:8000/api/v1/dlq?limit=10" | python -m json.tool
```

Key fields to review in each DLQ event:

| Field | Purpose |
|---|---|
| `failure_reason` | Human-readable explanation of why the event failed |
| `failure_category` | Structured category used for replay routing |
| `raw_payload` | The original event payload, verbatim — use this to confirm the root cause |
| `original_received_at` | When the event arrived; helps bound the replay window |
| `retry_count` | How many times this event has been replayed |

---

## 3. Trigger DLQ Replay

### Using make (replays last hour by default)

```bash
make dlq-replay
```

### Using curl (custom time window)

```bash
curl -s -X POST http://localhost:8000/api/v1/dlq/replay \
  -H "Content-Type: application/json" \
  -d '{
    "start_time": "2026-05-20T07:00:00Z",
    "end_time": "2026-05-20T09:00:00Z",
    "requested_by": "on-call"
  }' | python -m json.tool
```

### Filter by provider or instrument

```bash
curl -s -X POST http://localhost:8000/api/v1/dlq/replay \
  -H "Content-Type: application/json" \
  -d '{
    "start_time": "2026-05-20T07:00:00Z",
    "end_time": "2026-05-20T09:00:00Z",
    "provider": "ice-endex",
    "instrument": "TTF",
    "requested_by": "on-call"
  }' | python -m json.tool
```

A successful response looks like:

```json
{
  "job_id": "a1b2c3d4-...",
  "status": "pending",
  "source": "dlq",
  "events_to_replay": 1100,
  "requested_at": "2026-05-20T09:05:00Z"
}
```

Save the `job_id` for monitoring.

---

## 4. Monitor Replay Progress

### Poll the job status

```bash
JOB_ID="a1b2c3d4-..."
curl -s "http://localhost:8000/api/v1/replay/${JOB_ID}" | python -m json.tool
```

The response `status` field transitions through:

```
pending → running → completed
                 ↘ failed
```

The `events_replayed` counter increments in real time. A `failed` status includes an `error` field explaining what went wrong.

### Watch replay throughput in Grafana

Open the **MDRP Replay** Grafana dashboard (`:3000`) and look for:

| Panel | Expected behaviour during replay |
|---|---|
| `Replay Events/s` | Non-zero, typically matching the original ingest rate |
| `market.events.raw` producer rate | Spike corresponding to replayed events |
| `market.events.validated` consumer throughput | Should rise as replayed events pass validation |
| DLQ depth | Should decrease as successfully replayed events are cleared |

### Check consumer lag

```bash
docker compose exec redpanda rpk group describe validation-service \
  --brokers=redpanda:9092
```

During an active replay the lag on `market.events.raw` will temporarily increase as the replay engine emits events faster than the validation service consumes them. This is normal and will drain once the replay completes.

---

## 5. Verify No Duplicate Loads in Snowflake

DLQ events are re-emitted with `is_replay=true` and the original `event_id` preserved. The Snowflake Silver and Gold loaders use `COPY INTO` with `PURGE=false` and enforce uniqueness via a merge key (`event_id` in Silver, `(curve_name, tenor, as_of)` in Gold). Duplicates are therefore rejected at the database level.

### Confirm via Snowflake query

Run the following in Snowflake Worksheets (replace `$START` and `$END` with the replay window):

```sql
-- Check for duplicate event_ids in Silver (should return 0 rows)
SELECT event_id, COUNT(*) AS cnt
FROM MARKET_DATA.SILVER_EVENTS.VALIDATED_EVENTS
WHERE validated_at BETWEEN '$START' AND '$END'
  AND is_replay = TRUE
GROUP BY event_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;
```

```sql
-- Check for duplicate curve snapshots in Gold (should return 0 rows)
SELECT curve_name, tenor, as_of, COUNT(*) AS cnt
FROM MARKET_DATA.GOLD_CURVES.FORWARD_CURVE_SNAPSHOTS
WHERE created_at BETWEEN '$START' AND '$END'
GROUP BY curve_name, tenor, as_of
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;
```

If either query returns rows, escalate to the data engineering team — this indicates a deduplication logic bug rather than an operator error.

### Confirm replay count matches expectation

```bash
curl -s "http://localhost:8000/api/v1/replay/${JOB_ID}" \
  | python -c "import sys,json; d=json.load(sys.stdin); print(f'Replayed: {d[\"events_replayed\"]} / Expected: {d.get(\"events_to_replay\",\"unknown\")}')"
```

The `events_replayed` count should equal `events_to_replay`. A lower count may indicate some DLQ events were filtered out (e.g. they were already replayed previously and skipped due to `retry_count` limits) or that the replay window ended before all events were processed.
