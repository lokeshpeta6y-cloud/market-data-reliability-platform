# Runbook: Provider Outage

**Audience:** On-call engineer  
**Severity:** P1 (data gap) / P2 (degraded quality)  
**Last updated:** 2026-05-20

---

## 1. Detection

### Prometheus alerts that fire

| Alert | Meaning |
|---|---|
| `ProviderNoEventsFor5m` | A provider has produced zero events on `market.events.raw` for 5 continuous minutes. The validation-service consumer lag is zero but the raw topic shows no new messages. |
| `ProviderDLQRateHigh` | More than 20% of a provider's events in the last 60 s have been routed to the DLQ. Usually indicates a schema change or upstream API issue rather than a full outage. |
| `ProviderQualityScoreLow` | The rolling P50 quality score for a provider has dropped below 0.6. Often a leading indicator before a full outage. |
| `ValidationConsumerLagGrowing` | The validation-service Kafka consumer group is falling behind. May indicate the provider is flooding with bad events or the service itself has an issue. |

### What the alert means

`ProviderNoEventsFor5m` fires when the Prometheus metric `mdrp_events_raw_total{provider="<name>"}` has not increased for 5 minutes. This metric is emitted by the provider-emulator (or the real Databento adapter) every time a message is produced to `market.events.raw`.

A gap in this counter means either:
- The upstream provider API is down or rate-limiting.
- The provider-emulator service has crashed or lost its Kafka connection.
- Network connectivity between the emulator and Redpanda has been lost.

---

## 2. Immediate Triage

### 2a. Check the ops-api provider health summary

```bash
curl -s http://localhost:8000/api/v1/status | python -m json.tool
```

Or with make:

```bash
make health
```

Look for `"status": "outage"` or `"status": "degraded"` under the affected provider.

### 2b. Check per-provider health

```bash
curl -s http://localhost:8000/api/v1/providers/<provider-name> | python -m json.tool
```

Key fields to review:

| Field | Healthy value | Concerning value |
|---|---|---|
| `status` | `healthy` | `degraded` or `outage` |
| `events_last_60s` | > 0 | 0 |
| `dlq_rate_last_60s` | < 0.05 | > 0.20 |
| `quality_score_p50` | > 0.90 | < 0.60 |
| `last_event_at` | Within the last 5 minutes | Older than 10 minutes |

### 2c. Check the emulator / ingestor logs

```bash
docker compose logs --tail=100 provider-emulator
```

Look for:
- `ConnectionRefusedError` or `KafkaException` — Kafka/Redpanda connectivity issue.
- `DatabentAPIError` — upstream Databento API failure.
- `emitter_paused` log line — the emitter was deliberately paused (e.g. by a chaos test).

### 2d. Check Redpanda topic consumer lag

Via the Grafana dashboard (`:3000`, dashboard: **MDRP Consumer Lag**) or:

```bash
docker compose exec redpanda rpk group describe validation-service \
  --brokers=redpanda:9092
```

A growing lag on `market.events.raw` indicates the emitter is producing but the validator is not consuming. A zero or static lag with no new messages confirms a production-side outage.

---

## 3. Mitigation

### Option A — Wait for provider to recover

If the outage is upstream (e.g. Databento API is down), there is no action except monitoring. The platform will resume processing automatically when the provider recovers, because:
- Redpanda retains messages for 7 days (`retention.ms = 604800000`).
- The validation-service consumer group offset is committed only after successful processing, so no events are lost.

### Option B — Trigger a Bronze S3 replay

If the provider was delivering data but the platform ingestion pipeline was broken (e.g. validation-service crashed), you can replay from the already-written Bronze Parquet files:

```bash
make replay
```

Or with a custom time window:

```bash
curl -s -X POST http://localhost:8000/api/v1/replay \
  -H "Content-Type: application/json" \
  -d '{
    "source": "bronze_s3",
    "provider": "ice-endex",
    "start_time": "2026-05-20T08:00:00Z",
    "end_time": "2026-05-20T10:00:00Z",
    "requested_by": "on-call"
  }' | python -m json.tool
```

The replay engine will re-emit the events from S3 onto `market.events.replay`, which feeds back into the validation → normalisation → Snowflake pipeline. The `is_replay=true` flag on each event ensures idempotent Snowflake COPY INTO operations prevent duplicate loads.

### Option C — Trigger a Databento historical replay

If neither Bronze files nor the live feed is available (e.g. the outage happened before Bronze writer ran), trigger a historical backfill from Databento:

```bash
curl -s -X POST http://localhost:8000/api/v1/replay \
  -H "Content-Type: application/json" \
  -d '{
    "source": "databento_historical",
    "provider": "databento",
    "instrument": "TTF_CAL25",
    "start_time": "2026-05-20T08:00:00Z",
    "end_time": "2026-05-20T10:00:00Z",
    "requested_by": "on-call"
  }' | python -m json.tool
```

Note: Databento historical replays are rate-limited and may take several minutes to backfill a large window.

---

## 4. Recovery Verification

Watch the following metrics in Grafana to confirm full recovery:

| Metric / Panel | Expected value after recovery |
|---|---|
| `mdrp_events_raw_total` rate | Resuming at the provider's expected frequency |
| `mdrp_provider_status{status="healthy"}` | 1 for the affected provider |
| `mdrp_consumer_lag_total{group="validation-service"}` | Falling toward 0 |
| `mdrp_events_validated_total{outcome="passed"}` rate | Resuming at pre-outage rate |
| `mdrp_quality_score_p50` | Above 0.90 |
| `mdrp_forward_curve_completeness` | Above 0.95 for the affected instrument |

Recovery is confirmed when all panels have returned to their pre-outage baseline for at least 5 consecutive minutes.

---

## 5. Post-Incident

### 5a. Inspect the DLQ

Events that were produced during degraded operation (e.g. a schema change mid-outage) may have been routed to the DLQ. Inspect them:

```bash
curl -s "http://localhost:8000/api/v1/dlq?limit=20" | python -m json.tool
```

If DLQ events are recoverable (e.g. they failed due to a transient connectivity issue rather than a permanent schema change), trigger a DLQ replay:

```bash
make dlq-replay
```

### 5b. Review quality scores

```bash
curl -s "http://localhost:8000/api/v1/providers/<name>" | python -m json.tool
```

Check `events_last_60s` and `last_event_at` to confirm the provider is healthy. For quality score trends, open the Grafana **Pipeline Overview** dashboard (`:3000`) and inspect the **Provider Quality Scores** panel — a dip ahead of the alert suggests the threshold should be tightened.

### 5c. File an incident report

Document:
1. Time of outage onset (from `last_event_at` in provider health).
2. Time of detection (from alert firing timestamp in AlertManager).
3. Root cause (upstream API, internal service crash, network, etc.).
4. Events lost / replayed (from `events_replayed` in the replay job response).
5. Any forward curve snapshots marked incomplete (`completeness < 1.0`).
