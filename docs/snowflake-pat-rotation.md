# Snowflake PAT Token Rotation

PAT (Programmatic Access Token) tokens have a configurable expiry (maximum 1 year). When a token expires, the silver-loader and gold-loader start in no-op mode silently — Snowflake loads stop without crashing the pipeline.

## Detecting expiry

- Grafana: `mdrp_snowflake_loads_total{outcome="failed"}` rate rises
- Container logs: `snowflake_initial_connect_failed` event in silver-loader or gold-loader
- Silence: `mdrp_snowflake_rows_loaded_total{layer="silver"}` counter stops incrementing

## Rotating via Snowflake UI (recommended)

1. Log into [app.snowflake.com](https://app.snowflake.com)
2. Click your username (top-right) → **Programmatic Access Tokens**
3. Click **Generate Token**, set the desired expiry (up to 365 days), copy the token
4. Update `.env`:
   ```bash
   SNOWFLAKE_PAT_TOKEN=eyJ...new-token...
   ```
5. Restart the loaders:
   ```bash
   docker compose restart silver-loader gold-loader
   ```
6. Verify: watch logs for `snowflake_connected` within 30 s

## Production (AWS ECS): update via Secrets Manager

```bash
aws secretsmanager put-secret-value \
  --secret-id mdrp/prod/snowflake-pat \
  --secret-string '{"pat_token":"eyJ...new..."}'
```

Then trigger a new ECS task deployment to pick up the updated secret.

## Migrate to RSA key-pair (no expiry)

RSA key-pair auth does not expire and is more suitable for production service accounts.

```bash
# Generate a 2048-bit RSA key pair
openssl genrsa -out snowflake_rsa_key.pem 2048
openssl rsa -in snowflake_rsa_key.pem -pubout -out snowflake_rsa_key.pub

# Register the public key with the Snowflake user (run in Snowflake worksheet)
ALTER USER LOKESH SET RSA_PUBLIC_KEY='<contents of snowflake_rsa_key.pub without header/footer>';

# Verify
DESC USER LOKESH;  -- RSA_PUBLIC_KEY_FP should be populated
```

Then update the Snowflake client `connect()` method to use key-pair auth:

```python
from cryptography.hazmat.primitives.serialization import load_pem_private_key

with open("snowflake_rsa_key.pem", "rb") as f:
    private_key = load_pem_private_key(f.read(), password=None)

snowflake.connector.connect(
    account=account,
    user=user,
    private_key=private_key,
    database=database,
    schema=schema,
    warehouse=warehouse,
)
```

Store the private key in AWS Secrets Manager (not in `.env` or the repo).

## Account details (this deployment)

| Field | Value |
|---|---|
| Account | `YMAUZRZ-ME29964` |
| User | `Lokesh` |
| Current token expiry | ~2026-06-05 |
| Database | `MARKET_DATA` |
