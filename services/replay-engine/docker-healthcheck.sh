#!/bin/sh
python -c "
import redis, os, sys
try:
    r = redis.from_url(os.environ.get('REDIS_URL', 'redis://localhost:6379/0'))
    r.ping()
    sys.exit(0)
except Exception:
    sys.exit(1)
"
