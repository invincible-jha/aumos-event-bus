# Consumer Group Monitoring Guide

## Listing Consumer Groups

```bash
curl http://aumos-event-bus:8000/api/v1/consumer-groups \
  -H "Authorization: Bearer $AUMOS_JWT_TOKEN"
```

## Checking Lag

```bash
curl http://aumos-event-bus:8000/api/v1/consumer-groups/my-service-group/lag \
  -H "Authorization: Bearer $AUMOS_JWT_TOKEN"
```

Response includes `total_lag` and per-partition lag breakdown.

## Event Replay

Reset consumer group offsets to reprocess events:

```bash
curl -X POST http://aumos-event-bus:8000/api/v1/consumer-groups/my-service-group/replay \
  -H "Authorization: Bearer $AUMOS_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "my-first-topic",
    "from_offset_type": "earliest"
  }'
```

For timestamp-based replay:

```bash
-d '{
  "topic": "my-first-topic",
  "from_offset_type": "timestamp",
  "from_timestamp": "2026-01-15T00:00:00Z"
}'
```
