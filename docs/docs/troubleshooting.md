# Troubleshooting

## Topic Creation Fails

**Symptom:** `POST /api/v1/topics` returns 500 with "Failed to create topic".

**Causes:**
- Kafka broker unreachable: check `AUMOS_EVENTBUS_KAFKA_BROKERS` setting
- `replication_factor` exceeds available broker count
- Topic already exists on broker (the API returns 409 for duplicates)

**Resolution:** Run `kubectl logs -l app=aumos-event-bus` and check for broker connectivity errors.

## Consumer Lag Not Available

**Symptom:** `GET /api/v1/consumer-groups/{group_id}/lag` returns empty partitions.

**Cause:** The consumer group has no committed offsets yet (new group or reset group).

**Resolution:** Ensure the consumer has consumed at least one message before checking lag.

## ksqlDB Job Fails to Create

**Symptom:** `POST /api/v1/streams` returns 500 with ksqlDB error.

**Common errors:**
- `Topic does not exist`: create the input topic first
- `Stream already exists`: drop the existing stream first using `DROP STREAM IF EXISTS name;`
- `ksqlDB server unreachable`: check `AUMOS_EVENTBUS_KSQLDB_URL`

## Schema Incompatibility

**Symptom:** Schema registration returns 422 "Schema is not compatible".

**Resolution:** Use `POST /api/v1/schemas/preview-evolution` to see exactly which fields
were removed or had type changes before attempting registration.

## MirrorMaker2 Not Starting

**Symptom:** Geo-replication flow is created but no data is replicated.

**Resolution:**
1. Check the KafkaMirrorMaker2 CRD status: `kubectl describe kafkamirrormaker2 {name} -n kafka`
2. Verify both source and target bootstrap servers are reachable from the Kafka namespace
3. Check that the topics pattern matches at least one topic on the source cluster
