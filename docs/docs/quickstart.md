# Quickstart

Get your first Kafka topic provisioned and producing events in 5 minutes.

## Prerequisites

- AumOS platform installed (`aumos status` shows healthy)
- `AUMOS_JWT_TOKEN` set in your shell (obtain from auth-gateway)
- Event Bus API accessible at `http://aumos-event-bus:8000`

## Step 1: Create a Topic

```bash
curl -X POST http://aumos-event-bus:8000/api/v1/topics \
  -H "Authorization: Bearer $AUMOS_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "topic_name": "my-first-topic",
    "display_name": "My First Topic",
    "partitions": 6,
    "replication_factor": 3,
    "retention_ms": 604800000
  }'
```

## Step 2: Register a Schema

```bash
curl -X POST http://aumos-event-bus:8000/api/v1/schemas \
  -H "Authorization: Bearer $AUMOS_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "subject": "my-first-topic-value",
    "schema_definition": "syntax = \"proto3\";\nmessage MyEvent { string id = 1; string payload = 2; }",
    "schema_type": "PROTOBUF",
    "compatibility": "BACKWARD"
  }'
```

## Step 3: Check Consumer Group Lag

```bash
curl http://aumos-event-bus:8000/api/v1/consumer-groups/my-consumer-group/lag \
  -H "Authorization: Bearer $AUMOS_JWT_TOKEN"
```

## Step 4: Preview a Schema Evolution

```bash
curl -X POST http://aumos-event-bus:8000/api/v1/schemas/preview-evolution \
  -H "Authorization: Bearer $AUMOS_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "subject": "my-first-topic-value",
    "new_schema_definition": "syntax = \"proto3\";\nmessage MyEvent { string id = 1; string payload = 2; string timestamp = 3; }"
  }'
```

The preview returns `is_compatible`, `diff`, and `recommendation` before you commit the change.
