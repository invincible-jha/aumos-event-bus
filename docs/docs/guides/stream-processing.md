# Stream Processing Guide

AumOS Event Bus supports server-side stream processing via two backends:
**ksqlDB** (SQL-based, default) and **Apache Flink** (JAR-based).

## Creating a ksqlDB Stream Job

```bash
curl -X POST http://aumos-event-bus:8000/api/v1/streams \
  -H "Authorization: Bearer $AUMOS_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-enriched-stream",
    "query_text": "CREATE STREAM enriched AS SELECT id, payload, UCASE(payload) AS upper_payload FROM my_stream EMIT CHANGES;",
    "input_topics": ["my-first-topic"],
    "output_topic": "my-enriched-topic",
    "backend": "ksqldb"
  }'
```

## Listing Jobs

```bash
curl http://aumos-event-bus:8000/api/v1/streams \
  -H "Authorization: Bearer $AUMOS_JWT_TOKEN"
```

## Terminating a Job

```bash
curl -X DELETE http://aumos-event-bus:8000/api/v1/streams/CSAS_MY_ENRICHED_STREAM_0 \
  -H "Authorization: Bearer $AUMOS_JWT_TOKEN"
```

## Backend Selection

Set `AUMOS_EVENTBUS_STREAM_BACKEND=flink` to switch to Apache Flink for JAR-based jobs.
The `AUMOS_EVENTBUS_KSQLDB_URL` and `AUMOS_EVENTBUS_FLINK_REST_URL` settings control
the backend endpoint URLs.
