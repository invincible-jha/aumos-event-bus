# Geo-Replication Guide

AumOS Event Bus supports cross-cluster topic replication via Strimzi MirrorMaker2.

## Creating a Replication Flow

```bash
curl -X POST http://aumos-event-bus:8000/api/v1/geo-replication \
  -H "Authorization: Bearer $AUMOS_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "us-to-eu",
    "source_bootstrap": "kafka-us.internal:9092",
    "target_bootstrap": "kafka-eu.internal:9092",
    "topics_pattern": "aumos\\..*"
  }'
```

## Listing Flows

```bash
curl http://aumos-event-bus:8000/api/v1/geo-replication \
  -H "Authorization: Bearer $AUMOS_JWT_TOKEN"
```

## Deleting a Flow

```bash
curl -X DELETE http://aumos-event-bus:8000/api/v1/geo-replication/us-to-eu \
  -H "Authorization: Bearer $AUMOS_JWT_TOKEN"
```

## How It Works

The API creates a Strimzi `KafkaMirrorMaker2` custom resource in the configured namespace
(`AUMOS_EVENTBUS_STRIMZI_NAMESPACE`, default: `kafka`). The Strimzi operator reconciles
the resource and starts the MirrorMaker2 connectors.

Topics matching `topics_pattern` are replicated from the source cluster to the target.
Topic names are prefixed with the source cluster alias on the target cluster.
