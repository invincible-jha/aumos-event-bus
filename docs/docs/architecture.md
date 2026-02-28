# Architecture

## Component Overview

```mermaid
C4Component
    title AumOS Event Bus — Component Diagram

    Container_Boundary(eventbus, "aumos-event-bus") {
        Component(api, "FastAPI Service", "Python/FastAPI", "REST API for topic, schema, stream, connector management")
        Component(topicSvc, "TopicManagementService", "Python", "Kafka topic lifecycle")
        Component(schemaSvc, "SchemaValidationService", "Python", "Protobuf schema registry")
        Component(streamSvc, "StreamProcessingService", "Python", "ksqlDB / Flink job management")
        Component(connectorSvc, "ConnectorService", "Python", "Kafka Connect lifecycle")
        Component(dlqSvc, "DLQManagementService", "Python", "Dead letter queue")
        Component(geoSvc, "GeoReplicationService", "Python", "MirrorMaker2 flows")
    }

    Container(kafka, "Apache Kafka", "Strimzi", "Event streaming backbone")
    Container(sr, "Schema Registry", "Confluent", "Protobuf schema versioning")
    Container(ksqldb, "ksqlDB", "Confluent", "SQL stream processing")
    Container(flink, "Apache Flink", "Apache", "JAR-based stream processing")
    Container(connect, "Kafka Connect", "Apache", "Source/sink connectors")
    Container(mm2, "MirrorMaker2", "Strimzi", "Cross-cluster replication")

    Rel(api, topicSvc, "delegates")
    Rel(api, schemaSvc, "delegates")
    Rel(api, streamSvc, "delegates")
    Rel(api, connectorSvc, "delegates")
    Rel(topicSvc, kafka, "AdminClient")
    Rel(schemaSvc, sr, "HTTP REST")
    Rel(streamSvc, ksqldb, "HTTP REST")
    Rel(streamSvc, flink, "HTTP REST")
    Rel(connectorSvc, connect, "HTTP REST")
    Rel(geoSvc, mm2, "Kubernetes CRD")
```

## Hexagonal Architecture

```
api/          — FastAPI routes (thin, delegates to core)
core/         — Business logic services and domain models
adapters/     — Kafka AdminClient, Schema Registry, ksqlDB, Flink, Connect clients
```

## Tenant Isolation

All topics, consumer groups, and connectors are prefixed or filtered by `tenant_id`.
The `TenantPartitioner` uses SHA-256 to deterministically map tenants to partitions,
ensuring per-tenant message ordering without dedicated partitions.

## Event Publishing

Domain events are published to Kafka via `aumos-common.EventPublisher` after every
state change (topic created, schema registered, connector created).
Audit events go to `aumos.audit.{tenant_id}`.
