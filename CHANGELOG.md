# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial project scaffolding for aumos-event-bus
- FastAPI management API with topic, schema, DLQ, and partition endpoints
- `TopicManagementService` — provisions Kafka topics with full lifecycle management
- `SchemaValidationService` — registers Protobuf schemas via Confluent Schema Registry
- `DLQManagementService` — manages failed messages with exponential backoff retry
- `TenantPartitioner` — deterministic SHA-256-based tenant-to-partition assignment
- `KafkaAdminAdapter` — confluent-kafka AdminClient wrapper for topic operations
- `SchemaRegistryAdapter` — httpx-based Confluent Schema Registry REST client
- `EventBusEventPublisher` — publishes internal audit events via aumos-common
- SQLAlchemy repositories for `evt_topic_definitions`, `evt_schema_versions`, `evt_dlq_entries`
- `ConfluentKafkaTenantPartitioner` — drop-in partitioner for confluent-kafka producers
- Multi-stage Dockerfile with non-root runtime user
- Docker Compose dev stack: Kafka (KRaft), Schema Registry, PostgreSQL
- Standard project files: CLAUDE.md, README.md, pyproject.toml, Makefile, CI workflow
