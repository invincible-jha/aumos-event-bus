# API Reference

The full API reference is auto-generated from the OpenAPI specification.

Access the interactive API docs at runtime:
- Swagger UI: `http://aumos-event-bus:8000/docs`
- ReDoc: `http://aumos-event-bus:8000/redoc`
- OpenAPI JSON: `http://aumos-event-bus:8000/openapi.json`

## Endpoint Summary

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/v1/topics | Create a Kafka topic |
| GET | /api/v1/topics | List topics |
| GET | /api/v1/topics/{id} | Get topic |
| DELETE | /api/v1/topics/{id} | Delete topic |
| PUT | /api/v1/topics/{name}/tiered-storage | Configure tiered storage |
| POST | /api/v1/schemas | Register schema |
| GET | /api/v1/schemas/{subject} | Get latest schema |
| POST | /api/v1/schemas/preview-evolution | Preview schema evolution |
| POST | /api/v1/streams | Create stream job |
| GET | /api/v1/streams | List stream jobs |
| DELETE | /api/v1/streams/{id} | Terminate stream job |
| POST | /api/v1/connectors | Create connector |
| GET | /api/v1/connectors | List connectors |
| GET | /api/v1/connectors/{name}/status | Connector status |
| DELETE | /api/v1/connectors/{name} | Delete connector |
| POST | /api/v1/connectors/{name}/restart | Restart connector |
| GET | /api/v1/consumer-groups | List consumer groups |
| GET | /api/v1/consumer-groups/{id}/lag | Consumer group lag |
| POST | /api/v1/consumer-groups/{id}/replay | Event replay |
| POST | /api/v1/geo-replication | Create replication flow |
| GET | /api/v1/geo-replication | List flows |
| DELETE | /api/v1/geo-replication/{name} | Delete flow |
| GET | /api/v1/dlq | List DLQ entries |
| POST | /api/v1/dlq/{id}/retry | Retry DLQ entry |
| POST | /api/v1/dlq/{id}/resolve | Resolve DLQ entry |
