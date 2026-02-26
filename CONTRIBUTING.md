# Contributing to aumos-event-bus

Thank you for contributing to AumOS Enterprise. This guide covers everything you need
to get started and ensure your contributions meet our standards.

## Getting Started

1. Fork the repository (external contributors) or clone directly (AumOS team members)
2. Create a feature branch from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/bug-description
   ```
3. Make your changes following the standards below
4. Submit a pull request targeting `main`

## Development Setup

### Prerequisites

- Python 3.11 or 3.12
- Docker and Docker Compose
- Access to AumOS internal PyPI (for `aumos-common` and `aumos-proto`)

### Install

```bash
# Install all dependencies including dev tools
make install

# Copy and configure environment
cp .env.example .env
# Edit .env with your local settings

# Start local infrastructure (Kafka KRaft, Schema Registry, PostgreSQL)
make docker-run
```

### Verify Setup

```bash
make lint       # Should pass with no errors
make typecheck  # Should pass with no errors
make test       # Should pass with coverage >= 80%
```

## Code Standards

All code in this repository must follow the standards defined in [CLAUDE.md](CLAUDE.md).
Key requirements:

- **Type hints on every function** — no exceptions
- **Pydantic models for all API inputs/outputs** — never return raw dicts
- **Structured logging** — use `get_logger(__name__)`, never `print()`
- **Async by default** — all I/O must be async
- **Import from aumos-common** — never reimplement shared utilities
- **Google-style docstrings** on all public classes and methods
- **Max line length: 120 characters**

Run `make lint` and `make typecheck` before every commit.

## Domain-Specific Rules

### Tenant Partitioner Seed

**NEVER change `tenant_partitioner_seed` in settings.** Changing it will shift all
tenant-to-partition assignments, breaking per-tenant message ordering guarantees
for all active consumers.

### Schema Compatibility

All schema registrations default to BACKWARD compatibility. To register an
incompatible schema, you must first update the subject's compatibility mode via
`POST /api/v1/schemas/{subject}/compatibility`.

### DLQ Retry Logic

Do not modify `calculate_next_retry_ms()` without understanding the impact on
active DLQ entries. Background workers depend on the `next_retry_at` timestamps
calculated by this method.

## PR Process

1. Ensure all CI checks pass (lint, typecheck, test, docker build, license check)
2. Fill out the PR template completely
3. Request review from at least one member of `@aumos/platform-team`
4. Squash merge only — keep history clean
5. Delete your branch after merge

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add endpoint for batch topic creation
fix: resolve race condition in DLQ retry scheduling
refactor: extract Schema Registry auth into adapter
docs: update API reference for partition endpoint
test: add integration tests for DLQ exponential backoff
chore: bump confluent-kafka to 2.4.0
```

Commit messages explain **WHY**, not just what changed.

## License Compliance — CRITICAL

AumOS Enterprise is licensed under Apache 2.0. Our enterprise customers prohibit
AGPL and GPL licensed code in the platform.

**NEVER add a dependency with a GPL or AGPL license**, even indirectly.

### Approved Licenses

- MIT, BSD (2-clause or 3-clause), Apache 2.0, ISC, PSF, MPL 2.0 (with restrictions)

```bash
# Check license before adding any new package
pip install pip-licenses
pip install <new-package>
pip-licenses --packages <new-package>
```

## Testing Requirements

- Coverage must remain >= 80% for `core/` modules
- Coverage must remain >= 60% for `adapters/`
- Use `testcontainers` for integration tests requiring real infrastructure
- Mock external services (Kafka, Schema Registry) in unit tests

```bash
make test                              # Full test suite
pytest tests/test_services.py -v      # Specific file
pytest tests/ --cov --cov-report=html # HTML coverage report
```

## Code of Conduct

Be respectful, constructive, and focused on what is best for the platform.
Violations may result in removal from the project.
