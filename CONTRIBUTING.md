# Contributing to KafkaPulse

Thanks for your interest in contributing! Here's how to get started.

## Prerequisites

- **Java 11+**
- **Maven 3.6+**
- **Docker** (for the local Kafka broker)

## Local Development Setup

```bash
# 1. Clone the repo
git clone https://github.com/vishsharmaa/kafka-pulse.git
cd kafka-pulse

# 2. Start Kafka (KRaft mode, no ZooKeeper)
docker compose up -d

# 3. Build and run tests
mvn clean package

# 4. Run the three components in separate terminals
java -jar target/kafka-pulse-1.0.0.jar produce
java -jar target/kafka-pulse-1.0.0.jar consume
java -jar target/kafka-pulse-1.0.0.jar monitor
```

## Running Tests

```bash
mvn test
```

All new features should include corresponding unit tests. The project
uses JUnit 5. Place tests under `src/test/java/` mirroring the main
source package structure.

## Commit Messages

This project follows [Conventional Commits](https://www.conventionalcommits.org/):

| Prefix   | Use for                                  |
|----------|------------------------------------------|
| `feat:`  | New features                             |
| `fix:`   | Bug fixes                                |
| `test:`  | Adding or improving tests                |
| `docs:`  | Documentation changes                    |
| `chore:` | Build config, CI, dependencies           |
| `refactor:` | Code changes that don't fix bugs or add features |

## Code Style

- Follow standard Java conventions (4-space indentation, braces on same line).
- Keep classes `final` unless there's a reason for extension.
- Separate I/O from pure logic (see `LagCalculator` vs `LagMonitor`).
- No external web framework dependencies — use the JDK's built-in `HttpServer`.

## Pull Requests

1. Fork the repo and create a feature branch from `main`.
2. Make your changes with tests.
3. Ensure `mvn clean package` passes.
4. Open a PR with a clear description of the change.
