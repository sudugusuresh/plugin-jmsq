# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Open-source project files: Apache-2.0 `LICENSE` and `NOTICE`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`, GitHub issue and pull request templates, CI workflow and Dependabot.
- Maven Central release profile (`-Prelease`): sources and Javadoc jars, GPG signing, Central Portal publishing.
- `examples/application.properties.example` and `examples/log4j2.xml`.
- `KafkaJMSConnection`: working consumer and producer on the native Kafka client, configured through `kafka.*` properties.
- `AMQJMSConnection.getMessageCount()` and `getConsumerCount()` use the broker statistics plugin; message count falls back to browsing.
- `RabbitJMSConnection`: message priority, optional priority queues (`rabbit.queue.max-priority`), `vhost` connector key, `amq.user` / `amq.password` fallbacks.
- Javadoc for every public class and method.

### Changed
- **Breaking:** `KafkaJMSConnection.receive(String)` returns `String` instead of `javax.jms.TextMessage`.
- `KafkaJMSConnection` honours `isProducer` / `isConsumer` and returns `null` when no broker address is configured.
- The shaded jar is built by the default `shaded` profile and is no longer part of Maven Central releases.
- `TestQueue` and `AMQJMSConnection.main` take the connector, properties file and queues from the command line.
- Dependencies trimmed to log4j 2.17.2, activemq-client 5.16.7, amqp-client 5.15.0 and kafka-clients 3.5.1.

### Fixed
- ActiveMQ message expiry and scheduled delay were set as clock times instead of durations, so messages effectively never expired and delays lasted decades.
- Sessions opened by the `createQueue` methods had the transacted flag inverted.
- `logQConsumerCounter` built its SQL by string concatenation; it now uses a prepared statement.
- `RabbitJMSConnection.deinit(null)` threw a `NullPointerException`.
- `RabbitJMSConnection` kept using closed channels; they are now replaced on next use.
- `RabbitJMSConnection` used the broker host as the fallback username and password.

### Removed
- Private artifact repository settings from `pom.xml` and `.mvn/extensions.xml`.
- `src/main/resources/application.properties` and `log4j.xml`. A library should not ship application settings or logging configuration; see `examples/`.

### Security
- Removed hard-coded broker and database credentials from the source tree and the packaged jar.

## [1.0.1] - 2025-09-05

Internal release before open-sourcing.
