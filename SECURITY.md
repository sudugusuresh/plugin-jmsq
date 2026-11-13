# Security Policy

## Supported versions

| Version | Supported |
| --- | --- |
| 1.0.x | Yes |
| < 1.0 | No |

## Reporting a vulnerability

**Do not report security problems in public issues, discussions or pull requests.**

Report them privately in one of two ways:

1. **Preferred:** GitHub's [private vulnerability reporting](https://github.com/OWNER/plugin-jmsq/security/advisories/new) (*Security* tab, then *Report a vulnerability*).
2. Email [skact.tech@gmail.com](mailto:skact.tech@gmail.com) with the subject `[SECURITY] plugin-jmsq`.

Please include:

- the affected version or commit
- a description of the problem and its impact
- steps or code to reproduce it
- any fix or mitigation you suggest

## What to expect

| Step | Target time |
| --- | --- |
| Acknowledge your report | 3 working days |
| Confirm the problem and its severity | 10 working days |
| Release a fix for a confirmed high or critical issue | 30 days |

We'll keep you updated, agree a disclosure date with you, and credit you in the advisory unless you prefer otherwise.

## Scope

In scope: code in this repository. Vulnerabilities in a dependency (ActiveMQ, Kafka, RabbitMQ client, Log4j) belong with that project; tell us too if plugin-jmsq needs a version bump because of one.

## Safe use

- Keep broker credentials out of source code and out of jars. Pass them in at runtime; see `examples/application.properties.example`.
- Use TLS listeners on your brokers in production. For ActiveMQ, use an `ssl://` transport; for Kafka, use the `kafka.security.protocol` and `kafka.ssl.*` settings.
