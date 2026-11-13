# plugin-jmsq

[![CI](https://github.com/OWNER/plugin-jmsq/actions/workflows/ci.yml/badge.svg)](https://github.com/OWNER/plugin-jmsq/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
![Java](https://img.shields.io/badge/java-8%2B-orange.svg)

A small Java 8 library that gives applications one simple send/receive API over three message brokers:

| Connector | Broker | Client library | Typical use |
| --- | --- | --- | --- |
| `AMQJMSConnection` | ActiveMQ Classic 5.x | `activemq-client` (JMS 1.1, `javax.jms`) | Queues with priorities, selectors, message properties, expiry, delayed delivery |
| `RabbitJMSConnection` | RabbitMQ 3.x | `amqp-client` (native AMQP 0-9-1) | Durable queues, delayed delivery through a TTL holding queue |
| `KafkaJMSConnection` | Apache Kafka 2.x/3.x | `kafka-clients` | Topics: send records, poll records |

All three exchange messages as UTF-8 strings.

## Build

```sh
./mvnw clean package
```

On Windows, use `mvnw.cmd`. The wrapper downloads Maven 3.9.9 on first use.

- **Output:** `target/plugin-jmsq.jar` (classes only) and `target/plugin-jmsq-1.0.1-shaded.jar` (with all dependencies, about 18 MB). Use the shaded jar when the host application doesn't already provide the broker clients.
- **Java:** compiled for Java 8; CI builds on JDK 8, 11, 17 and 21.
- **Releases:** published to Maven Central with `./mvnw -Prelease deploy`. See [CONTRIBUTING.md](CONTRIBUTING.md#releasing-maintainers).

### Use as a dependency

Once released to Maven Central (the `groupId` will change to the project's verified namespace):

```xml
<dependency>
  <groupId>com.plugin</groupId>
  <artifactId>plugin-jmsq</artifactId>
  <version>1.0.1</version>
</dependency>
```

The library ships no configuration files. Start from [examples/application.properties.example](examples/application.properties.example) and [examples/log4j2.xml](examples/log4j2.xml).

## Quick start

### ActiveMQ

```java
Properties props = new Properties();
props.setProperty("amq.producer-window", "1048576");   // required
props.setProperty("amq.persist.enable", "true");

AMQJMSConnection producer = AMQJMSConnection.getInstance(
        props, "host=10.0.0.5&senderport=61616&user=app&password=secret",
        "SMS_MT_QUEUE", true, false);
producer.send("hello", 4);                               // text, JMS priority 0..9

// expiry and delayed delivery
List<PropertyBean> opts = Arrays.asList(
        new PropertyBean(PropertyBean.STRING_PROPERTY, "msisdn", "6591234567"),
        new PropertyBean(PropertyBean.EXPIRY_PROPERTY, null, "30"),        // expire after 30 min
        new PropertyBean(PropertyBean.SCHEDULE_SECS_PROPERTY, null, "60")); // deliver in 60 s
producer.send("hello later", 4, opts);
producer.deinit();

AMQJMSConnection consumer = AMQJMSConnection.getInstance(
        props, "host=10.0.0.5&senderport=61616", "SMS_MT_QUEUE", false, true);
TextMessage m = consumer.receive();                     // waits up to 50 ms, null if none
consumer.deinit();
```

### RabbitMQ

```java
RabbitJMSConnection rabbit = RabbitJMSConnection.getInstance(
        props, "host=10.0.0.6&port=5672&uname=app&password=secret");
rabbit.send("SMS_MT_QUEUE", "hello", 0);
rabbit.send("SMS_RETRYQ", "retry me", 0, 30000);        // delivered after 30 s
String body = rabbit.receive("SMS_MT_QUEUE");           // null if the queue is empty
rabbit.deinit(null);                                     // close the connection
```

### Kafka

```java
Properties props = new Properties();
props.setProperty("kafka.group.id", "sms-workers");

KafkaJMSConnection kafka = KafkaJMSConnection.getInstance(
        props, "bootstrap=10.0.0.7:9092", null, "sms-events", true, true);
kafka.send(null, "hello", 0);                            // null topic = "sms-events"
String record = kafka.receive(null);                     // null if nothing arrived
kafka.deinit();
```

## Configuration

### Connector strings

`key=value` pairs joined with `&`. Values can't contain `&` or `=`.

| Connector | Keys |
| --- | --- |
| ActiveMQ | `host`, `senderport`, `receiverport`, `user` (default `admin`), `password` (default `admin`) |
| RabbitMQ | `host`, `port`, `uname`, `password`, `vhost` (default `/`) |
| Kafka | `bootstrap` (`host:port[,host:port]`), or `host` + `port` |

### Properties

| Property | Default | Used by | Meaning |
| --- | --- | --- | --- |
| `amq.host`, `amq.port` | none | ActiveMQ, RabbitMQ | Broker address when the connector string doesn't give one |
| `amq.user`, `amq.password` | `guest`/`guest` | RabbitMQ | Credentials when the connector string doesn't give them |
| `amq.producer-window` | **required** | ActiveMQ | Producer flow-control window, bytes |
| `amq.transacted` | `false` | ActiveMQ | Transacted session, committed in batches |
| `amq.commit.records.count` | `2000` | ActiveMQ | Commit after this many messages (transacted only) |
| `amq.commit.interval` | `2` | ActiveMQ | Or after this many seconds (transacted only) |
| `amq.persist.enable` | `false` | ActiveMQ | Persistent delivery |
| `amq.use.asyncsend` | `true` | ActiveMQ | Asynchronous sends |
| `amq.prefetch.size` | `1000` | ActiveMQ | Consumer prefetch |
| `amq.compress.msg.enable` | `false` | ActiveMQ | Compress message bodies |
| `amq.tcp-buffer` | `20971520` | ActiveMQ | Socket buffer, bytes |
| `amq.statistics.timeout.ms` | `2000` | ActiveMQ | Wait for the broker statistics plugin |
| `qmanager.db.driver`, `qmanager.db.url` | none | ActiveMQ | JDBC for `logQConsumerCounter()` |
| `rabbit.queue.max-priority` | `0` (off) | RabbitMQ | Declare queues with `x-max-priority` |
| `kafka.bootstrap.servers` | none | Kafka | Brokers when the connector string doesn't give them |
| `kafka.group.id` | `plugin-jmsq` | Kafka | Consumer group |
| `kafka.poll.timeout.ms` | `100` | Kafka | How long `receive` waits |
| `kafka.send.timeout.ms` | `10000` | Kafka | How long `send` waits for the ack |
| `kafka.*` (any other) | n/a | Kafka | Passed to the Kafka clients with the `kafka.` prefix removed |

## Broker requirements

- **ActiveMQ delayed delivery:** set `schedulerSupport="true"` on the `<broker>` element in `activemq.xml`. Otherwise `SCHEDULE_*` delays are ignored and messages are delivered at once.
- **ActiveMQ counts:** `getConsumerCount()` and exact `getMessageCount()` need `<statisticsBrokerPlugin/>` under `<plugins>`. Without it, `getMessageCount()` browses the queue, which counts at most 400 messages by default, and `getConsumerCount()` returns `-1`.
- **RabbitMQ priority:** only queues declared with `x-max-priority` honour priority. You can't change an existing queue's arguments: delete the queue before setting `rabbit.queue.max-priority`, or the declare fails with `PRECONDITION_FAILED`.

## Error handling

Methods report failure through their return value: `false`, `null` or `-1`. They don't throw. Each connector counts failed operations in its public field `m_nFailureCount`. For ActiveMQ and Kafka this is a run of consecutive failures, reset to 0 on success; RabbitMQ never resets it. The RabbitMQ connector reconnects and retries once when its connection has dropped. The other connectors expect the caller to call `deinit()` and create a new instance.

## Threading

None of the connectors is safe to share between threads for receiving. Use one instance per consumer thread. `KafkaJMSConnection.send` is the only method that is thread-safe.

## Manual smoke tests

```sh
# RabbitMQ: one message to each named queue (missing queues are created)
java -cp target/plugin-jmsq-1.0.1-shaded.jar com.plugin.jmsq.util.TestQueue \
     "host=localhost&port=5672&uname=guest&password=guest" orders invoices

# ActiveMQ: one message to a queue, using a properties file
java -cp target/plugin-jmsq-1.0.1-shaded.jar com.plugin.jmsq.mq.AMQJMSConnection \
     my-settings.properties orders
```

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `AMQJMSConnection.getInstance` returns `null` | Set `amq.producer-window`; check the host and port |
| ActiveMQ delayed messages arrive at once | Add `schedulerSupport="true"` to `<broker>` |
| `getConsumerCount()` returns `-1` | Enable `<statisticsBrokerPlugin/>` |
| RabbitMQ `PRECONDITION_FAILED` | The queue exists with different arguments; delete it or match `rabbit.queue.max-priority` |
| `KafkaJMSConnection.getInstance` returns `null` | Set `bootstrap=` in the connector or `kafka.bootstrap.servers` |
| Kafka consumer misses old records | Set `kafka.auto.offset.reset=earliest` |

## Contributing

Contributions are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md) and the [Code of Conduct](CODE_OF_CONDUCT.md) first. Report security problems privately as described in [SECURITY.md](SECURITY.md). Changes are listed in [CHANGELOG.md](CHANGELOG.md).

## License

Copyright 2024-2026 SKACT Tech. Licensed under the [Apache License, Version 2.0](LICENSE).

Maintainer contact: [skact.tech@gmail.com](mailto:skact.tech@gmail.com)
