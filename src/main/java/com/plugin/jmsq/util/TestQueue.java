/*
 * Copyright 2026 SKACT Tech
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.plugin.jmsq.util;

import java.util.Properties;

import com.plugin.jmsq.mq.RabbitJMSConnection;

/**
 * Manual smoke test for {@link RabbitJMSConnection}.
 * <p>
 * Connects to a RabbitMQ broker and publishes one {@code "Test Message"} to
 * each queue named on the command line. Sending declares each queue as
 * durable, so running this also creates any missing queues. It is not an
 * automated test.
 * <p>
 * Usage: {@code TestQueue <connector> [queue ...]}, where the connector is,
 * for example, {@code host=localhost&port=5672&uname=guest&password=guest}.
 * The connector can also come from the {@code rabbit.mq.connector} system
 * property. With no queue names, the message goes to {@code plugin-jmsq-test}.
 */
public class TestQueue {

	/**
	 * Runs the smoke test.
	 *
	 * @param args {@code args[0]} = RabbitMQ connector string (optional if the
	 *             system property is set); remaining arguments = queue names
	 */
	public static void main(String[] args) {
		String connector = args.length > 0 ? args[0] : System.getProperty("rabbit.mq.connector");
		if (connector == null || connector.isEmpty()) {
			System.err.println("Usage: TestQueue host=<host>&port=<port>&uname=<user>&password=<password> [queue ...]"
					+ "  (or -Drabbit.mq.connector=...)");
			return;
		}
		String[] queues = args.length > 1 ? java.util.Arrays.copyOfRange(args, 1, args.length)
				: new String[] { "plugin-jmsq-test" };

		RabbitJMSConnection queueConnector = RabbitJMSConnection.getInstance(new Properties(), connector);
		if (queueConnector == null) {
			System.err.println("Invalid connector settings: " + connector);
			return;
		}
		int failures = 0;
		for (String queue : queues) {
			boolean ok = queueConnector.send(queue, "Test Message", 0);
			System.out.println((ok ? "OK     " : "FAILED ") + queue);
			if (!ok) {
				failures++;
			}
		}
		queueConnector.deinit(null);
		System.out.println(failures == 0 ? "All sends succeeded." : failures + " send(s) failed.");
	}

}
