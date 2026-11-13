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
package com.plugin.jmsq.mq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.plugin.jmsq.util.QUtil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;

/**
 * RabbitMQ connector built on the native AMQP client ({@code com.rabbitmq.client}),
 * not on JMS.
 * <p>
 * One instance holds one AMQP {@link Connection} and opens one {@link Channel}
 * per queue, cached separately for consuming and producing. A cached channel
 * that has been closed (for example by a broker error) is replaced on next use.
 * Every queue it touches is declared as durable, non-exclusive and not
 * auto-deleted, so it is created if missing. Messages are UTF-8 text sent
 * through the default exchange.
 * <p>
 * If {@link #receive(String)} or {@link #send(String, String, int)} fails and
 * the connection has dropped, the connector reconnects with the settings it was
 * created with and retries the operation once.
 *
 * <h2>Connector string</h2>
 * {@code host=<host>&port=<port>&uname=<user>&password=<password>&vhost=<vhost>}.
 * Missing values fall back to the properties {@code amq.host}, {@code amq.port},
 * {@code amq.user} and {@code amq.password}, then to the RabbitMQ defaults
 * ({@code localhost}, 5672, {@code guest}, {@code guest}). {@code vhost}
 * defaults to {@code /}.
 *
 * <h2>Priority</h2>
 * Every message carries its priority (clamped to 0..255). RabbitMQ only orders
 * by priority on queues declared with {@code x-max-priority}; set the property
 * {@code rabbit.queue.max-priority} (1..255) to declare queues that way. A
 * queue's arguments cannot change after it is created, so an existing queue
 * must be deleted before it can become a priority queue; otherwise declaring
 * it fails with {@code PRECONDITION_FAILED}.
 */
public class RabbitJMSConnection {

	static Logger LOGGER = LogManager.getLogger(RabbitJMSConnection.class);
	private static Object objSync = new Object();
	/** The AMQP connection shared by all channels. */
	Connection connection = null;
	/** Number of failed send or receive calls since the connector was created. */
	public int m_nFailureCount = 0;
	/** Channels used for receiving, keyed by queue name. */
	Hashtable<String, Channel> mConsumerList = new Hashtable<String, Channel>();
	/** Channels used for sending, keyed by queue name (including {@code <queue>_DELAY} queues). */
	Hashtable<String, Channel> mProducerList = new Hashtable<String, Channel>();

	/** Value of {@code x-max-priority} for declared queues; 0 declares ordinary queues. */
	private int maxPriority = 0;

	// Store last used props and connector for reconnection
	private Properties lastProps = null;
	private String lastQConnector = null;

	private boolean isConnectionOpen() {
		try {
			return connection != null && connection.isOpen();
		} catch (Exception e) {
			return false;
		}
	}

	private synchronized boolean reconnect(Properties props, String qConnector) {
		try {
			if (connection != null) {
				try { connection.close(); } catch (Exception ignore) {}
			}
			return init(props, qConnector);
		} catch (Exception e) {
			LOGGER.error("RabbitJMSConnection : reconnect() Failed, Ex : " + e.getMessage(), e);
			return false;
		}
	}

	private void storeInitParams(Properties props, String qConnector) {
		this.lastProps = props;
		this.lastQConnector = qConnector;
	}

	/**
	 * Creates a connector and opens the AMQP connection.
	 *
	 * @param props      fallback settings and {@code rabbit.queue.max-priority};
	 *                   see the class description
	 * @param qConnector connector string,
	 *                   {@code host=..&port=..&uname=..&password=..}; may be {@code null}
	 * @return the connector. If the broker cannot be reached, the error is
	 *         logged and the connector is still returned: the first send or
	 *         receive will try to connect again. {@code null} is returned only
	 *         if the settings themselves are invalid.
	 */
	public static RabbitJMSConnection getInstance(Properties props, String qConnector) {
		synchronized (objSync) {
			try {
				RabbitJMSConnection obj = new RabbitJMSConnection();
				Properties safeProps = props != null ? props : new Properties();
				obj.storeInitParams(safeProps, qConnector);
				obj.init(safeProps, qConnector);
				return obj;
			} catch (Exception e) {
				LOGGER.error("RabbitJMSConnection : could not initialize, Ex : " + e.getMessage(), e);
			}
			return null;
		} // end of sync object
	}

	/**
	 * Opens the AMQP connection.
	 *
	 * @return {@code true} if the connection is open
	 */
	private boolean init(Properties props, String qConnector) {
		Hashtable<String, String> qParams = QUtil.getExtraparamslist(qConnector != null ? qConnector : "");
		String host = firstNonNull(qParams.get("host"), props.getProperty("amq.host"));
		String port = firstNonNull(qParams.get("port"), props.getProperty("amq.port"));
		String uname = firstNonNull(qParams.get("uname"), props.getProperty("amq.user"));
		String password = firstNonNull(qParams.get("password"), props.getProperty("amq.password"));
		String vhost = firstNonNull(qParams.get("vhost"), "/");
		maxPriority = Math.max(0, Math.min(255, QUtil.intVal(props.getProperty("rabbit.queue.max-priority"), 0)));

		ConnectionFactory rmqFactory = new ConnectionFactory();
		if (host != null) {
			rmqFactory.setHost(host);
		}
		if (port != null) {
			rmqFactory.setPort(Integer.parseInt(port.trim()));
		}
		if (uname != null) {
			rmqFactory.setUsername(uname);
		}
		if (password != null) {
			rmqFactory.setPassword(password);
		}
		rmqFactory.setVirtualHost(vhost);
		try {
			connection = rmqFactory.newConnection();
			return true;
		} catch (Exception e) {
			connection = null;
			LOGGER.error("RabbitJMSConnection : connect to " + rmqFactory.getHost() + ":" + rmqFactory.getPort()
					+ " Failed, Ex : " + e.getMessage(), e);
		}
		return false;
	}

	private static String firstNonNull(String a, String b) {
		return a != null ? a : b;
	}

	/** Arguments for an ordinary queue: {@code x-max-priority} when configured, else none. */
	private Map<String, Object> queueArgs() {
		if (maxPriority <= 0) {
			return null;
		}
		Map<String, Object> args = new Hashtable<String, Object>();
		args.put("x-max-priority", maxPriority);
		return args;
	}

	/**
	 * Returns the cached channel for a queue, opening a new one and declaring
	 * the queue if there is none or the cached one has been closed.
	 */
	private Channel channelFor(Hashtable<String, Channel> cache, String queue, Map<String, Object> args)
			throws IOException {
		Channel channel = cache.get(queue);
		if (channel != null && channel.isOpen()) {
			return channel;
		}
		if (connection == null) {
			throw new IOException("not connected");
		}
		channel = connection.createChannel();
		channel.queueDeclare(queue, true, false, false, args);
		cache.put(queue, channel);
		return channel;
	}

	private static AMQP.BasicProperties messageProps(int nPriority) {
		return new AMQP.BasicProperties.Builder().contentType("text/plain").contentEncoding("UTF-8")
				.priority(Math.max(0, Math.min(255, nPriority))).build();
	}

	/**
	 * Fetches one message from a queue without waiting. The message is
	 * acknowledged automatically.
	 *
	 * @param QUEUE_NAME queue to read from; declared if it does not exist
	 * @return the message body as UTF-8 text, or {@code null} if the queue is
	 *         empty or the read failed
	 */
	public String receive(String QUEUE_NAME) {
		return receive(QUEUE_NAME, false);
	}

	private String receive(String QUEUE_NAME, boolean isRetry) {
		try {
			Channel channel = channelFor(mConsumerList, QUEUE_NAME, queueArgs());
			GetResponse response = channel.basicGet(QUEUE_NAME, true);
			if (response != null) {
				return new String(response.getBody(), StandardCharsets.UTF_8);
			}
		} catch (Exception e) {
			m_nFailureCount++;
			LOGGER.error("RabbitJMSConnection : receive() Failed, Ex : " + e.getMessage(), e);
			if (!isRetry && lastProps != null && !isConnectionOpen()) {
				LOGGER.info("Connection lost. Attempting to reconnect and retry receive...");
				if (reconnect(lastProps, lastQConnector)) {
					mConsumerList.clear();
					return receive(QUEUE_NAME, true);
				}
			}
		}
		return null;
	}

	/**
	 * Publishes a text message to a queue.
	 *
	 * @param QUEUE_NAME queue to publish to; declared if it does not exist
	 * @param strMessage message body, sent as UTF-8
	 * @param nPriority  message priority, 0..255; only honoured by priority
	 *                   queues (see the class description)
	 * @return {@code true} if the message was published
	 */
	public boolean send(String QUEUE_NAME, String strMessage, int nPriority) {
		return send(QUEUE_NAME, strMessage, nPriority, false);
	}

	/**
	 * Publishes a text message to a queue, reconnecting and retrying once on
	 * failure unless {@code isRetry} is {@code true}.
	 *
	 * @param QUEUE_NAME queue to publish to; declared if it does not exist
	 * @param strMessage message body, sent as UTF-8
	 * @param nPriority  message priority, 0..255; only honoured by priority queues
	 * @param isRetry    {@code true} if this call is already a retry, which
	 *                   disables further reconnect attempts
	 * @return {@code true} if the message was published
	 */
	public boolean send(String QUEUE_NAME, String strMessage, int nPriority, boolean isRetry) {
		try {
			Channel channel = channelFor(mProducerList, QUEUE_NAME, queueArgs());
			channel.basicPublish("", QUEUE_NAME, messageProps(nPriority), strMessage.getBytes(StandardCharsets.UTF_8));
			LOGGER.debug("RabbitJMSConnection : send() Success, Queue : " + QUEUE_NAME);
			return true;
		} catch (Exception e) {
			m_nFailureCount++;
			LOGGER.error("RabbitJMSConnection : send() Failed, Ex : " + e.getMessage() + ", Queue : " + QUEUE_NAME
					+ ", Query : " + strMessage, e);
			if (!isRetry && lastProps != null && !isConnectionOpen()) {
				LOGGER.info("Connection lost. Attempting to reconnect and retry send...");
				if (reconnect(lastProps, lastQConnector)) {
					mProducerList.clear();
					return send(QUEUE_NAME, strMessage, nPriority, true);
				}
			}
		}
		return false;
	}

	/**
	 * Publishes a text message that is delivered to {@code QUEUE_NAME} after a
	 * delay.
	 * <p>
	 * The message is published to a holding queue named
	 * {@code <QUEUE_NAME>_DELAY} whose message TTL is {@code nDelay}. When the
	 * TTL expires, RabbitMQ dead-letters the message into {@code QUEUE_NAME}.
	 * The TTL is fixed when the holding queue is first declared, so every later
	 * call for the same queue must use the same delay.
	 *
	 * @param QUEUE_NAME target queue; declared if it does not exist
	 * @param strMessage message body, sent as UTF-8
	 * @param nPriority  message priority, 0..255; applies once the message
	 *                   reaches {@code QUEUE_NAME}
	 * @param nDelay     delay in milliseconds
	 * @return {@code true} if the message was published to the holding queue
	 */
	public boolean send(String QUEUE_NAME, String strMessage, int nPriority, int nDelay) {
		return send(QUEUE_NAME, strMessage, nPriority, nDelay, false);
	}

	private boolean send(String QUEUE_NAME, String strMessage, int nPriority, int nDelay, boolean isRetry) {
		try {
			// Make sure the target queue exists before messages are dead-lettered into it.
			channelFor(mProducerList, QUEUE_NAME, queueArgs());
			String delayQueue = QUEUE_NAME + "_DELAY";
			Map<String, Object> args = new Hashtable<String, Object>();
			args.put("x-dead-letter-exchange", "");
			args.put("x-dead-letter-routing-key", QUEUE_NAME);
			args.put("x-message-ttl", nDelay);
			Channel channel = channelFor(mProducerList, delayQueue, args);
			channel.basicPublish("", delayQueue, messageProps(nPriority), strMessage.getBytes(StandardCharsets.UTF_8));
			return true;
		} catch (Exception e) {
			m_nFailureCount++;
			LOGGER.error("RabbitJMSConnection : send() Failed, Ex : " + e.getMessage() + ", Queue : " + QUEUE_NAME
					+ ", Query : " + strMessage, e);
			if (!isRetry && lastProps != null && !isConnectionOpen()) {
				LOGGER.info("Connection lost. Attempting to reconnect and retry send (delay)...");
				if (reconnect(lastProps, lastQConnector)) {
					mProducerList.clear();
					return send(QUEUE_NAME, strMessage, nPriority, nDelay, true);
				}
			}
		}
		return false;
	}

	/**
	 * Closes channels or the whole connection.
	 * <p>
	 * With a queue name, closes the consumer and producer channels for that
	 * queue and for its {@code <queue>_DELAY} holding queue (matched
	 * case-insensitively) and leaves the connection open; a later send or
	 * receive on that queue opens a fresh channel. With {@code null}, closes
	 * the connection and all its channels.
	 *
	 * @param qSource queue whose channels to close, or {@code null} to close the
	 *                connection
	 */
	public void deinit(String qSource) {
		if (qSource == null) {
			mConsumerList.clear();
			mProducerList.clear();
			if (connection != null) {
				try {
					connection.close();
					LOGGER.info("RabbitJMSConnection : deinit() connection closed.");
				} catch (Exception ex) {
					LOGGER.error("RabbitJMSConnection : deinit() Failed, Ex : " + ex.getMessage(), ex);
				}
			}
			return;
		}
		closeChannels(mConsumerList, qSource, "Consumer");
		closeChannels(mProducerList, qSource, "Producer");
	}

	private void closeChannels(Hashtable<String, Channel> cache, String qSource, String kind) {
		String delayQueue = qSource + "_DELAY";
		Iterator<Map.Entry<String, Channel>> it = cache.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Channel> entry = it.next();
			if (qSource.equalsIgnoreCase(entry.getKey()) || delayQueue.equalsIgnoreCase(entry.getKey())) {
				it.remove();
				try {
					if (entry.getValue().isOpen()) {
						entry.getValue().close();
					}
					LOGGER.info("RabbitJMSConnection : deinit() " + kind + " Queue : " + entry.getKey());
				} catch (Exception ex) {
					LOGGER.error("RabbitJMSConnection : deinit() " + kind + " Queue : " + entry.getKey()
							+ " Failed, Ex : " + ex.getMessage(), ex);
				}
			}
		}
	}

}
