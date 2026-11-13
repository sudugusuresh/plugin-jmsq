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

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.command.ActiveMQQueue;

import com.plugin.jmsq.util.PerfParams;
import com.plugin.jmsq.util.PropertyBean;
import com.plugin.jmsq.util.QUtil;


/**
 * ActiveMQ Classic connector using the JMS 1.1 API ({@code javax.jms}).
 * <p>
 * Each instance owns one JMS connection and one session to a single queue,
 * and acts as a producer, a consumer, or both. Messages are always
 * {@link TextMessage}s. Create instances with one of the {@code getInstance}
 * methods and release them with {@link #deinit()}.
 *
 * <h2>Connector string</h2>
 * {@code host=<host>&senderport=<port>&receiverport=<port>&user=<user>&password=<pw>}.
 * Each key is optional. A missing {@code host} or port falls back to the
 * {@code amq.host} or {@code amq.port} property; a missing {@code user} or
 * {@code password} defaults to {@code admin}. The broker URL is always built
 * as {@code tcp://host:senderport}, including for consumers.
 *
 * <h2>Properties read</h2>
 * <ul>
 * <li>{@code amq.host}, {@code amq.port}: broker fallbacks for the connector string.</li>
 * <li>{@code amq.producer-window}: producer window size in bytes. <b>Required</b>;
 * initialisation fails if it is missing.</li>
 * <li>{@code amq.transacted} (default {@code false}): use a transacted session.
 * Sends and receives are then committed every
 * {@code amq.commit.records.count} messages (default 2000) or every
 * {@code amq.commit.interval} seconds (default 2), whichever comes first.
 * The check happens on the next send or receive call, not on a timer.</li>
 * <li>{@code amq.persist.enable} (default {@code false}): send persistent messages.</li>
 * <li>{@code amq.use.asyncsend} (default {@code true}): asynchronous sends.</li>
 * <li>{@code amq.prefetch.size} (default 1000): consumer queue prefetch.</li>
 * <li>{@code amq.compress.msg.enable} (default {@code false}): compress message bodies.</li>
 * <li>{@code amq.tcp-buffer} (default 20971520): socket buffer size in bytes.</li>
 * <li>{@code amq.statistics.timeout.ms} (default 2000): how long
 * {@link #getMessageCount()} and {@link #getConsumerCount()} wait for the
 * broker's statistics plugin.</li>
 * <li>{@code amq.durable}, {@code amq.block-ack}: stored in {@link PerfParams}
 * but have no effect on the connection.</li>
 * <li>{@code amq.connector.url}: stored and returned by
 * {@link #getQConnectorURL()} when no connector string is given.</li>
 * <li>{@code qmanager.db.driver}, {@code qmanager.db.url}: JDBC settings for
 * {@link #logQConsumerCounter()}.</li>
 * </ul>
 *
 * <h2>Threading</h2>
 * Instances are not thread-safe: a JMS session must be used by one thread at a
 * time. Errors are printed to standard output and counted in
 * {@link #m_nFailureCount}; methods report failure through their return value
 * rather than by throwing.
 */
public class AMQJMSConnection {

	private static Object objSync = new Object();
	/**
	 * Number of consecutive failed operations. Reset to 0 after each
	 * successful send or receive; callers can use it to decide when to reconnect.
	 */
	public int m_nFailureCount = 0;

//    private ClientSessionFactory factory;
	private Session session = null;
	private TextMessage message = null;
	boolean m_bProducer = false;
	boolean m_bConsumer = false;

	/** Consumer for the queue, or {@code null} if this is not a consumer or uses selector-based receives. */
	public MessageConsumer myConsumer = null;
	/** Producer for the queue, or {@code null} if this is not a producer. */
	public MessageProducer myProducer = null;
	String strQueue = "";
	boolean bConnect = false;

	int nSeconds = 2;
	int nCommitRecords = 2000;
	Hashtable<String, Long> hTable = null;
	String strDriver = "";
	String strURL = "";

	boolean autoCommitEnable = true;
	PerfParams perfParams = new PerfParams();
	String m_objConnectorURL = "";
	javax.jms.Connection m_Connection = null;
	Properties props;

	String amqIps;
	String amqPort;
	String amqServers;

	/**
	 * Creates a connector for a queue. The address is the same as the queue name.
	 *
	 * @param props      settings; see the class description
	 * @param qConnector connector string; may be {@code null}
	 * @param strQname   queue name
	 * @param isProducer {@code true} to create a producer
	 * @param isConsumer {@code true} to create a consumer
	 * @return a connected instance, or {@code null} if initialisation failed
	 */
	public static AMQJMSConnection getInstance(Properties props, String qConnector, String strQname, boolean isProducer,
			boolean isConsumer) {
		synchronized (objSync) {
			try {
				AMQJMSConnection obj = new AMQJMSConnection();
				if (obj != null) {
					if (obj.init(props, qConnector, strQname, strQname, isProducer, isConsumer)) {
						// This is added later as getDefaultInstance Is null every time
						return obj;
					}
				}
			} catch (Exception e) {
				System.out.println("HQInitializer : could not initialize HQ Exep: " + e.getMessage());
			}
			return null;
		} // end of sync object
	}

	/**
	 * Creates a connector for a queue, optionally for selector-based consuming.
	 * The address is the same as the queue name.
	 *
	 * @param props                settings; see the class description
	 * @param qConnector           connector string; may be {@code null}
	 * @param strQname             queue name
	 * @param isProducer           {@code true} to create a producer
	 * @param isConsumer           {@code true} to allow consuming
	 * @param isConditionAvailable {@code true} if messages will be read with a
	 *                             selector ({@link #receive(String, int)} or
	 *                             {@link #receive(String, int, int)}); no
	 *                             standing consumer is created in that case
	 * @return a connected instance, or {@code null} if initialisation failed
	 */
	public static AMQJMSConnection getInstance(Properties props, String qConnector, String strQname, boolean isProducer,
			boolean isConsumer, boolean isConditionAvailable) {
		synchronized (objSync) {
			try {
				AMQJMSConnection obj = new AMQJMSConnection();
				if (obj != null) {
					if (obj.init(props, qConnector, strQname, strQname, isProducer, isConsumer, isConditionAvailable)) {
						// This is added later as getDefaultInstance Is null every time
						return obj;
					}
				}
			} catch (Exception e) {
				System.out.println("HQInitializer : could not initialize HQ Exep: " + e.getMessage());
			}
			return null;
		} // end of sync object
	}
	/**
	 * Creates a connector for a queue with a separate address. The address is
	 * the destination used by selector-based receives.
	 *
	 * @param props      settings; see the class description
	 * @param qConnector connector string; may be {@code null}
	 * @param address    destination for selector-based receives
	 * @param strQname   queue name
	 * @param isProducer {@code true} to create a producer
	 * @param isConsumer {@code true} to create a consumer
	 * @return a connected instance, or {@code null} if initialisation failed
	 */
	public static AMQJMSConnection getInstance(Properties props, String qConnector, String address, String strQname,
			boolean isProducer, boolean isConsumer) {
		synchronized (objSync) {
			try {
				AMQJMSConnection obj = new AMQJMSConnection();
				if (obj != null) {
					if (obj.init(props, qConnector, address, strQname, isProducer, isConsumer)) {
						// This is added later as getDefaultInstance Is null every time
						return obj;
					}
				}
			} catch (Exception e) {
				System.out.println("HQInitializer : could not initialize HQ Exep: " + e.getMessage());
			}
			return null;
		} // end of sync object
	}
	/**
	 * Creates a connector for a queue with a separate address, optionally for
	 * selector-based consuming.
	 *
	 * @param props                settings; see the class description
	 * @param qConnector           connector string; may be {@code null}
	 * @param address              destination for selector-based receives
	 * @param strQname             queue name
	 * @param isProducer           {@code true} to create a producer
	 * @param isConsumer           {@code true} to allow consuming
	 * @param isConditionAvailable {@code true} if messages will be read with a
	 *                             selector; no standing consumer is created in
	 *                             that case
	 * @return a connected instance, or {@code null} if initialisation failed
	 */
	public static AMQJMSConnection getInstance(Properties props, String qConnector, String address, String strQname,
			boolean isProducer, boolean isConsumer, boolean isConditionAvailable) {
		synchronized (objSync) {
			try {
				AMQJMSConnection obj = new AMQJMSConnection();
				if (obj != null) {
					if (obj.init(props, qConnector, address, strQname, isProducer, isConsumer, isConditionAvailable)) {
						// This is added later as getDefaultInstance Is null every time
						return obj;
					}
				}
			} catch (Exception e) {
				System.out.println("HQInitializer : could not initialize HQ Exep: " + e.getMessage());
			}
			return null;
		} // end of sync object
	}

	private boolean init(Properties props, String qConnector, String address, String strQname, boolean isProducer,
			boolean isConsumer) {
		return init(props, qConnector, address, strQname, isProducer, isConsumer, false);
	}

	String user = "admin";
	String password = "admin";

	private boolean init(Properties props, String qConnector, String strAddress, String strQname, boolean isProducer,
			boolean isConsumer, boolean isConditionAvailable) {
		try {
			if (session == null) {
				System.out.println("============");
				System.out.println("qConnector:" + qConnector);
				System.out.println("strAddress:" + strAddress);
				System.out.println("strQname:" + strQname);
				System.out.println("isProducer:" + isProducer);
				System.out.println("isConsumer:" + isConsumer);
				System.out.println("isConditionAvailable:" + isConditionAvailable);
				hTable = new Hashtable<String, Long>();
				strDriver = props.getProperty("qmanager.db.driver");
				strURL = props.getProperty("qmanager.db.url");
				Hashtable qParams = QUtil.getExtraparamslist(qConnector != null ? qConnector : "");
				strQueue = strQname;
				nSeconds = Integer.parseInt(props.getProperty("amq.commit.interval", "" + nSeconds));
				nCommitRecords = Integer.parseInt(props.getProperty("amq.commit.records.count", "" + nCommitRecords));
				statsTimeoutMs = QUtil.longVal(props.getProperty("amq.statistics.timeout.ms"), statsTimeoutMs);
				boolean durable = Boolean.valueOf(props.getProperty("amq.durable"));
				boolean transacted = Boolean.valueOf(props.getProperty("amq.transacted", "false"));
				autoCommitEnable = !transacted;
				String host = qParams.get("host") != null ? (String) qParams.get("host")
						: props.getProperty("amq.host");
				int senderPort = Integer.valueOf(qParams.get("senderport") != null ? (String) qParams.get("senderport")
						: props.getProperty("amq.port"));
				int receieverPort = Integer
						.valueOf(qParams.get("receiverport") != null ? (String) qParams.get("receiverport")
								: props.getProperty("amq.port"));
				if (qConnector != null) {
					m_objConnectorURL = qConnector;
				} else {
					if (props.getProperty("amq.connector.url") != null) {
						m_objConnectorURL = props.getProperty("amq.connector.url");
					} else {
						String mURL = "url=host=$(host)&senderport=$(sendport)&receiverport=$(recvport)";
						mURL = mURL.replaceAll("\\$\\(host\\)", host);
						mURL = mURL.replaceAll("\\$\\(sendport\\)", "5445");
						mURL = mURL.replaceAll("\\$\\(recvport\\)", "5446");
						m_objConnectorURL = mURL;
					}
				}
				user = qParams.get("user") != null ? (String) qParams.get("user") : user;
				password = qParams.get("password") != null ? (String) qParams.get("password") : password;
				//Log.info("##########AMQ isProducer" + isProducer + " Connected with :" + host + ":" + senderPort);
				//Log.info("##########AMQ isConsumer" + isConsumer + " Connected with :" + host + ":" + receieverPort);

				int tcpBufferSize = Integer.valueOf(props.getProperty("amq.tcp-buffer", "20971520"));
				int producerWindowSize = Integer.valueOf(props.getProperty("amq.producer-window"));
				boolean blockOnACK = Boolean.valueOf(props.getProperty("amq.block-ack", "false"));
				boolean blockOnPersistent = Boolean.valueOf(props.getProperty("amq.persist.enable", "false"));
				boolean useAsyncSend = Boolean.valueOf(props.getProperty("amq.use.asyncsend", "true"));
				int preFetchSize = Integer.valueOf(props.getProperty("amq.prefetch.size", "1000"));
				boolean compressEnable = Boolean.valueOf(props.getProperty("amq.compress.msg.enable", "false"));

				perfParams.setDurable(durable);
				perfParams.setSessionTransacted(transacted);
				perfParams.setQueueName(strQname);
				perfParams.setAddress(strAddress);
				perfParams.setHost(host);
				if (isProducer) {
					perfParams.setPort(senderPort);
				} else {
					perfParams.setPort(receieverPort);
				}
				perfParams.setTcpBufferSize(tcpBufferSize);
				perfParams.setProducerWindow(producerWindowSize);
				perfParams.setBlockOnACK(blockOnACK);
				perfParams.setBlockOnPersistent(blockOnPersistent);
				perfParams.setUseAsyncSend(useAsyncSend);
				perfParams.setPrefetchsize(preFetchSize);
				perfParams.setCompressEnable(compressEnable);

				String connectorURL = "tcp://" + host + ":" + senderPort
						+ (perfParams.getTcpBufferSize() > 0 ? ("?socketBufferSize=" + perfParams.getTcpBufferSize())
								: "");
				ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectorURL);
				if (isProducer) {
					factory.setProducerWindowSize(perfParams.getProducerWindow());
					factory.setUseAsyncSend(perfParams.isUseAsyncSend());
					if (perfParams.isCompressEnable()) {
						factory.setUseCompression(perfParams.isCompressEnable());
					}

				} else {
					ActiveMQPrefetchPolicy policy = new ActiveMQPrefetchPolicy();
					policy.setQueuePrefetch(perfParams.getPrefetchsize());
					factory.setPrefetchPolicy(policy);
				}
				m_Connection = factory.createConnection(user, password);
				m_Connection.start();
//        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
				session = m_Connection.createSession(perfParams.isSessionTransacted(), Session.AUTO_ACKNOWLEDGE);
				m_bProducer = isProducer;
				m_bConsumer = isConsumer;
			}
			if (isProducer) {
				Destination dest = new ActiveMQQueue(perfParams.getQueueName());
				myProducer = session.createProducer(dest);
				myProducer.setDeliveryMode(
						perfParams.isBlockOnPersistent() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
//                myProducer = session.createProducer(new QueueImpl("queue://" + perfParams.getAddress()));
				message = session.createTextMessage();
			}
			if (isConsumer && !isConditionAvailable) {
				Destination dest = new ActiveMQQueue(perfParams.getQueueName());
				myConsumer = session.createConsumer(dest);
//                myConsumer = session.createConsumer(new QueueImpl("queue://" + perfParams.getQueueName()));
//                session.start();
			}
			bConnect = true;
			//Log.info((isProducer ? "HQProducer : " : "HQConsumer") + " started for Q : " + strQname);
//            int nCCounter = QUtil.intVal(props.getProperty("consumer-time-counter"), 5);
//            Qcounter =  new QConsumerCounter(this, nCCounter);
//            Qcounter.start();
			return true;
		} catch (Exception e) {
			bConnect = false;
			System.out.println("HQInitializer : init() failed : strQueue " + strQueue + ", Ex: " + e.getMessage());
		}
		return false;
	}

	/**
	 * Returns the connector string this instance was created with. If none was
	 * given, returns {@code amq.connector.url} or a generated
	 * {@code url=host=..&senderport=5445&receiverport=5446} string.
	 *
	 * @return the connector string
	 */
	public String getQConnectorURL() {
		return m_objConnectorURL;
	}

	/**
	 * Reports whether initialisation succeeded. Set to {@code false} again
	 * when a selector-based receive fails.
	 *
	 * @return {@code true} if the connector is believed to be connected
	 */
	public boolean isConnected() {
		return bConnect;
	}

	int icount = 0;
	int iSendcount = 0;
	long lstart = System.currentTimeMillis();

	/**
	 * Receives the next message from the queue, waiting up to 50 ms.
	 * <p>
	 * Requires an instance created with {@code isConsumer = true} and
	 * {@code isConditionAvailable = false}. Records the receive time for
	 * {@link #logQConsumerCounter()} and, in a transacted session, commits when
	 * the batch size or interval is reached.
	 *
	 * @return the message, or {@code null} if none arrived or the receive failed
	 */
	public TextMessage receive() {
		try {
			if (!autoCommitEnable) {
				long diff = System.currentTimeMillis() - lstart;
				if (icount > 0 && (icount % nCommitRecords == 0 || diff > nSeconds * 1000)) {
					session.commit();
					icount = 0;
					lstart = System.currentTimeMillis();
				}
			}
			Object obj = myConsumer.receive(50);
			m_nFailureCount = 0;
			if (obj != null) {
				TextMessage message = (TextMessage) obj;
				hTable.put(strQueue, System.currentTimeMillis());
				icount++;
				message.acknowledge();
				return message;
			}
		} catch (Exception e) {
			m_nFailureCount++;
			System.out.println("HQInitializer : receive() Failed, Ex : " + e.getMessage());
		}
		return null;
	}

	/**
	 * Receives one message that matches a JMS selector and a priority, waiting
	 * up to 50 ms.
	 * <p>
	 * A temporary consumer is created on the address with the selector
	 * {@code condition AND JMSPriority=nPriority}, and closed before returning.
	 *
	 * @param condition JMS message selector, for example {@code "type='SMS'"};
	 *                  must not be empty
	 * @param nPriority JMS priority the message must have (0 to 9)
	 * @return the message text, or {@code null} if none matched, this instance
	 *         is not a consumer, or the receive failed
	 */
	public String receive(String condition, int nPriority) {
		try {
			if (m_bConsumer) {
				condition = condition + " AND JMSPriority=" + nPriority;
				Destination dest = new ActiveMQQueue(perfParams.getAddress());
				myConsumer = session.createConsumer(dest, condition, false);
//                session.start();
			} else {
				return null;
			}
			return receivemsg();
		} catch (Exception e) {
			bConnect = false;
			m_nFailureCount++;
			System.out.println("HQInitializer : receive() Failed, Ex : " + e.getMessage());
		} finally {
			if (m_bConsumer) {
				try {
					myConsumer.close();
					myConsumer = null;
				} catch (Exception e) {
					System.out.println("HQInitializer : receive() consumer close Failed, Ex : " + e.getMessage());
				}
			}
		}
		return null;
	}

	/**
	 * Receives up to {@code maxRows} messages that match a JMS selector and a
	 * priority. Stops at the first 50 ms wait in which no message arrives.
	 * <p>
	 * A temporary consumer is created on the address with the selector
	 * {@code condition AND JMSPriority=nPriority}, and closed before returning.
	 *
	 * @param condition JMS message selector; must not be empty
	 * @param nPriority JMS priority the messages must have (0 to 9)
	 * @param maxRows   maximum number of messages; {@code -1} means 1
	 * @return the message texts, or {@code null} if none matched, this instance
	 *         is not a consumer, or the receive failed
	 */
	public String[] receive(String condition, int nPriority, int maxRows) {
		try {
			if (m_bConsumer) {
				condition = condition + " AND JMSPriority=" + nPriority;
				Destination dest = new ActiveMQQueue(perfParams.getAddress());
				myConsumer = session.createConsumer(dest, condition, false);
//                session.start();
			} else {
				return null;
			}
			ArrayList<String> list = null;
			if (maxRows == -1) {
				maxRows = 1;
			}
			for (int icount = 0; icount < maxRows; icount++) {
				String strXML = receivemsg();
				if (strXML != null) {
					if (list == null) {
						list = new ArrayList<String>();
					}
					list.add(strXML);
					continue;
				}
				break;
			}
			return (list != null && list.size() > 0) ? list.toArray(new String[1]) : null;
		} catch (Exception e) {
			bConnect = false;
			m_nFailureCount++;
			System.out.println("HQInitializer : receive() Failed, Ex : " + e.getMessage());
		} finally {
			if (m_bConsumer) {
				try {
					myConsumer.close();
					myConsumer = null;
				} catch (Exception e) {
				}
			}
		}
		return null;
	}

	private String receivemsg() throws Exception {
		if (!autoCommitEnable) {
			long diff = System.currentTimeMillis() - lstart;
			if (icount > 0 && (icount % nCommitRecords == 0 || diff > nSeconds * 1000)) {
				session.commit();
				icount = 0;
				lstart = System.currentTimeMillis();
			}
		}
		Object obj = myConsumer.receive(50);
		m_nFailureCount = 0;
		if (obj != null) {
			hTable.put(strQueue, System.currentTimeMillis());
			TextMessage message = (TextMessage) obj;
			icount++;
			message.acknowledge();
			return message.getText();
		}
//                break;
		return null;
	}

	/**
	 * Sends a text message to the queue. The message never expires.
	 * <p>
	 * Delivery is persistent if {@code amq.persist.enable} is {@code true}. In
	 * a transacted session, commits when the batch size or interval is reached.
	 *
	 * @param strQuery  message text
	 * @param nPriority JMS priority (0 to 9; 4 is the JMS default)
	 * @return {@code true} if the message was sent
	 */
	public boolean send(String strQuery, int nPriority) {
		try {
			if (!autoCommitEnable) {
				long diff = System.currentTimeMillis() - lstart;
				if (iSendcount > 0 && (iSendcount % nCommitRecords == 0 || diff > nSeconds * 1000)) {
					session.commit();
					iSendcount = 0;
					lstart = System.currentTimeMillis();
				}
			}
//            this.message.setStringProperty("content-length", "" + strQuery.getBytes().length);
			message.setText(strQuery);
			myProducer.send(message,
					perfParams.isBlockOnPersistent() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT, nPriority,
					0);
			m_nFailureCount = 0;
			iSendcount++;
			return true;
		} catch (Exception e) {
			m_nFailureCount++;
			System.out.println("HQInitializer : send() Failed, Ex : " + e.getMessage() + ", strQueue : " + strQueue
					+ ", Query : " + strQuery);
		}
		return false;
	}

	/** ActiveMQ message property holding a delivery delay in milliseconds. */
	private static final String SCHEDULED_DELAY = "AMQ_SCHEDULED_DELAY";

	/** Parses a whole number, returning 0 for null, blank, negative or invalid input. */
	private static long parsePositiveLong(String value) {
		if (value == null) {
			return 0;
		}
		try {
			return Math.max(0, Long.parseLong(value.trim()));
		} catch (NumberFormatException e) {
			return 0;
		}
	}

	/**
	 * Sends a text message with JMS properties, an expiry or a delivery delay.
	 * <p>
	 * Each {@link PropertyBean} is applied according to its type: types 1 to
	 * 10 are set as message properties; {@code EXPIRY_*} types set the
	 * message's time to live (it expires that long after sending); and
	 * {@code SCHEDULE_*} types set {@code AMQ_SCHEDULED_DELAY}, so the broker
	 * holds the message that long before delivering it. Delayed delivery only
	 * works if the broker has {@code schedulerSupport="true"}; otherwise the
	 * message is delivered at once. Properties from the previous message are
	 * cleared first. Missing, zero, negative or invalid values for the expiry
	 * and schedule types are ignored; invalid values for the other types make
	 * the send fail.
	 *
	 * @param strQuery  message text
	 * @param nPriority JMS priority (0 to 9)
	 * @param lstProps  properties and send options; may be {@code null}
	 * @return {@code true} if the message was sent
	 */
	public boolean send(String strQuery, int nPriority, List<PropertyBean> lstProps) {
		try {
			if (!autoCommitEnable) {
				long diff = System.currentTimeMillis() - lstart;
				if (iSendcount > 0 && (iSendcount % nCommitRecords == 0 || diff > nSeconds * 1000)) {
					session.commit();
					iSendcount = 0;
					lstart = System.currentTimeMillis();
				}
			}
			long lTimeToLive = 0; // 0 = never expires
			this.message.clearBody();
			this.message.clearProperties();
			for (int iCount = 0; lstProps != null && iCount < lstProps.size(); iCount++) {
				PropertyBean propBean = lstProps.get(iCount);
				if (propBean.getPropType() == PropertyBean.BOOLEAN_PROPERTY) {
					this.message.setBooleanProperty(propBean.getPropKey(), Boolean.valueOf(propBean.getPropValue()));
				} else if (propBean.getPropType() == PropertyBean.BYTE_PROPERTY) {
					this.message.setByteProperty(propBean.getPropKey(), Byte.parseByte(propBean.getPropValue()));
				} else if (propBean.getPropType() == PropertyBean.DOUBLE_PROPERTY) {
					this.message.setDoubleProperty(propBean.getPropKey(), Double.valueOf(propBean.getPropValue()));
				} else if (propBean.getPropType() == PropertyBean.FLOAT_PROPERTY) {
					this.message.setFloatProperty(propBean.getPropKey(), Float.valueOf(propBean.getPropValue()));
				} else if (propBean.getPropType() == PropertyBean.INT_PROPERTY) {
					this.message.setIntProperty(propBean.getPropKey(), Integer.valueOf(propBean.getPropValue()));
				} else if (propBean.getPropType() == PropertyBean.LONG_PROPERTY) {
					this.message.setLongProperty(propBean.getPropKey(), Long.valueOf(propBean.getPropValue()));
				} else if (propBean.getPropType() == PropertyBean.OBJECT_PROPERTY) {
					this.message.setObjectProperty(propBean.getPropKey(), propBean.getPropValue());
				} else if (propBean.getPropType() == PropertyBean.SHORT_PROPERTY) {
					this.message.setShortProperty(propBean.getPropKey(), Short.valueOf(propBean.getPropValue()));
				} else if (propBean.getPropType() == PropertyBean.STRING_PROPERTY) {
					this.message.setStringProperty(propBean.getPropKey(), propBean.getPropValue());
				} else if (propBean.getPropType() == PropertyBean.EXPIRY_PROPERTY) {
					// JMS time-to-live is a duration in ms, not a clock time.
					long minutes = parsePositiveLong(propBean.getPropValue());
					if (minutes > 0) {
						lTimeToLive = minutes * 60 * 1000;
					}
				} else if (propBean.getPropType() == PropertyBean.EXPIRY_SECS_PROPERTY) {
					long seconds = parsePositiveLong(propBean.getPropValue());
					if (seconds > 0) {
						lTimeToLive = seconds * 1000;
					}
				} else if (propBean.getPropType() == PropertyBean.SCHEDULE_MINS_PROPERTY) {
					// AMQ_SCHEDULED_DELAY is a delay in ms from the time the broker receives the message.
					long minutes = parsePositiveLong(propBean.getPropValue());
					if (minutes > 0) {
						message.setLongProperty(SCHEDULED_DELAY, minutes * 60 * 1000);
					}
				} else if (propBean.getPropType() == PropertyBean.SCHEDULE_SECS_PROPERTY) {
					long seconds = parsePositiveLong(propBean.getPropValue());
					if (seconds > 0) {
						message.setLongProperty(SCHEDULED_DELAY, seconds * 1000);
					}
				}
			}
//            this.message.setStringProperty("content-length", "" + strQuery.getBytes().length);
//            myProducer.send(message);
			message.setText(strQuery);
			myProducer.send(message,
					perfParams.isBlockOnPersistent() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT, nPriority,
					lTimeToLive);
			m_nFailureCount = 0;
			iSendcount++;
			return true;
		} catch (Exception e) {
			m_nFailureCount++;
			System.out.println("HQInitializer : send() Failed, Ex : " + e.getMessage() + ", strQueue : " + strQueue
					+ ", Query : " + strQuery);
		}
		return false;
	}

	/**
	 * Closes the producer and consumer, commits any open transaction, and
	 * closes the session and connection.
	 *
	 * @return {@code true} if everything closed without errors
	 */
	public boolean deinit() {
		try {
			if (m_bProducer) {
				if (myProducer != null) {
					myProducer.close();
				}
			}
			if (m_bConsumer) {
				if (myConsumer != null) {
					myConsumer.close();
				}
			}
			/*
			 * if(Qcounter!=null){ Qcounter.stopHelper(); }
			 */
			if (!autoCommitEnable) {
				session.commit();
			}
			session.close();
			m_Connection.close();
			return true;
		} catch (Exception e) {
			System.out.println("HQ deinit() failed, Exception : " + e.getMessage());
		}
		return false;
	}

	private boolean getSession(Properties props, String qConnector, String strAddress, String strQname) {
		try {
			if (session == null) {
				hTable = new Hashtable<String, Long>();
				strDriver = props.getProperty("qmanager.db.driver");
				strURL = props.getProperty("qmanager.db.url");
				Hashtable qParams = QUtil.getExtraparamslist(qConnector != null ? qConnector : "");
				strQueue = strQname;
				nSeconds = Integer.parseInt(props.getProperty("amq.commit.interval", "" + nSeconds));
				nCommitRecords = Integer.parseInt(props.getProperty("amq.commit.records.count", "" + nCommitRecords));
				statsTimeoutMs = QUtil.longVal(props.getProperty("amq.statistics.timeout.ms"), statsTimeoutMs);
				boolean transacted = Boolean.valueOf(props.getProperty("amq.transacted", "false"));
				autoCommitEnable = !transacted;
				String host = qParams.get("host") != null ? (String) qParams.get("host")
						: props.getProperty("amq.host");
				int senderPort = Integer.valueOf(qParams.get("senderport") != null ? (String) qParams.get("senderport")
						: props.getProperty("amq.port"));
				int receieverPort = Integer
						.valueOf(qParams.get("receiverport") != null ? (String) qParams.get("receiverport")
								: props.getProperty("amq.port"));
				if (qConnector != null) {
					m_objConnectorURL = qConnector;
				} else {
					if (props.getProperty("amq.connector.url") != null) {
						m_objConnectorURL = props.getProperty("amq.connector.url");
					} else {
						String mURL = "url=host=$(host)&senderport=$(sendport)&receiverport=$(recvport)";
						mURL = mURL.replaceAll("\\$\\(host\\)", host);
						mURL = mURL.replaceAll("\\$\\(sendport\\)", "5445");
						mURL = mURL.replaceAll("\\$\\(recvport\\)", "5446");
						m_objConnectorURL = mURL;
					}
				}
				user = qParams.get("user") != null ? (String) qParams.get("user") : user;
				password = qParams.get("password") != null ? (String) qParams.get("password") : password;
				//Log.info("##########AMQ getSession Connected with :" + host + ":" + senderPort);
				//Log.info("##########AMQ getSession Connected with :" + host + ":" + receieverPort);

				int tcpBufferSize = Integer.valueOf(props.getProperty("amq.tcp-buffer", "20971520"));
				int producerWindowSize = Integer.valueOf(props.getProperty("amq.producer-window"));
				boolean blockOnACK = Boolean.valueOf(props.getProperty("amq.block-ack", "false"));
				boolean persistEnable = Boolean.valueOf(props.getProperty("amq.persist.enable", "false"));
				boolean useAsyncSend = Boolean.valueOf(props.getProperty("amq.use.asyncsend", "true"));
				int preFetchSize = Integer.valueOf(props.getProperty("amq.prefetch.size", "1000"));
				boolean compressEnable = Boolean.valueOf(props.getProperty("amq.compress.msg.enable", "false"));

				perfParams.setSessionTransacted(transacted);
				perfParams.setQueueName(strQname);
				perfParams.setAddress(strAddress);
				perfParams.setHost(host);
				if (true) {
					perfParams.setPort(senderPort);
				} else {
					perfParams.setPort(receieverPort);
				}
				perfParams.setTcpBufferSize(tcpBufferSize);
				perfParams.setProducerWindow(producerWindowSize);
				perfParams.setBlockOnACK(blockOnACK);
				perfParams.setBlockOnPersistent(persistEnable);
				perfParams.setUseAsyncSend(useAsyncSend);
				perfParams.setPrefetchsize(preFetchSize);
				perfParams.setCompressEnable(compressEnable);

				String connectorURL = "tcp://" + host + ":" + senderPort
						+ (perfParams.getTcpBufferSize() > 0 ? ("?socketBufferSize=" + perfParams.getTcpBufferSize())
								: "");
				//Log.info("Connector URL : " + connectorURL);
				ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectorURL);
				factory.setProducerWindowSize(perfParams.getProducerWindow());
				factory.setUseAsyncSend(perfParams.isUseAsyncSend());
				if (perfParams.isCompressEnable()) {
					factory.setUseCompression(perfParams.isCompressEnable());
				}

				ActiveMQPrefetchPolicy policy = new ActiveMQPrefetchPolicy();
				policy.setQueuePrefetch(perfParams.getPrefetchsize());
				factory.setPrefetchPolicy(policy);
//                ConnectionFactoryImpl factory = new ConnectionFactoryImpl(host, senderPort, user, password);

				m_Connection = factory.createConnection(user, password);
				m_Connection.start();
//        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
				session = m_Connection.createSession(perfParams.isSessionTransacted(), Session.AUTO_ACKNOWLEDGE);
				//Log.info("########## Session created, Connected with :" + host + ":" + senderPort + " | " + host + ":" + receieverPort);
			}
			return true;
		} catch (Exception e) {
			bConnect = false;
			System.out.println("HQInitializer : init() failed : strQueue " + strQueue + ", Ex: " + e.getMessage());
		}
		return false;
	}
	/**
	 * Deletes a queue on the broker, along with any messages in it.
	 * <p>
	 * Opens a separate, unauthenticated connection to the host and port this
	 * instance was initialised with.
	 *
	 * @param props      unused
	 * @param qConnector unused
	 * @param queueName  queue to delete
	 * @throws Exception if the connection or the delete fails
	 */
	public void deleteQueue(Properties props, String qConnector, String queueName) throws Exception {
		// better stop session i think
		ActiveMQConnection conn = null;
		try {
			conn = (ActiveMQConnection) new ActiveMQConnectionFactory(
					"tcp://" + perfParams.getHost() + ":" + perfParams.getPort()).createConnection();
			conn.destroyDestination(new ActiveMQQueue(queueName));
			
		} catch (Exception ex) {
			System.out.println("HQ deleteQueue() failed, Exception : " + ex.getMessage());
			throw ex;
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (JMSException e) {
					System.out.println("Error closing connection"+ e);
				}
			}
		}
	}
	/**
	 * Opens a session if needed and creates a producer for {@code strQname}.
	 * <p>
	 * ActiveMQ creates queues automatically on first use, so this does not
	 * create anything on the broker by itself. The temporary, durable and
	 * filter arguments are kept for compatibility and are ignored.
	 *
	 * @param props       settings; see the class description
	 * @param qConnector  connector string; may be {@code null}
	 * @param strAddress  address to record for selector-based receives
	 * @param strQname    queue name
	 * @param isTempQueue ignored
	 * @param isDurable   ignored
	 * @param isFilter    ignored
	 * @param strFilter   ignored
	 * @throws Exception if the producer cannot be created
	 */
	public void createQueue(Properties props, String qConnector, String strAddress, String strQname,
			boolean isTempQueue, boolean isDurable, boolean isFilter, String strFilter) throws Exception {
		getSession(props, qConnector, strAddress, strQname);
		if (isFilter) {// Creating non-temporary queue with filter
			Destination dest = new ActiveMQQueue(strQname);
			myProducer = session.createProducer(dest);
		} else {// Creating non-temporary queue without filter
//                session.createQueue("queue://" + strAddress);
			Destination dest = new ActiveMQQueue(strQname);
			myProducer = session.createProducer(dest);
		}
	}

	/**
	 * Opens a session if needed and creates a producer for {@code qName}.
	 * The filter is ignored; see
	 * {@link #createQueue(Properties, String, String, String, boolean, boolean, boolean, String)}.
	 *
	 * @param props      settings; see the class description
	 * @param qConnector connector string; may be {@code null}
	 * @param address    address to record for selector-based receives
	 * @param qName      queue name
	 * @param qFilter    ignored
	 * @return {@code true}
	 * @throws Exception if the producer cannot be created
	 */
	public boolean createFilterQueue(Properties props, String qConnector, String address, String qName, String qFilter)
			throws Exception {
		getSession(props, qConnector, address, qName);
		//Log.info(qConnector + " | " + address + " | " + qName + " | " + qFilter + " | " + perfParams.isDurable());
//        session.createQueue(address, qName, qFilter, perfParams.isDurable());
		Destination dest = new ActiveMQQueue(perfParams.getQueueName());
		myProducer = session.createProducer(dest);
		return true;
	}

	/**
	 * Opens a session if needed and creates a producer for {@code qName}. See
	 * {@link #createQueue(Properties, String, String, String, boolean, boolean, boolean, String)}.
	 *
	 * @param props      settings; see the class description
	 * @param qConnector connector string; may be {@code null}
	 * @param address    address to record for selector-based receives
	 * @param qName      queue name
	 * @return {@code true}
	 * @throws Exception if the producer cannot be created
	 */
	public boolean createQueue(Properties props, String qConnector, String address, String qName) throws Exception {
		getSession(props, qConnector, address, qName);
		//Log.info(qConnector + " | " + address + " | " + qName + " | " + perfParams.isDurable());
		Destination dest = new ActiveMQQueue(perfParams.getQueueName());
		myProducer = session.createProducer(dest);
		return true;
	}

	private Connection getConnection() {
		try {
			Class.forName(strDriver);
			return DriverManager.getConnection(strURL);
		} catch (Exception sqlEx) {
			System.out.println("Get connection to smsgw_config database failed. reason : " + sqlEx.toString());
		}
		return null;
	}

	/**
	 * Writes the time each queue was last consumed from to the database.
	 * <p>
	 * For every queue this instance has received from, runs
	 * {@code UPDATE queue_list SET last_consumed = ? WHERE qname = ?} as a
	 * prepared statement, on a new JDBC connection built from
	 * {@code qmanager.db.driver} and {@code qmanager.db.url}.
	 * Errors are printed and otherwise ignored.
	 */
	public void logQConsumerCounter() {
		try {
			Hashtable<String, Long> hTmp = (Hashtable<String, Long>) hTable.clone();
			if (hTmp != null && hTmp.size() > 0) {
				Set<Map.Entry<String, Long>> objMap = hTmp.entrySet();
				for (Map.Entry<String, Long> obj : objMap) {
					String strQname = obj.getKey();
					Long lValue = obj.getValue();
					Connection conn = getConnection();
					PreparedStatement stmt = null;
					try {
						if (conn != null) {
							stmt = conn.prepareStatement("update queue_list set last_consumed=? where qname=?");
							stmt.setString(1, QUtil.millisToDate(lValue));
							stmt.setString(2, strQname);
							stmt.executeUpdate();
						}
					} catch (Exception e) {
						System.out.println("Failed to update QConsumerTime for Q :" + strQueue + "| Ex: " + e.getMessage());
					} finally {
						if (stmt != null) {
							stmt.close();
						}
						if (conn != null) {
							conn.close();
						}
					}
				}
			}
		} catch (Exception e) {
			System.out.println("Failed to update QConsumerTime counter for Q :" + strQueue + "| Ex: " + e.getMessage());

		}
	}

	/** Prefix of the destinations served by the broker's statistics plugin. */
	private static final String STATISTICS_PREFIX = "ActiveMQ.Statistics.Destination.";
	/** How long to wait for a statistics reply, in ms ({@code amq.statistics.timeout.ms}). */
	long statsTimeoutMs = 2000;

	/**
	 * Asks the broker's statistics plugin for this queue's statistics.
	 * Uses its own short-lived session so a transacted main session is not
	 * affected.
	 *
	 * @return the statistics, or {@code null} if no reply arrived in time
	 *         (usually because the plugin is not enabled)
	 */
	private MapMessage queryStatistics() throws JMSException {
		Session statsSession = m_Connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		TemporaryQueue replyTo = null;
		try {
			replyTo = statsSession.createTemporaryQueue();
			MessageConsumer replies = statsSession.createConsumer(replyTo);
			Message request = statsSession.createMessage();
			request.setJMSReplyTo(replyTo);
			MessageProducer requester = statsSession.createProducer(new ActiveMQQueue(STATISTICS_PREFIX + strQueue));
			requester.send(request);
			Message reply = replies.receive(statsTimeoutMs);
			return reply instanceof MapMessage ? (MapMessage) reply : null;
		} finally {
			statsSession.close();
			if (replyTo != null) {
				try {
					replyTo.delete();
				} catch (JMSException ignore) {
				}
			}
		}
	}

	/**
	 * Returns the number of messages waiting in this connector's queue.
	 * <p>
	 * Uses the broker's statistics plugin when it is enabled
	 * ({@code <statisticsBrokerPlugin/>} in {@code activemq.xml}), which gives
	 * an exact count. Otherwise falls back to browsing the queue, which counts
	 * at most the broker's browse page size (400 messages by default) and may
	 * be slow on large queues.
	 *
	 * @return the message count, or {@code -1} if not connected or the query failed
	 */
	public long getMessageCount() {
		if (m_Connection == null) {
			return -1;
		}
		try {
			MapMessage stats = queryStatistics();
			if (stats != null) {
				return stats.getLong("size");
			}
			Session browseSession = m_Connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			try {
				QueueBrowser browser = browseSession.createBrowser(new ActiveMQQueue(strQueue));
				Enumeration<?> messages = browser.getEnumeration();
				long count = 0;
				while (messages.hasMoreElements()) {
					messages.nextElement();
					count++;
				}
				return count;
			} finally {
				browseSession.close();
			}
		} catch (Exception ex) {
			m_nFailureCount++;
			System.out.println("AMQ getMessageCount() for Q : " + strQueue + " failed, Exception : " + ex.getMessage());
		}
		return -1;
	}

	/**
	 * Returns the number of consumers attached to this connector's queue,
	 * including this one.
	 * <p>
	 * Requires the broker's statistics plugin
	 * ({@code <statisticsBrokerPlugin/>} in {@code activemq.xml}).
	 *
	 * @return the consumer count, or {@code -1} if not connected, the plugin
	 *         is not enabled, or the query failed
	 */
	public long getConsumerCount() {
		if (m_Connection == null) {
			return -1;
		}
		try {
			MapMessage stats = queryStatistics();
			if (stats != null) {
				return stats.getLong("consumerCount");
			}
			System.out.println("AMQ getConsumerCount() for Q : " + strQueue
					+ " : no reply from the statistics plugin; is <statisticsBrokerPlugin/> enabled?");
		} catch (Exception ex) {
			m_nFailureCount++;
			System.out.println("AMQ getConsumerCount() for Q : " + strQueue + " failed, Exception : " + ex.getMessage());
		}
		return -1;
	}

	/**
	 * Manual smoke test: loads a properties file and sends one message to a
	 * queue.
	 * <p>
	 * Usage: {@code AMQJMSConnection <properties-file> [queue]}. The queue
	 * defaults to {@code plugin-jmsq-test}.
	 *
	 * @param args {@code args[0]} = properties file, {@code args[1]} = queue (optional)
	 */
	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: AMQJMSConnection <properties-file> [queue]");
			return;
		}
		Properties props = new Properties();
		try (FileInputStream fis = new FileInputStream(args[0])) {
			props.load(fis);
		} catch (Exception e) {
			System.err.println("Cannot read " + args[0] + " : " + e.getMessage());
			return;
		}
		String queue = args.length > 1 ? args[1] : "plugin-jmsq-test";
		AMQJMSConnection amq = AMQJMSConnection.getInstance(props, null, queue, true, false);
		if (amq == null) {
			System.err.println("Could not connect; check the settings in " + args[0]);
			return;
		}
		boolean bResult = amq.send("testMessage", 0);
		System.out.println("Sent to " + queue + " : " + bResult);
		amq.deinit();
	}

}
