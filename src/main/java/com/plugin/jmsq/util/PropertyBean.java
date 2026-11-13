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

/**
 * One typed JMS message property, or a send option, attached to an outgoing
 * message.
 * <p>
 * Every value is held as a string and converted to the type given by
 * {@link #getPropType()} when the message is sent (see
 * {@link com.plugin.jmsq.mq.AMQJMSConnection#send(String, int, java.util.List)}).
 * Types 1 to 10 become ordinary JMS properties. Types 11 to 14 are not
 * properties: they set the message expiry or a delayed delivery.
 */
public class PropertyBean {

	/** {@code int} property; set with {@code Message.setIntProperty}. */
	public static final int INT_PROPERTY = 1;
	/** {@code short} property; set with {@code Message.setShortProperty}. */
	public static final int SHORT_PROPERTY = 2;
	/** {@code long} property; set with {@code Message.setLongProperty}. */
	public static final int LONG_PROPERTY = 3;
	/** {@code float} property; set with {@code Message.setFloatProperty}. */
	public static final int FLOAT_PROPERTY = 4;
	/** {@code double} property; set with {@code Message.setDoubleProperty}. */
	public static final int DOUBLE_PROPERTY = 5;
	/** {@code String} property; set with {@code Message.setStringProperty}. */
	public static final int STRING_PROPERTY = 6;
	/** {@code boolean} property; set with {@code Message.setBooleanProperty}. */
	public static final int BOOLEAN_PROPERTY = 7;
	/** {@code byte} property; set with {@code Message.setByteProperty}. */
	public static final int BYTE_PROPERTY = 8;
	/** Byte-array property. Declared but not currently handled when sending. */
	public static final int BYTES_PROPERTY = 9;
	/** Object property; the string value is set with {@code Message.setObjectProperty}. */
	public static final int OBJECT_PROPERTY = 10;
	/** Time to live: the message expires this many minutes after it is sent. */
	public static final int EXPIRY_PROPERTY = 11;
	/** Time to live: the message expires this many seconds after it is sent. */
	public static final int EXPIRY_SECS_PROPERTY = 12;
	/**
	 * Delivery delay in minutes, sent as {@code AMQ_SCHEDULED_DELAY}. Needs
	 * {@code schedulerSupport="true"} on the broker.
	 */
	public static final int SCHEDULE_MINS_PROPERTY = 13;
	/**
	 * Delivery delay in seconds, sent as {@code AMQ_SCHEDULED_DELAY}. Needs
	 * {@code schedulerSupport="true"} on the broker.
	 */
	public static final int SCHEDULE_SECS_PROPERTY = 14;

	/** Property name. */
	private String propKey;
	/** Property value as a string; converted according to {@link #propType}. */
	private String propValue;
	/** One of the {@code *_PROPERTY} type constants. */
	private int propType;

	/**
	 * @return the propKey
	 */
	public String getPropKey() {
		return propKey;
	}

	/**
	 * @param propKey the propKey to set
	 */
	public void setPropKey(String propKey) {
		this.propKey = propKey;
	}

	/**
	 * @return the propValue
	 */
	public String getPropValue() {
		return propValue;
	}

	/**
	 * @param propValue the propValue to set
	 */
	public void setPropValue(String propValue) {
		this.propValue = propValue;
	}

	/**
	 * @return the property type, one of the {@code *_PROPERTY} constants
	 */
	public int getPropType() {
		return propType;
	}

	/**
	 * @param propType the property type, one of the {@code *_PROPERTY} constants
	 */
	public void setPropType(int propType) {
		this.propType = propType;
	}

	/** Creates an empty property; set the fields with the setters. */
	public PropertyBean() {

	}

	/**
	 * Creates a fully populated property.
	 *
	 * @param propType  one of the {@code *_PROPERTY} constants
	 * @param propKey   property name
	 * @param propValue property value as a string
	 */
	public PropertyBean(int propType, String propKey, String propValue) {
		this.propKey = propKey;
		this.propType = propType;
		this.propValue = propValue;
	}

	/**
	 * Returns the property as one pipe-delimited string.
	 *
	 * @return {@code "type|key|value"}
	 */
	public String getString() {
		return this.propType + "|" + this.propKey + "|" + this.propValue;
	}
}
