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
package com.plugin.jmsq;

/**
 * Contract for a queue-backed logging client.
 * <p>
 * An implementation connects to a message broker, accepts log entries through
 * {@link #addLog(String, String, boolean)}, and forwards them to the broker,
 * optionally buffering entries that have not been delivered yet.
 * <p>
 * Lifecycle: call {@link #init(String, int, String)} once, call
 * {@link #addLog(String, String, boolean)} as often as needed, then call
 * {@link #deinit()} to release resources.
 */
public interface IJmsClient {

	/**
	 * Connects the client to the broker.
	 *
	 * @param strIPHost  broker host name or IP address
	 * @param nPort      broker port
	 * @param strLogPath local path used for file logging or as a fallback store
	 * @return {@code true} if the client is ready to accept log entries
	 */
	public boolean init(String strIPHost, int nPort, String strLogPath);

	/**
	 * Submits one log entry for delivery.
	 *
	 * @param strQueryID identifier that groups or correlates the entry
	 * @param str        log payload
	 * @param bLogToFile {@code true} to also write the entry to the local log path
	 * @return an implementation-defined status code
	 * @throws Exception if the entry cannot be queued or written
	 */
	public int addLog(String strQueryID, String str, boolean bLogToFile) throws Exception;

	/**
	 * Returns how many submitted entries have not yet been delivered.
	 *
	 * @return number of pending entries
	 */
	public int getPendingSize();

	/**
	 * Closes the broker connection and releases resources.
	 *
	 * @return {@code true} if shutdown completed without errors
	 */
	public boolean deinit();
}
