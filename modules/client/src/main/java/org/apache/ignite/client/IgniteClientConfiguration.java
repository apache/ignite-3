/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client;

/**
 * Ignite client configuration.
 */
public interface IgniteClientConfiguration {
    /** Default port. */
    int DFLT_PORT = 10800;

    /** Default port range. */
    int DFLT_PORT_RANGE = 100;

    /**
     * Gets the address finder.
     *
     * @return Address finder.
     */
    IgniteClientAddressFinder addressesFinder();

    /**
     * Gets the addresses of Ignite server nodes within a cluster. An address can be an IP address or a hostname,
     * with or without port. If port is not set then Ignite will generate multiple addresses for default port range.
     * See {@link IgniteClientConfiguration#DFLT_PORT}, {@link IgniteClientConfiguration#DFLT_PORT_RANGE}.
     *
     * @return Addresses.
     */
    String[] addresses();

    /**
     * Gets the retry limit.
     *
     * @return Retry limit.
     */
    int retryLimit();

    /**
     * Gets the socket connect timeout.
     *
     * @return Socket connect timeout.
     */
    int connectTimeout();

    /**
     * Gets the reconnect throttling period.
     *
     * @return Reconnect period (for throttling).
     */
    long reconnectThrottlingPeriod();

    /**
     * Gets the reconnect throttling retries.
     *
     * @return Reconnect retries within period (for throttling).
     */
    int reconnectThrottlingRetries();
}
