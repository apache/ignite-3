/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.network.configuration;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;

/**
 * Configuration schema for network endpoint subtree.
 */
@Config
public class NetworkConfigurationSchema {
    /** Default network port. */
    public static final int DEFAULT_PORT = 3344;

    /** Network port. */
    @Range(min = 1024, max = 0xFFFF)
    @Value(hasDefault = true)
    public final int port = DEFAULT_PORT;

    /** Addresses (IPs or hostnames) to listen on. Will listen on all interfaces if empty. */
    @Value(hasDefault = true)
    public String[] listenAddresses = new String[0];

    /**
     * Graceful shutdown of the Netty's EventExecutorGroup ensures that no tasks are submitted for
     * <i>'the quiet period'</i> before it shuts itself down. If a task is submitted during the quiet period,
     * it is guaranteed to be accepted and the quiet period will start over.
     */
    @Range(min = 0)
    @Value(hasDefault = true)
    @PublicName(legacyNames = "shutdownQuietPeriod")
    public final long shutdownQuietPeriodMillis = 0;

    /**
     * The maximum amount of time to wait until each Netty's EventExecutorGroup is shutdown regardless if a new network message was
     * submitted during the quiet period.
     */
    @Range(min = 0)
    @Value(hasDefault = true)
    @PublicName(legacyNames = "shutdownTimeout")
    public final long shutdownTimeoutMillis = 15_000;

    /** Inbound (TCP server) configuration. */
    @ConfigValue
    public InboundConfigurationSchema inbound;

    /** Outbound (TCP client of server-to-server protocol) configuration. */
    @ConfigValue
    public OutboundConfigurationSchema outbound;

    /** Membership configuration. */
    @ConfigValue
    public ClusterMembershipConfigurationSchema membership;

    /** NodeFinder configuration. */
    @ConfigValue
    public NodeFinderConfigurationSchema nodeFinder;

    /** SSL configuration.*/
    @ConfigValue
    @SslConfigurationValidator
    public SslConfigurationSchema ssl;

    /** File transferring configuration. */
    @ConfigValue
    public FileTransferConfigurationSchema fileTransfer;
}
