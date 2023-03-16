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

package org.apache.ignite.configuration.schemas.network;

import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;

/**
 * Configuration schema for the network endpoint subtree.
 */
@ConfigurationRoot(rootName = "network", type = ConfigurationType.LOCAL)
public class NetworkConfigurationSchema {
    /** Network port. */
    @Range(min = 1024, max = 0xFFFF)
    @Value(hasDefault = true)
    public final int port = 47500;

    /** Network port range. */
    @Range(min = 0)
    @Value(hasDefault = true)
    public final int portRange = 0;

    /**
     * Graceful shutdown of the Netty's EventExecutorGroup ensures that no tasks are submitted for
     * <i>'the quiet period'</i> before the group shuts itself down. If a task is submitted during the quiet period,
     * it is guaranteed to be accepted; this restarts the quiet period.
     */
    @Range(min = 0)
    @Value(hasDefault = true)
    public final long shutdownQuietPeriod = 0;

    /**
     * The maximum amount of time to wait until each Netty's EventExecutorGroup is shut down regardless of whether a new network message was
     * submitted during the quiet period.
     */
    @Range(min = 0)
    @Value(hasDefault = true)
    public final long shutdownTimeout = 15_000;

    /** Server configuration. */
    @ConfigValue
    public InboundConfigurationSchema inbound;

    /** Client configuration. */
    @ConfigValue
    public OutboundConfigurationSchema outbound;

    /** Membership configuration. */
    @ConfigValue
    public ClusterMembershipConfigurationSchema membership;

    /** NodeFinder configuration. */
    @ConfigValue
    public NodeFinderConfigurationSchema nodeFinder;
}
