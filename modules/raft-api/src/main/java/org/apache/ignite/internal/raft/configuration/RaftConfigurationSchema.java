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

package org.apache.ignite.internal.raft.configuration;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;

/**
 * Raft configuration schema.
 */
@Config
public class RaftConfigurationSchema {
    /**
     * Timeout for InstallSnapshot request (in milliseconds). This is the maximum allowed duration from sending
     * InstallSnapshot request and getting a response to it; during it, the snapshot must be fully transferred to
     * a recipient and installed.
     */
    @Value(hasDefault = true)
    @PublicName(legacyNames = "installSnapshotTimeout")
    public int installSnapshotTimeoutMillis = Integer.MAX_VALUE;

    /** Configuration for Raft groups corresponding to table partitions. */
    // TODO: IGNITE-16647 - Volatile RAFT configuration should be moved elsewhere
    @ConfigValue
    public VolatileRaftConfigurationSchema volatileRaft;

    /**
     * Timeout value (in milliseconds) for which the Raft client will try to receive a successful response from a remote peer.
     */
    @Value(hasDefault = true)
    @PublicName(legacyNames = "retryTimeout")
    public long retryTimeoutMillis = 10_000;

    /**
     * Delay (in milliseconds) used by the Raft client between re-sending a failed request.
     */
    @Value(hasDefault = true)
    @PublicName(legacyNames = "retryDelay")
    public long retryDelayMillis = 200;

    /**
     * Timeout value (in milliseconds) for which the Raft client will try to receive a response from a remote peer.
     */
    @Value(hasDefault = true)
    @PublicName(legacyNames = "responseTimeout")
    public long responseTimeoutMillis = 3_000;

    /**
     * Whether Raft log entries should be fsynced on table partition groups before reporting that the entries are replicated.
     *
     * <p>When this is {@code false}, the node might lose user data in case of an OS crash. Crash of just Ignite application
     * does not cause user data loss even when this setting is {@code false}.
     */
    @Value(hasDefault = true)
    public boolean fsync = false;

    /**
     * Amount of Disruptors that will handle the RAFT server.
     *
     * @see DisruptorConfigurationSchema#stripes
     */
    @Deprecated
    @Value(hasDefault = true)
    public int stripes = DisruptorConfigurationSchema.DEFAULT_STRIPES_COUNT;

    /**
     * Amount of log manager Disruptors stripes.
     *
     * @see DisruptorConfigurationSchema#logManagerStripes
     */
    @Deprecated
    @Value(hasDefault = true)
    public int logStripesCount = DisruptorConfigurationSchema.DEFAULT_LOG_MANAGER_STRIPES_COUNT;

    /**
     * Set true to use the non-blocking strategy in the log manager.
     */
    @Value(hasDefault = true)
    public boolean logYieldStrategy = false;

    /**
     * Value for max inflights overflow rate. It's used in partitions throttling context.
     * {@code 1.0} is too strict, so we use {@code 1.3}, allows 30% overflow.
     */
    @Value(hasDefault = true)
    public double maxInflightOverflowRate = 1.3;

    /** Configuration for RAFT disruptor's. */
    @ConfigValue
    public DisruptorConfigurationSchema disruptor;
}
