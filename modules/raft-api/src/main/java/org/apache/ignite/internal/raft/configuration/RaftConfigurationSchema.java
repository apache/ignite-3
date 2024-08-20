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
import org.apache.ignite.configuration.annotation.Value;

/**
 * Raft configuration schema.
 */
@Config
public class RaftConfigurationSchema {
    /**
     * RPC Timeout for InstallSnapshot request (in milliseconds). This is the maximum allowed duration from sending
     * InstallSnapshot request and getting a response to it; during it, the snapshot must be fully transferred to
     * a recipient and installed.
     */
    @Value(hasDefault = true)
    // TODO: IGNITE-18480 - is 5 minutes a good default?
    public int rpcInstallSnapshotTimeout = 5 * 60 * 1000;

    /** Configuration for Raft groups corresponding to table partitions. */
    // TODO: IGNITE-16647 - Volatile RAFT configuration should be moved elsewhere
    @ConfigValue
    public VolatileRaftConfigurationSchema volatileRaft;

    /**
     * Timeout value (in milliseconds) for which the Raft client will try to receive a successful response from a remote peer.
     */
    @Value(hasDefault = true)
    public long retryTimeout = 10_000;

    /**
     * Delay (in milliseconds) used by the Raft client between re-sending a failed request.
     */
    @Value(hasDefault = true)
    public long retryDelay = 200;

    /**
     * Timeout value (in milliseconds) for which the Raft client will try to receive a response from a remote peer.
     */
    @Value(hasDefault = true)
    public long responseTimeout = 3_000;

    /**
     * Call fsync when need.
     */
    @Value(hasDefault = true)
    public boolean fsync = true;

    /**
     * Amount of Disruptors that will handle the RAFT server.
     */
    @Value(hasDefault = true)
    public int stripes = Runtime.getRuntime().availableProcessors();

    /**
     * Amount of log manager Disruptors stripes.
     */
    @Value(hasDefault = true)
    public int logStripesCount = 4;

    /**
     * Set true to use the non-blocking strategy in the log manager.
     */
    @Value(hasDefault = true)
    public boolean logYieldStrategy = false;
}
