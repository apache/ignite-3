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

package org.apache.ignite.internal.worker.configuration;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;

/**
 * Configuration for critical workers handling.
 */
@Config
public class CriticalWorkersConfigurationSchema {
    public static final int DEFAULT_LIVENESS_CHECK_INTERVAL_MILLIS = 200;
    public static final int DEFAULT_MAX_ALLOWED_LAG_MILLIS = 500;
    public static final int DEFAULT_NETTY_THREADS_HEARTBEAT_INTERVAL_MILLIS = 100;

    /**
     * Interval between liveness checks (ms) performed by the critical workers infrastructure.
     *
     * <p>Should not be greater than a half of {@link #maxAllowedLagMillis}.
     */
    @Value(hasDefault = true)
    @Range(min = 1)
    @PublicName(legacyNames = "livenessCheckInterval")
    public long livenessCheckIntervalMillis = DEFAULT_LIVENESS_CHECK_INTERVAL_MILLIS;

    /**
     * Maximum allowed delay of the last heartbeat from current time; if it's exceeded, the critical worker is considered to be blocked.
     *
     * <p>Should be at least twice as large as {@link #livenessCheckIntervalMillis}.
     */
    @Value(hasDefault = true)
    @Range(min = 1)
    @PublicName(legacyNames = "maxAllowedLag")
    public long maxAllowedLagMillis = DEFAULT_MAX_ALLOWED_LAG_MILLIS;

    /**
     * Interval between heartbeats used to update Netty threads heartbeat timestamps.
     */
    @Value(hasDefault = true)
    @Range(min = 1)
    @PublicName(legacyNames = "nettyThreadsHeartbeatInterval")
    public long nettyThreadsHeartbeatIntervalMillis = DEFAULT_NETTY_THREADS_HEARTBEAT_INTERVAL_MILLIS;
}
