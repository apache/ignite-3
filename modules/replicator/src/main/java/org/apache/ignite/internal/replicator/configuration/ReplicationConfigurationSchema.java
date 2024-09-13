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

package org.apache.ignite.internal.replicator.configuration;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;

/**
 * Configuration for Replication.
 */
@Config
public class ReplicationConfigurationSchema {
    /** Default value for {@link #idleSafeTimePropagationDuration}. */
    public static final long DEFAULT_IDLE_SAFE_TIME_PROP_DURATION = TimeUnit.SECONDS.toMillis(1);

    /** Idle safe time propagation duration (ms) for partitions. */
    @Value(hasDefault = true)
    @Range(min = 0)
    // TODO: IGNITE-19792 - make @Immutable when it gets being handled property for distributed config.
    public long idleSafeTimePropagationDuration = DEFAULT_IDLE_SAFE_TIME_PROP_DURATION;

    /** Replication request processing timeout.  */
    @Value(hasDefault = true)
    @Range(min = 1000)
    public long rpcTimeout = TimeUnit.SECONDS.toMillis(60);

    /** The interval in milliseconds that is used in the beginning of lease granting process. */
    @Value(hasDefault = true)
    @Range(min = 5000)
    public long leaseAgreementAcceptanceTimeLimit = 120_000;

    /** Lease holding interval. */
    @Value(hasDefault = true)
    @Range(min = 2000, max = 120000)
    public long leaseExpirationInterval = 5_000;
}
