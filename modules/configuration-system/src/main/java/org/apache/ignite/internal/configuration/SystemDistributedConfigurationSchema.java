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

package org.apache.ignite.internal.configuration;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.CamelCaseKeys;
import org.apache.ignite.configuration.validation.Range;

/** Distributed system configuration schema. */
@Config
public class SystemDistributedConfigurationSchema {
    static final int DEFAULT_IDLE_SAFE_TIME_SYNC_INTERVAL_MILLIS = 250;

    /**
     * Duration (in milliseconds) used to determine how often to issue safe time sync commands when the Meta Storage is idle
     * (no writes are being issued).
     *
     * <p>Making this value too small increases the network load, while making this value too large can lead to increased latency of
     * Meta Storage reads.
     */
    @Value(hasDefault = true)
    @Range(min = 1)
    public long idleSafeTimeSyncIntervalMillis = DEFAULT_IDLE_SAFE_TIME_SYNC_INTERVAL_MILLIS;

    /** System properties. */
    @CamelCaseKeys
    @NamedConfigValue
    public SystemPropertyConfigurationSchema properties;
}
