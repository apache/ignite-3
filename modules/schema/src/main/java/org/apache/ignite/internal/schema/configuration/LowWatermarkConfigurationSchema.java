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

package org.apache.ignite.internal.schema.configuration;

import static org.apache.ignite.internal.replicator.ReplicatorConstants.DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;

/** Low watermark configuration schema. */
@Config
public class LowWatermarkConfigurationSchema {
    /**
     * Data availability time (in milliseconds), after which the overwritten/deleted data can be finally removed from storage.
     *
     * <p>It is also used for read-only transactions that can read data in the past.
     *
     * <p>Value is used when calculating the new low watermark candidate, which at the time of the update is calculated as
     * {@code now() - dataAvailabilityTimeMillis()}.
     */
    // TODO https://issues.apache.org/jira/browse/IGNITE-18977 Create dynamic validator making sure this value is more than safe time
    // propagation period plus max clock skew.
    @Range(min = DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS)
    @Value(hasDefault = true)
    @PublicName(legacyNames = "dataAvailabilityTime")
    public long dataAvailabilityTimeMillis = TimeUnit.MINUTES.toMillis(10);

    /** Low watermark update interval (in milliseconds). */
    @Range(min = 0)
    @Value(hasDefault = true)
    @PublicName(legacyNames = "updateInterval")
    public long updateIntervalMillis = TimeUnit.MINUTES.toMillis(5);
}
