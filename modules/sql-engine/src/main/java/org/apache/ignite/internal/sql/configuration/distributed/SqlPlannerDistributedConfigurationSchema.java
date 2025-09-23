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

package org.apache.ignite.internal.sql.configuration.distributed;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;

/** Distributed configuration of SQL planner. */
@Config
public class SqlPlannerDistributedConfigurationSchema {
    /** Planner timeout, in ms. */
    @Value(hasDefault = true)
    @Range(min = 0)
    @PublicName(legacyNames = "maxPlanningTime")
    public final long maxPlanningTimeMillis = 15_000L;

    /**
     * The estimated number of unique queries that are planned to be executed in the cluster in a certain period of time.
     * Used to optimize internal caches and processes.
     */
    @Value(hasDefault = true)
    @Range(min = 0)
    public final int estimatedNumberOfQueries = 1024;

    /**
     * The number of seconds after which a query plan is removed from the query plan cache if it is not used.
     */
    @Value(hasDefault = true)
    @Range(min = 0)
    public final int planCacheExpiresAfterSeconds = 30 * 60;
}
