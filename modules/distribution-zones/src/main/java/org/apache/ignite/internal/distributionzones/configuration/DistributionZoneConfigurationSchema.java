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

package org.apache.ignite.internal.distributionzones.configuration;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_ID;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Immutable;
import org.apache.ignite.configuration.validation.Range;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageConfigurationSchema;

/**
 * Distribution zone configuration schema class.
 */
@Config
public class DistributionZoneConfigurationSchema {
    /** Zone name. */
    @InjectedName
    public String name;

    /** Integer zone id. */
    @Immutable
    @Range(min = DEFAULT_ZONE_ID)
    @Value(hasDefault = true)
    public int zoneId = DEFAULT_ZONE_ID;

    /** Count of zone partitions. */
    @Immutable
    @Range(min = 1, max = 65_000)
    @Value(hasDefault = true)
    public int partitions = DEFAULT_PARTITION_COUNT;

    /** Count of zone partition replicas. */
    @Range(min = 1)
    @Value(hasDefault = true)
    public int replicas = DEFAULT_REPLICA_COUNT;

    /** Data storage configuration. */
    @ConfigValue
    public DataStorageConfigurationSchema dataStorage;

    /** Timeout in seconds between node added or node left topology event itself and data nodes switch. */
    @Range(min = 0)
    @Value(hasDefault = true)
    public int dataNodesAutoAdjust = INFINITE_TIMER_VALUE;

    /** Timeout in seconds between node added topology event itself and data nodes switch. */
    @Range(min = 0)
    @Value(hasDefault = true)
    public int dataNodesAutoAdjustScaleUp = IMMEDIATE_TIMER_VALUE;

    /** Timeout in seconds between node left topology event itself and data nodes switch. */
    @Range(min = 0)
    @Value(hasDefault = true)
    public int dataNodesAutoAdjustScaleDown = INFINITE_TIMER_VALUE;

    /**
     * Filter to form nodes which must be included to data nodes of this zone.
     * Default value is {@code $..*}, which is a {@link com.jayway.jsonpath.JsonPath} expression for including all attributes of nodes.
     *
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/distribution-zones/tech-notes/filters.md">Filter documentation</a>
     */
    @ValidFilter
    @Value(hasDefault = true)
    public String filter = DEFAULT_FILTER;
}
