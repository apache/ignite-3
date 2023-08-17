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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_ID;

import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.Name;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.ExceptKeys;

/** Distribution zones configuration schema. */
@ConfigurationRoot(rootName = "zone", type = ConfigurationType.DISTRIBUTED)
public class DistributionZonesConfigurationSchema {
    /** Global integer id counter. Used as an auto-increment counter to generate integer identifiers for distribution zone. */
    @Value(hasDefault = true)
    public int globalIdCounter = DEFAULT_ZONE_ID;

    /** Default distribution zone. */
    @Name(DEFAULT_ZONE_NAME)
    @ConfigValue
    public DistributionZoneConfigurationSchema defaultDistributionZone;

    /** Other distribution zones. */
    @ExceptKeys(DEFAULT_ZONE_NAME)
    @NamedConfigValue
    public DistributionZoneConfigurationSchema distributionZones;
}
