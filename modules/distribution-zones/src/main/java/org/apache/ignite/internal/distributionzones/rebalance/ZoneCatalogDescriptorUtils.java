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

package org.apache.ignite.internal.distributionzones.rebalance;

import java.lang.reflect.Method;
import org.apache.ignite.internal.catalog.descriptors.CatalogDataStorageDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageView;

/**
 * Utils used to convert configuration representations of distribution zones to catalog representations.
 */
// TODO: IGNITE-20114 избавиться
@Deprecated(forRemoval = true)
public class ZoneCatalogDescriptorUtils {
    // TODO: IGNITE-19719 Fix it
    /**
     * Converts a distribution zone configuration to a Distribution zone descriptor.
     *
     * @param config Distribution zone configuration.
     */
    @Deprecated(forRemoval = true)
    public static CatalogZoneDescriptor toZoneDescriptor(DistributionZoneView config) {
        return new CatalogZoneDescriptor(
                config.zoneId(),
                config.name(),
                config.partitions(),
                config.replicas(),
                config.dataNodesAutoAdjust(),
                config.dataNodesAutoAdjustScaleUp(),
                config.dataNodesAutoAdjustScaleDown(),
                config.filter(),
                toDataStorageDescriptor(config.dataStorage())
        );
    }

    @Deprecated(forRemoval = true)
    private static CatalogDataStorageDescriptor toDataStorageDescriptor(DataStorageView config) {
        String dataRegion;

        try {
            Method dataRegionMethod = config.getClass().getMethod("dataRegion");

            dataRegionMethod.setAccessible(true);

            dataRegion = (String) dataRegionMethod.invoke(config);
        } catch (ReflectiveOperationException e) {
            dataRegion = e.getMessage();
        }

        return new CatalogDataStorageDescriptor(config.name(), dataRegion);
    }
}
