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

package org.apache.ignite.internal.runner.app;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.CreateZoneParams;
import org.apache.ignite.internal.catalog.commands.DataStorageParams;
import org.jetbrains.annotations.Nullable;

/**
 * Utilities to work with distribution zones in tests.
 */
// TODO: IGNITE-19502 - remove after switching to the Catalog.
public class DistributionZonesTestUtil {
    /**
     * Creates distribution zone in the catalog.
     *
     * @param catalogManager Catalog manager.
     * @param zoneName Zone name.
     * @param partitions Zone number of partitions.
     * @param replicas Zone number of replicas.
     */
    public static void createZone(CatalogManager catalogManager, String zoneName, int partitions, int replicas) {
        createZone(catalogManager, zoneName, partitions, replicas, null, null, null, null);
    }

    private static void createZone(
            CatalogManager catalogManager,
            String zoneName,
            @Nullable Integer partitions,
            @Nullable Integer replicas,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter,
            @Nullable String dataStorage
    ) {
        CreateZoneParams.Builder builder = CreateZoneParams.builder().zoneName(zoneName);

        if (partitions != null) {
            builder.partitions(partitions);
        }

        if (replicas != null) {
            builder.replicas(replicas);
        }

        if (dataNodesAutoAdjustScaleUp != null) {
            builder.dataNodesAutoAdjustScaleUp(dataNodesAutoAdjustScaleUp);
        }

        if (dataNodesAutoAdjustScaleDown != null) {
            builder.dataNodesAutoAdjustScaleDown(dataNodesAutoAdjustScaleDown);
        }

        if (filter != null) {
            builder.filter(filter);
        }

        if (dataStorage != null) {
            builder.dataStorage(DataStorageParams.builder().engine(dataStorage).build());
        }

        assertThat(catalogManager.createZone(builder.build()), willCompleteSuccessfully());
    }
}
