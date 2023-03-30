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

package org.apache.ignite.internal.distributionzones;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.distributionzones.DistributionZoneConfigurationParameters.Builder;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneChange;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageChange;

/**
 * Utils to manage distribution zones inside tests.
 */
public class DistributionZonesTestUtil {
    /**
     * Creates distribution zone.
     *
     * @param zoneManager Zone manager.
     * @param zoneName Zone name.
     * @param partitions Zone number of partitions.
     * @param replicas Zone number of replicas.
     * @return A future, which will be completed, when create operation finished.
     */
    public static CompletableFuture<Integer> createZone(
            DistributionZoneManager zoneManager, String zoneName,
            int partitions, int replicas, Consumer<DataStorageChange> dataStorageChangeConsumer) {
        var distributionZoneCfgBuilder = new Builder(zoneName)
                .replicas(replicas)
                .partitions(partitions);

        if (dataStorageChangeConsumer != null) {
            distributionZoneCfgBuilder
                    .dataStorageChangeConsumer(dataStorageChangeConsumer);
        }

        var distributionZoneCfg = distributionZoneCfgBuilder.build();

        return zoneManager.createZone(distributionZoneCfg).thenApply((v) -> zoneManager.getZoneId(zoneName));
    }

    public static CompletableFuture<Integer> createZone(
            DistributionZoneManager zoneManager, String zoneName,
            int partitions, int replicas) {
        return createZone(zoneManager, zoneName, partitions, replicas, null);
    }

    /**
     * Alter the number of zone replicas.
     *
     * @param zoneManager Zone manager.
     * @param zoneName Zone name.
     * @param replicas The new number of zone replicas.
     * @return A future, which will be completed, when update operation finished.
     */
    public static CompletableFuture<Void> alterZoneReplicas(DistributionZoneManager zoneManager, String zoneName, int replicas) {
        var distributionZoneCfgBuilder = new Builder(zoneName)
                .replicas(replicas);

        return zoneManager.alterZone(zoneName, distributionZoneCfgBuilder.build());
    }
}
