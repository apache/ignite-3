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

package org.apache.ignite.internal.table.distributed.disaster;

import static java.util.Collections.emptySet;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.enabledColocation;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;

/**
 * Helper util class for disaster recovery tests.
 */
public class TestDisasterRecoveryUtils {

    /**
     * Reset partitions depending on {@link IgniteSystemProperties#enabledColocation}.
     *
     * @param disasterRecoveryManager disaster recovery manager.
     * @param zoneName zone name.
     * @param schemaName schema name.
     * @param tableName table name.
     * @param partitionIds partition ids.
     * @param manualUpdate manual update flag.
     * @param triggerRevision trigger revision.
     * @return future when reset is done.
     */
    public static CompletableFuture<Void> resetPartitions(
            DisasterRecoveryManager disasterRecoveryManager,
            String zoneName,
            String schemaName,
            String tableName,
            Set<Integer> partitionIds,
            boolean manualUpdate,
            long triggerRevision
    ) {
        if (enabledColocation()) {
            return disasterRecoveryManager.resetPartitions(zoneName, partitionIds, manualUpdate, triggerRevision);
        } else {
            return disasterRecoveryManager
                    .resetTablePartitions(zoneName, schemaName, tableName, partitionIds, manualUpdate, triggerRevision);
        }
    }

    /**
     * Return assignments based on states of partitions in the cluster depending on {@link IgniteSystemProperties#enabledColocation}. It is
     * possible that returned value contains nodes from stable and pending, for example, when rebalance is in progress.
     *
     * @param disasterRecoveryManager disaster recovery manager.
     * @param zoneName zone name.
     * @param zoneId zone id.
     * @param tableId table id.
     * @param partitionId partition id.
     * @return assignments.
     */
    public static Set<String> getRealAssignments(
            DisasterRecoveryManager disasterRecoveryManager,
            String zoneName,
            int zoneId,
            int tableId,
            int partitionId
    ) {
        return enabledColocation()
                ? getZoneRealAssignments(disasterRecoveryManager, zoneName, new ZonePartitionId(zoneId, partitionId))
                : getTableRealAssignments(disasterRecoveryManager, zoneName, new TablePartitionId(tableId, partitionId));
    }

    /**
     * Return assignments based on states of partitions in the cluster. It is possible that returned value contains nodes from stable and
     * pending, for example, when rebalance is in progress.
     */
    private static Set<String> getTableRealAssignments(
            DisasterRecoveryManager disasterRecoveryManager,
            String zoneName,
            TablePartitionId tablePartitionId
    ) {
        CompletableFuture<Map<TablePartitionId, LocalTablePartitionStateByNode>> partitionStatesFut = disasterRecoveryManager
                .localTablePartitionStates(Set.of(zoneName), Set.of(), Set.of());
        assertThat(partitionStatesFut, willCompleteSuccessfully());

        LocalTablePartitionStateByNode partitionStates = partitionStatesFut.join().get(tablePartitionId);

        if (partitionStates == null) {
            return emptySet();
        }

        return partitionStates.keySet();
    }

    private static Set<String> getZoneRealAssignments(
            DisasterRecoveryManager disasterRecoveryManager,
            String zoneName,
            ZonePartitionId zonePartitionId
    ) {
        CompletableFuture<Map<ZonePartitionId, LocalPartitionStateByNode>> partitionStatesFut = disasterRecoveryManager
                .localPartitionStates(Set.of(zoneName), Set.of(), Set.of());
        assertThat(partitionStatesFut, willCompleteSuccessfully());

        LocalPartitionStateByNode partitionStates = partitionStatesFut.join().get(zonePartitionId);

        if (partitionStates == null) {
            return emptySet();
        }

        return partitionStates.keySet();
    }

}
