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

package org.apache.ignite.internal;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.replicator.ZonePartitionId;

// TODO https://issues.apache.org/jira/browse/IGNITE-22522 Remove this class and change its usages to {@link ZoneRebalanceUtil}.
/**
 * Helper util class for rebalance tests.
 */
public class TestRebalanceUtil {

    /**
     * Returns stable partition assignments key.
     *
     * @param partitionGroupId Partition group identifier.
     * @return Stable partition assignments key.
     */
    public static ByteArray stablePartitionAssignmentsKey(ZonePartitionId partitionGroupId) {
        return ZoneRebalanceUtil.stablePartAssignmentsKey(partitionGroupId);
    }

    /**
     * Returns pending partition assignments key.
     *
     * @param partitionGroupId Partition group identifier.
     * @return Pending partition assignments key.
     */
    public static ByteArray pendingPartitionAssignmentsKey(ZonePartitionId partitionGroupId) {
        return ZoneRebalanceUtil.pendingPartAssignmentsQueueKey(partitionGroupId);
    }

    /**
     * Returns pending partition change trigger key.
     *
     * @param partitionGroupId Partition group identifier.
     * @return Pending partition change trigger key.
     */
    public static ByteArray pendingChangeTriggerKey(ZonePartitionId partitionGroupId) {
        return ZoneRebalanceUtil.pendingChangeTriggerKey(partitionGroupId);
    }

    /**
     * Returns planned partition assignments key.
     *
     * @param partitionGroupId Partition group identifier.
     * @return Planned partition assignments key.
     */
    public static ByteArray plannedPartitionAssignmentsKey(ZonePartitionId partitionGroupId) {
        return ZoneRebalanceUtil.plannedPartAssignmentsKey(partitionGroupId);
    }

    /**
     * Returns stable partition assignments.
     *
     * @param metaStorageManager Meta storage manager.
     * @param zoneId Zone identifier.
     * @param partitionId Partition identifier.
     * @return Stable partition assignments.
     */
    public static CompletableFuture<Set<Assignment>> stablePartitionAssignments(
            MetaStorageManager metaStorageManager,
            int zoneId,
            int partitionId
    ) {
        return ZoneRebalanceUtil.zonePartitionAssignments(metaStorageManager, zoneId, partitionId);
    }

    /**
     * Returns pending partition assignments.
     *
     * @param metaStorageManager Meta storage manager.
     * @param zoneId Zone identifier.
     * @param partitionId Partition identifier.
     * @return Pending partition assignments.
     */
    public static CompletableFuture<Set<Assignment>> pendingPartitionAssignments(
            MetaStorageManager metaStorageManager,
            int zoneId,
            int partitionId
    ) {
        return metaStorageManager
                .get(pendingPartitionAssignmentsKey(new ZonePartitionId(zoneId, partitionId)))
                .thenApply(e -> e.value() == null ? null : AssignmentsQueue.fromBytes(e.value()).poll().nodes());
    }

    /**
     * Returns planned partition assignments.
     *
     * @param metaStorageManager Meta storage manager.
     * @param zoneId Zone identifier.
     * @param partitionId Partition identifier.
     * @return Planned partition assignments.
     */
    public static CompletableFuture<Set<Assignment>> plannedPartitionAssignments(
            MetaStorageManager metaStorageManager,
            int zoneId,
            int partitionId
    ) {
        return metaStorageManager
                .get(plannedPartitionAssignmentsKey(new ZonePartitionId(zoneId, partitionId)))
                .thenApply(e -> e.value() == null ? null : Assignments.fromBytes(e.value()).nodes());
    }
}
