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

package org.apache.ignite.internal.placementdriver;

import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignments;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.leases.LeaseBatch;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.jetbrains.annotations.Nullable;

/** Base class for testing the placement driver. */
abstract class BasePlacementDriverTest extends IgniteAbstractTest {
    private static ZonePartitionId targetReplicationGroupId(int zoneId, int partId) {
        return new ZonePartitionId(zoneId, partId);
    }

    /**
     * Creates an assignment for the fake table.
     *
     * @return Replication group id.
     */
    protected PartitionGroupId createAssignments(
            MetaStorageManager metastore,
            int zoneId,
            List<String> dataNodes,
            long assignmentsTimestamp
    ) {
        List<Set<Assignment>> assignments = calculateAssignments(dataNodes, 1, dataNodes.size(), dataNodes.size());

        Map<ByteArray, byte[]> partitionAssignments = new HashMap<>(assignments.size());

        for (int i = 0; i < assignments.size(); i++) {
            ZonePartitionId replicationGroupId = targetReplicationGroupId(zoneId, i);
            ByteArray stableAssignmentsKey = ZoneRebalanceUtil.stablePartAssignmentsKey(replicationGroupId);

            partitionAssignments.put(
                    stableAssignmentsKey,
                    Assignments.toBytes(assignments.get(i), assignmentsTimestamp));
        }

        metastore.putAll(partitionAssignments).join();

        ZonePartitionId grpPart0 = targetReplicationGroupId(zoneId, 0);

        log.info("Fake table created [id={}, replicationGroup={}]", zoneId, grpPart0);

        return grpPart0;
    }

    protected static @Nullable Lease leaseFromBytes(byte[] bytes, ReplicationGroupId groupId) {
        LeaseBatch leaseBatch = LeaseBatch.fromBytes(bytes);

        return leaseBatch.leases().stream()
                .filter(l -> l.replicationGroupId().equals(groupId))
                .findAny()
                .orElse(null);
    }
}
