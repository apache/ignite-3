/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.utils;

import static org.apache.ignite.internal.metastorage.client.CompoundCondition.and;
import static org.apache.ignite.internal.metastorage.client.CompoundCondition.or;
import static org.apache.ignite.internal.metastorage.client.Conditions.exists;
import static org.apache.ignite.internal.metastorage.client.Conditions.value;
import static org.apache.ignite.internal.metastorage.client.Operations.ops;
import static org.apache.ignite.internal.metastorage.client.Operations.put;
import static org.apache.ignite.internal.metastorage.client.Operations.remove;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.If;
import org.apache.ignite.internal.metastorage.client.StatementResult;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.NotNull;

/**
 * Util class for methods needed for the rebalance process.
 */
public class RebalanceUtil {

    /**
     * Update keys that related to rebalance algorithm in Meta Storage. Keys are specific for partition.
     *
     * @param partId Unique identifier of a partition.
     * @param baselineNodes Nodes in baseline.
     * @param partitions Number of partitions in a table.
     * @param replicas Number of replicas for a table.
     * @param revision Revision of Meta Storage that is specific for the assignment update.
     * @param metaStorageMgr Meta Storage manager.
     * @return Future representing result of updating keys in {@code metaStorageMgr}
     */
    public static @NotNull CompletableFuture<StatementResult> updateAssignmentsKeys(String partId, Collection<ClusterNode> baselineNodes,
            int partitions, int replicas, long revision, MetaStorageManager metaStorageMgr) {
        ByteArray partChangeTriggerKey = partChangeTriggerKey(partId);

        ByteArray partAssignmentsPendingKey = partAssignmentsPendingKey(partId);

        ByteArray partAssignmentsPlannedKey = partAssignmentsPlannedKey(partId);

        ByteArray partAssignmentsStableKey = partAssignmentsStableKey(partId);

        byte[] partAssignmentsBytes = ByteUtils.toBytes(AffinityUtils.calculateAssignments(baselineNodes, partitions, replicas));

        //    if empty(partition.change.trigger.revision) || partition.change.trigger.revision < event.revision:
        //        if empty(partition.assignments.pending) && partition.assignments.stable != calcPartAssighments():
        //            partition.assignments.pending = calcPartAssignments()
        //            partition.change.trigger.revision = event.revision
        //        else:
        //            if partition.assignments.pending != calcPartAssignments
        //                partition.assignments.planned = calcPartAssignments()
        //                partition.change.trigger.revision = event.revision
        //            else
        //                remove(partition.assignments.planned)
        //    else:
        //        skip
        var iif = If.iif(or(exists(partChangeTriggerKey), value(partChangeTriggerKey).lt(ByteUtils.longToBytes(revision))),
                If.iif(and(exists(partAssignmentsPendingKey), value(partAssignmentsStableKey).ne(partAssignmentsBytes)),
                        ops(
                                put(partAssignmentsPendingKey, partAssignmentsBytes),
                                put(partChangeTriggerKey, ByteUtils.longToBytes(revision))
                        ).yield(),
                        If.iif(value(partAssignmentsPendingKey).ne(partAssignmentsBytes),
                                ops(
                                        put(partAssignmentsPlannedKey, partAssignmentsBytes),
                                        put(partChangeTriggerKey, ByteUtils.longToBytes(revision))
                                ).yield(),
                                ops(remove(partAssignmentsPlannedKey)).yield())),
                ops().yield());

        return metaStorageMgr.invoke(iif);
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalnce documentation</a>
     */
    public static ByteArray partChangeTriggerKey(String partId) {
        return new ByteArray(partId + ".change.trigger");
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalnce documentation</a>
     */
    public static ByteArray partAssignmentsPendingKey(String partId) {
        return new ByteArray(partId + ".assignments.pending");
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalnce documentation</a>
     */
    public static ByteArray partAssignmentsPlannedKey(String partId) {
        return new ByteArray(partId + ".assignments.planned");
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalnce documentation</a>
     */
    public static ByteArray partAssignmentsStableKey(String partId) {
        return new ByteArray(partId + ".assignments.stable");
    }
}
