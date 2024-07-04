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

package org.apache.ignite.internal.utils;

import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.pendingChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.switchReduceKey;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.and;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.revision;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.util.ByteUtils.longToBytesKeepingOrder;
import static org.apache.ignite.internal.util.CollectionUtils.difference;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.replicator.TablePartitionId;

/**
 * Util class for methods needed for the rebalance process. "Ex" stands for "Excommunicado". This class should be removed in the future.
 */
// TODO: https://issues.apache.org/jira/browse/IGNITE-18857 All rebalance logic and thus given RebalanceUtil should be moved to zones one.
public class RebalanceUtilEx {
    /**
     * Starts the process of removing peer from raft group if that peer has in-memory storage or if its
     * storage has been cleared.
     *
     * @param partId Partition's raft group id.
     * @param peerAssignment Assignment of the peer to be removed.
     * @param metaStorageMgr MetaStorage manager.
     * @return Completable future that signifies the completion of this operation.
     */
    public static CompletableFuture<Void> startPeerRemoval(
            TablePartitionId partId,
            Assignment peerAssignment,
            MetaStorageManager metaStorageMgr,
            int catalogVersion
    ) {
        ByteArray key = switchReduceKey(partId);

        return metaStorageMgr.get(key)
                .thenCompose(retrievedAssignmentsSwitchReduce -> {
                    byte[] prevValue = retrievedAssignmentsSwitchReduce.value();

                    if (prevValue != null) {
                        Assignments prev = Assignments.fromBytes(prevValue);

                        prev.add(peerAssignment);

                        return metaStorageMgr.invoke(
                                revision(key).eq(retrievedAssignmentsSwitchReduce.revision()),
                                put(key, prev.toBytes()),
                                Operations.noop()
                        );
                    } else {
                        var newValue = Assignments.of(catalogVersion, new HashSet<>());

                        newValue.add(peerAssignment);

                        return metaStorageMgr.invoke(
                                notExists(key),
                                put(key, newValue.toBytes()),
                                Operations.noop()
                        );
                    }
                }).thenCompose(res -> {
                    if (!res) {
                        return startPeerRemoval(partId, peerAssignment, metaStorageMgr, catalogVersion);
                    }

                    return nullCompletedFuture();
                });
    }

    /**
     * Handles assignments switch reduce changed updating pending assignments if there is no rebalancing in progress.
     * If there is rebalancing in progress, then new assignments will be applied when rebalance finishes.
     *
     * @param metaStorageMgr MetaStorage manager.
     * @param dataNodes Data nodes.
     * @param replicas Replicas count.
     * @param partId Partition's raft group id.
     * @param event Assignments switch reduce change event.
     * @return Completable future that signifies the completion of this operation.
     */
    public static CompletableFuture<Void> handleReduceChanged(
            MetaStorageManager metaStorageMgr,
            Collection<String> dataNodes,
            int replicas,
            TablePartitionId partId,
            WatchEvent event,
            int catalogVersion
    ) {
        Entry entry = event.entryEvent().newEntry();
        byte[] eventData = entry.value();

        assert eventData != null : "Null event data for " + partId;

        Assignments switchReduce = Assignments.fromBytes(eventData);

        if (switchReduce.isEmpty()) {
            return nullCompletedFuture();
        }

        Set<Assignment> assignments = AffinityUtils.calculateAssignmentForPartition(dataNodes, partId.partitionId(), replicas);

        ByteArray pendingKey = pendingPartAssignmentsKey(partId);

        Set<Assignment> pendingAssignments = difference(assignments, switchReduce.nodes());

        byte[] pendingByteArray = Assignments.toBytes(catalogVersion, pendingAssignments);
        byte[] assignmentsByteArray = Assignments.toBytes(catalogVersion, assignments);

        ByteArray changeTriggerKey = pendingChangeTriggerKey(partId);
        byte[] rev = longToBytesKeepingOrder(entry.revision());

        // Here is what happens in the MetaStorage:
        // if ((notExists(changeTriggerKey) || value(changeTriggerKey) < revision) && (notExists(pendingKey) && notExists(stableKey)) {
        //     put(pendingKey, pending)
        //     put(stableKey, assignments)
        //     put(changeTriggerKey, revision)
        // } else if ((notExists(changeTriggerKey) || value(changeTriggerKey) < revision) && (notExists(pendingKey))) {
        //     put(pendingKey, pending)
        //     put(changeTriggerKey, revision)
        // }

        Iif resultingOperation = iif(
                and(
                        or(notExists(changeTriggerKey), value(changeTriggerKey).lt(rev)),
                        and(notExists(pendingKey), (notExists(stablePartAssignmentsKey(partId))))
                ),
                ops(
                        put(pendingKey, pendingByteArray),
                        put(stablePartAssignmentsKey(partId), assignmentsByteArray),
                        put(changeTriggerKey, rev)
                ).yield(),
                iif(
                        and(
                                or(notExists(changeTriggerKey), value(changeTriggerKey).lt(rev)),
                                notExists(pendingKey)
                        ),
                        ops(
                                put(pendingKey, pendingByteArray),
                                put(changeTriggerKey, rev)
                        ).yield(),
                        ops().yield()
                )
        );

        return metaStorageMgr.invoke(resultingOperation).thenApply(unused -> null);
    }
}
