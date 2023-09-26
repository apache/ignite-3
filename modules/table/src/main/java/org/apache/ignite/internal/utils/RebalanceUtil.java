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

import static org.apache.ignite.internal.metastorage.dsl.Conditions.and;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.revision;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.util.CollectionUtils.difference;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.util.ByteUtils;

/**
 * Util class for methods needed for the rebalance process.
 */
// TODO: https://issues.apache.org/jira/browse/IGNITE-18857 All rebalance logic and thus given RebalanceUtil should be moved to zones one.
public class RebalanceUtil {
    /** Key prefix for pending assignments. */
    public static final String PENDING_ASSIGNMENTS_PREFIX = "assignments.pending.";

    /** Key prefix for stable assignments. */
    public static final String STABLE_ASSIGNMENTS_PREFIX = "assignments.stable.";

    /** Key prefix for switch reduce assignments. */
    public static final String ASSIGNMENTS_SWITCH_REDUCE_PREFIX = "assignments.switch.reduce.";

    /** Key prefix for switch append assignments. */
    public static final String ASSIGNMENTS_SWITCH_APPEND_PREFIX = "assignments.switch.append.";

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalnce documentation</a>
     */
    public static ByteArray partChangeTriggerKey(TablePartitionId partId) {
        return new ByteArray(partId + ".change.trigger");
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalnce documentation</a>
     */
    public static ByteArray pendingPartAssignmentsKey(TablePartitionId partId) {
        return new ByteArray(PENDING_ASSIGNMENTS_PREFIX + partId);
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalnce documentation</a>
     */
    public static ByteArray plannedPartAssignmentsKey(TablePartitionId partId) {
        return new ByteArray("assignments.planned." + partId);
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalnce documentation</a>
     */
    public static ByteArray stablePartAssignmentsKey(TablePartitionId partId) {
        return new ByteArray(STABLE_ASSIGNMENTS_PREFIX + partId);
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalnce documentation</a>
     */
    public static ByteArray switchReduceKey(TablePartitionId partId) {
        return new ByteArray(ASSIGNMENTS_SWITCH_REDUCE_PREFIX + partId);
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalnce documentation</a>
     */
    public static ByteArray switchAppendKey(TablePartitionId partId) {
        return new ByteArray(ASSIGNMENTS_SWITCH_APPEND_PREFIX + partId);
    }

    /**
     * Extract table id from a metastorage key of partition.
     *
     * @param key Key.
     * @return Table id.
     */
    public static int extractTableId(byte[] key) {
        return extractTableId(key, "");
    }

    /**
     * Extract table id from a metastorage key of partition.
     *
     * @param key Key.
     * @param prefix Key prefix.
     * @return Table id.
     */
    public static int extractTableId(byte[] key, String prefix) {
        String strKey = new String(key, StandardCharsets.UTF_8);

        return Integer.parseInt(strKey.substring(prefix.length(), strKey.indexOf("_part_")));
    }

    /**
     * Extract partition number from the rebalance key of partition.
     *
     * @param key Key.
     * @return Partition number.
     */
    public static int extractPartitionNumber(byte[] key) {
        var strKey = new String(key, StandardCharsets.UTF_8);

        return Integer.parseInt(strKey.substring(strKey.indexOf("_part_") + "_part_".length()));
    }

    /**
     * Checks if an error is recoverable, so we can retry a rebalance intent.
     *
     * @param t The throwable.
     * @return {@code True} if this is a recoverable exception.
     */
    public static boolean recoverable(Throwable t) {
        // As long as we don't have a general failure handler, we assume that all errors are recoverable.
        return true;
    }

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
            MetaStorageManager metaStorageMgr
    ) {
        ByteArray key = switchReduceKey(partId);

        return metaStorageMgr.get(key)
                .thenCompose(retrievedAssignmentsSwitchReduce -> {
                    byte[] prevValue = retrievedAssignmentsSwitchReduce.value();

                    if (prevValue != null) {
                        Set<Assignment> prev = ByteUtils.fromBytes(prevValue);

                        prev.add(peerAssignment);

                        return metaStorageMgr.invoke(
                                revision(key).eq(retrievedAssignmentsSwitchReduce.revision()),
                                put(key, ByteUtils.toBytes(prev)),
                                Operations.noop()
                        );
                    } else {
                        var newValue = new HashSet<>();

                        newValue.add(peerAssignment);

                        return metaStorageMgr.invoke(
                                notExists(key),
                                put(key, ByteUtils.toBytes(newValue)),
                                Operations.noop()
                        );
                    }
                }).thenCompose(res -> {
                    if (!res) {
                        return startPeerRemoval(partId, peerAssignment, metaStorageMgr);
                    }

                    return CompletableFuture.completedFuture(null);
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
    public static CompletableFuture<Void> handleReduceChanged(MetaStorageManager metaStorageMgr, Collection<String> dataNodes,
            int replicas, TablePartitionId partId, WatchEvent event) {
        Entry entry = event.entryEvent().newEntry();
        byte[] eventData = entry.value();

        Set<Assignment> switchReduce = ByteUtils.fromBytes(eventData);

        if (switchReduce.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        Set<Assignment> assignments = AffinityUtils.calculateAssignmentForPartition(dataNodes, partId.partitionId(), replicas);

        ByteArray pendingKey = pendingPartAssignmentsKey(partId);

        Set<Assignment> pendingAssignments = difference(assignments, switchReduce);

        byte[] pendingByteArray = ByteUtils.toBytes(pendingAssignments);
        byte[] assignmentsByteArray = ByteUtils.toBytes(assignments);

        ByteArray changeTriggerKey = partChangeTriggerKey(partId);
        byte[] rev = ByteUtils.longToBytes(entry.revision());

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

    /**
     * Adds nodes to the set of nodes.
     *
     * @param op1 First operand.
     * @param op2 Second operand.
     * @return Result of the addition.
     */
    public static <T> Set<T> union(Set<T> op1, Set<T> op2) {
        var res = new HashSet<>(op1);

        res.addAll(op2);

        return res;
    }
}
