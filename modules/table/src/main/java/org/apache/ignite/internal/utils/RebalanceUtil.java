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

import static org.apache.ignite.internal.metastorage.client.CompoundCondition.and;
import static org.apache.ignite.internal.metastorage.client.CompoundCondition.or;
import static org.apache.ignite.internal.metastorage.client.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.client.Conditions.revision;
import static org.apache.ignite.internal.metastorage.client.Conditions.value;
import static org.apache.ignite.internal.metastorage.client.If.iif;
import static org.apache.ignite.internal.metastorage.client.Operations.ops;
import static org.apache.ignite.internal.metastorage.client.Operations.put;
import static org.apache.ignite.internal.metastorage.client.Operations.remove;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.metastorage.client.If;
import org.apache.ignite.internal.metastorage.client.Operations;
import org.apache.ignite.internal.metastorage.client.WatchEvent;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.NotNull;

/**
 * Util class for methods needed for the rebalance process.
 */
public class RebalanceUtil {

    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RebalanceUtil.class);

    /** Return code of metastore multi-invoke which identifies,
     * that pending key was updated to new value (i.e. there is no active rebalance at the moment of call).
     */
    private static final int PENDING_KEY_UPDATED = 0;

    /** Return code of metastore multi-invoke which identifies,
     * that planned key was updated to new value (i.e. there is an active rebalance at the moment of call).
     */
    private static final int PLANNED_KEY_UPDATED = 1;

    /** Return code of metastore multi-invoke which identifies,
     * that planned key was removed, because current rebalance is already have the same target.
     */
    private static final int PLANNED_KEY_REMOVED = 2;

    /** Return code of metastore multi-invoke which identifies,
     * that this trigger event was already processed by another node and must be skipped.
     */
    private static final int OUTDATED_UPDATE_RECEIVED = 3;

    /**
     * Update keys that related to rebalance algorithm in Meta Storage. Keys are specific for partition.
     *
     * @param tableName Table name.
     * @param partId Unique identifier of a partition.
     * @param baselineNodes Nodes in baseline.
     * @param replicas Number of replicas for a table.
     * @param revision Revision of Meta Storage that is specific for the assignment update.
     * @param metaStorageMgr Meta Storage manager.
     * @return Future representing result of updating keys in {@code metaStorageMgr}
     */
    public static @NotNull CompletableFuture<Void> updatePendingAssignmentsKeys(
            String tableName, TablePartitionId partId, Collection<ClusterNode> baselineNodes,
            int replicas, long revision, MetaStorageManager metaStorageMgr, int partNum) {
        ByteArray partChangeTriggerKey = partChangeTriggerKey(partId);

        ByteArray partAssignmentsPendingKey = pendingPartAssignmentsKey(partId);

        ByteArray partAssignmentsPlannedKey = plannedPartAssignmentsKey(partId);

        ByteArray partAssignmentsStableKey = stablePartAssignmentsKey(partId);

        Set<Assignment> partAssignments = AffinityUtils.calculateAssignmentForPartition(baselineNodes, partNum, replicas);

        byte[] partAssignmentsBytes = ByteUtils.toBytes(partAssignments);

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
        var iif = iif(or(notExists(partChangeTriggerKey), value(partChangeTriggerKey).lt(ByteUtils.longToBytes(revision))),
                iif(and(notExists(partAssignmentsPendingKey), value(partAssignmentsStableKey).ne(partAssignmentsBytes)),
                        ops(
                                put(partAssignmentsPendingKey, partAssignmentsBytes),
                                put(partChangeTriggerKey, ByteUtils.longToBytes(revision))
                        ).yield(PENDING_KEY_UPDATED),
                        iif(value(partAssignmentsPendingKey).ne(partAssignmentsBytes),
                                ops(
                                        put(partAssignmentsPlannedKey, partAssignmentsBytes),
                                        put(partChangeTriggerKey, ByteUtils.longToBytes(revision))
                                ).yield(PLANNED_KEY_UPDATED),
                                ops(remove(partAssignmentsPlannedKey)).yield(PLANNED_KEY_REMOVED))),
                ops().yield(OUTDATED_UPDATE_RECEIVED));

        return metaStorageMgr.invoke(iif).thenAccept(sr -> {
            switch (sr.getAsInt()) {
                case PENDING_KEY_UPDATED:
                    LOG.info(
                            "Update metastore pending partitions key [key={}, partition={}, table={}, newVal={}]",
                            partAssignmentsPendingKey.toString(), partNum, tableName,
                            ByteUtils.fromBytes(partAssignmentsBytes));

                    break;
                case PLANNED_KEY_UPDATED:
                    LOG.info(
                            "Update metastore planned partitions key [key={}, partition={}, table={}, newVal={}]",
                            partAssignmentsPlannedKey, partNum, tableName, ByteUtils.fromBytes(partAssignmentsBytes));

                    break;
                case PLANNED_KEY_REMOVED:
                    LOG.info(
                            "Remove planned key because current pending key has the same value [key={}, partition={}, table={}, val={}]",
                            partAssignmentsPlannedKey.toString(), partNum, tableName, ByteUtils.fromBytes(partAssignmentsBytes));

                    break;
                case OUTDATED_UPDATE_RECEIVED:
                    LOG.debug(
                            "Received outdated rebalance trigger event [revision={}, partition={}, table={}]",
                            revision, partNum, tableName);

                    break;
                default:
                    throw new IllegalStateException("Unknown return code for rebalance metastore multi-invoke");
            }
        });
    }

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
    public static UUID extractTableId(ByteArray key) {
        return extractTableId(key, "");
    }

    /**
     * Extract table id from a metastorage key of partition.
     *
     * @param key Key.
     * @param prefix Key prefix.
     * @return Table id.
     */
    public static UUID extractTableId(ByteArray key, String prefix) {
        String strKey = key.toString();

        return UUID.fromString(strKey.substring(prefix.length(), strKey.indexOf("_part_")));
    }

    /**
     * Extract partition number from the rebalance key of partition.
     *
     * @param key Key.
     * @return Partition number.
     */
    public static int extractPartitionNumber(ByteArray key) {
        var strKey = key.toString();

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
     * @param baselineNodes Baseline nodes.
     * @param replicas Replicas count.
     * @param partNum Number of the partition.
     * @param partId Partition's raft group id.
     * @param event Assignments switch reduce change event.
     * @return Completable future that signifies the completion of this operation.
     */
    public static CompletableFuture<Void> handleReduceChanged(MetaStorageManager metaStorageMgr, Collection<ClusterNode> baselineNodes,
            int replicas, int partNum, TablePartitionId partId, WatchEvent event) {
        Entry entry = event.entryEvent().newEntry();
        byte[] eventData = entry.value();

        Set<Assignment> switchReduce = ByteUtils.fromBytes(eventData);

        if (switchReduce.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        Set<Assignment> assignments = AffinityUtils.calculateAssignmentForPartition(baselineNodes, partNum, replicas);

        ByteArray pendingKey = pendingPartAssignmentsKey(partId);

        Set<Assignment> pendingAssignments = subtract(assignments, switchReduce);

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

        If resultingOperation = iif(
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
     * Removes nodes from set of nodes.
     *
     * @param minuend Set to remove nodes from.
     * @param subtrahend Set of nodes to be removed.
     * @return Result of the subtraction.
     */
    public static <T> Set<T> subtract(Set<T> minuend, Set<T> subtrahend) {
        return minuend.stream().filter(v -> !subtrahend.contains(v)).collect(Collectors.toSet());
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

    /**
     * Returns an intersection of two set of nodes.
     *
     * @param op1 First operand.
     * @param op2 Second operand.
     * @return Result of the intersection.
     */
    public static <T> Set<T> intersect(Set<T> op1, Set<T> op2) {
        return op1.stream().filter(op2::contains).collect(Collectors.toSet());
    }
}
