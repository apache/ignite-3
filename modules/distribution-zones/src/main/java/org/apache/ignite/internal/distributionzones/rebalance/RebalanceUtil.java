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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.rebalance.AssignmentUtil.metastoreAssignments;
import static org.apache.ignite.internal.distributionzones.rebalance.AssignmentUtil.partitionIds;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.UpdateStatus.ASSIGNMENT_NOT_UPDATED;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.UpdateStatus.OUTDATED_UPDATE_RECEIVED;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.UpdateStatus.PENDING_KEY_UPDATED;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.UpdateStatus.PLANNED_KEY_REMOVED_EMPTY_PENDING;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.UpdateStatus.PLANNED_KEY_REMOVED_EQUALS_PENDING;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.UpdateStatus.PLANNED_KEY_UPDATED;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.and;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.exists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.util.ByteUtils.longToBytesKeepingOrder;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.StringUtils.toStringWithoutPrefix;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsChain;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Util class for methods needed for the rebalance process.
 * TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this class and use {@link ZoneRebalanceUtil} instead
 *  after switching to zone-based replication.
 */
public class RebalanceUtil {

    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RebalanceUtil.class);

    /**
     * Status values for methods like {@link #updatePendingAssignmentsKeys}.
     */
    public enum UpdateStatus {
        /**
         * Return code of metastore multi-invoke which identifies,
         * that pending key was updated to new value (i.e. there is no active rebalance at the moment of call).
         */
        PENDING_KEY_UPDATED,

        /**
         * Return code of metastore multi-invoke which identifies,
         * that planned key was updated to new value (i.e. there is an active rebalance at the moment of call).
         */
        PLANNED_KEY_UPDATED,

        /**
         * Return code of metastore multi-invoke which identifies,
         * that planned key was removed, because current rebalance is already have the same target.
         */
        PLANNED_KEY_REMOVED_EQUALS_PENDING,

        /**
         * Return code of metastore multi-invoke which identifies,
         * that planned key was removed, because current assignment is empty.
         */
        PLANNED_KEY_REMOVED_EMPTY_PENDING,

        /**
         * Return code of metastore multi-invoke which identifies,
         * that assignments do not need to be updated.
         */
        ASSIGNMENT_NOT_UPDATED,

        /**
         * Return code of metastore multi-invoke which identifies,
         * that this trigger event was already processed by another node and must be skipped.
         */
        OUTDATED_UPDATE_RECEIVED;

        private static final UpdateStatus[] VALUES = values();

        public static UpdateStatus valueOf(int ordinal) {
            return VALUES[ordinal];
        }
    }

    /** Rebalance scheduler pool size. */
    public static final int REBALANCE_SCHEDULER_POOL_SIZE = Math.min(Runtime.getRuntime().availableProcessors() * 3, 20);

    /**
     * Update keys that related to rebalance algorithm in Meta Storage. Keys are specific for partition.
     *
     * @param tableDescriptor Table descriptor.
     * @param partId Unique identifier of a partition.
     * @param dataNodes Data nodes.
     * @param partitions Number of partitions.
     * @param replicas Number of replicas for a table.
     * @param revision Revision of Meta Storage that is specific for the assignment update.
     * @param metaStorageMgr Meta Storage manager.
     * @param partNum Partition id.
     * @param tableCfgPartAssignments Table configuration assignments.
     * @return Future representing result of updating keys in {@code metaStorageMgr}
     */
    public static CompletableFuture<Void> updatePendingAssignmentsKeys(
            CatalogTableDescriptor tableDescriptor,
            TablePartitionId partId,
            Collection<String> dataNodes,
            int partitions,
            int replicas,
            long revision,
            MetaStorageManager metaStorageMgr,
            int partNum,
            Set<Assignment> tableCfgPartAssignments,
            long assignmentsTimestamp,
            Set<String> aliveNodes,
            ConsistencyMode consistencyMode
    ) {
        ByteArray partChangeTriggerKey = pendingChangeTriggerKey(partId);

        ByteArray partAssignmentsPendingKey = pendingPartAssignmentsKey(partId);

        ByteArray partAssignmentsPlannedKey = plannedPartAssignmentsKey(partId);

        ByteArray partAssignmentsStableKey = stablePartAssignmentsKey(partId);

        Set<Assignment> calculatedAssignments = calculateAssignmentForPartition(dataNodes, partNum, partitions, replicas);

        Set<Assignment> partAssignments;

        if (consistencyMode == ConsistencyMode.HIGH_AVAILABILITY) {
            // All complicated logic here is needed because we want to return back to stable nodes
            // that are returned back after majority is lost and stable was narrowed.
            // Let's consider example:
            // stable = [A, B, C], dataNodes = [A, B, C]
            // B, C left, stable = [A], dataNodes = [A, B, C]
            // B returned, we want stable = [A, B], but in terms of data nodes they are not changed and equal [A, B, C]
            // So, because scale up mechanism in this case won't adjust stable, we need to add B to stable manually.
            // General idea is to filter offline nodes from data nodes, but we need to be careful and do not remove nodes
            // bypassing scale down mechanism. If node is offline and presented in previous stable, we won't remove that node.

            // First of all, we remove offline nodes from calculated assignments
            Set<Assignment> resultingAssignments = calculatedAssignments
                    .stream()
                    .filter(a -> aliveNodes.contains(a.consistentId()))
                    .collect(toSet());

            // Here we re-introduce nodes that currently exist in the stable configuration
            // but were previously removed without using the normal scale-down process.
            for (Assignment assignment : tableCfgPartAssignments) {
                if (calculatedAssignments.contains(assignment)) {
                    resultingAssignments.add(assignment);
                }
            }

            partAssignments = resultingAssignments;
        } else {
            partAssignments = calculatedAssignments;
        }

        boolean isNewAssignments = !tableCfgPartAssignments.equals(partAssignments);

        byte[] partAssignmentsBytes = Assignments.toBytes(partAssignments, assignmentsTimestamp);

        //    if empty(partition.change.trigger.revision) || partition.change.trigger.revision < event.revision:
        //        if empty(partition.assignments.pending)
        //              && ((isNewAssignments && empty(partition.assignments.stable))
        //                  || (partition.assignments.stable != calcPartAssignments() && !empty(partition.assignments.stable))):
        //            partition.assignments.pending = calcPartAssignments()
        //            partition.change.trigger.revision = event.revision
        //        else:
        //            if partition.assignments.pending != calcPartAssignments && !empty(partition.assignments.pending)
        //                partition.assignments.planned = calcPartAssignments()
        //                partition.change.trigger.revision = event.revision
        //            else if partition.assignments.pending == calcPartAssignments
        //                remove(partition.assignments.planned)
        //                partition.change.trigger.revision = event.revision
        //                message after the metastorage invoke:
        //                "Remove planned key because current pending key has the same value."
        //            else if empty(partition.assignments.pending)
        //                remove(partition.assignments.planned)
        //                partition.change.trigger.revision = event.revision
        //                message after the metastorage invoke:
        //                "Remove planned key because pending is empty and calculated assignments are equal to current assignments."
        //    else:
        //        skip

        Condition newAssignmentsCondition = exists(partAssignmentsStableKey).and(value(partAssignmentsStableKey).ne(partAssignmentsBytes));

        if (isNewAssignments) {
            newAssignmentsCondition = notExists(partAssignmentsStableKey).or(newAssignmentsCondition);
        }

        byte[] revisionBytes = longToBytesKeepingOrder(revision);

        Iif iif = iif(or(notExists(partChangeTriggerKey), value(partChangeTriggerKey).lt(revisionBytes)),
                iif(and(notExists(partAssignmentsPendingKey), newAssignmentsCondition),
                        ops(
                                put(partAssignmentsPendingKey, partAssignmentsBytes),
                                put(partChangeTriggerKey, revisionBytes)
                        ).yield(PENDING_KEY_UPDATED.ordinal()),
                        iif(and(value(partAssignmentsPendingKey).ne(partAssignmentsBytes), exists(partAssignmentsPendingKey)),
                                ops(
                                        put(partAssignmentsPlannedKey, partAssignmentsBytes),
                                        put(partChangeTriggerKey, revisionBytes)
                                ).yield(PLANNED_KEY_UPDATED.ordinal()),
                                iif(value(partAssignmentsPendingKey).eq(partAssignmentsBytes),
                                        ops(
                                                remove(partAssignmentsPlannedKey),
                                                put(partChangeTriggerKey, revisionBytes)
                                        ).yield(PLANNED_KEY_REMOVED_EQUALS_PENDING.ordinal()),
                                        iif(notExists(partAssignmentsPendingKey),
                                                ops(
                                                        remove(partAssignmentsPlannedKey),
                                                        put(partChangeTriggerKey, revisionBytes)
                                                ).yield(PLANNED_KEY_REMOVED_EMPTY_PENDING.ordinal()),
                                                ops().yield(ASSIGNMENT_NOT_UPDATED.ordinal()))
                                ))),
                ops().yield(OUTDATED_UPDATE_RECEIVED.ordinal()));

        return metaStorageMgr.invoke(iif).thenAccept(sr -> {
            switch (UpdateStatus.valueOf(sr.getAsInt())) {
                case PENDING_KEY_UPDATED:
                    LOG.info(
                            "Update metastore pending partitions key [key={}, partition={}, table={}/{}, newVal={}]",
                            partAssignmentsPendingKey.toString(), partNum, tableDescriptor.id(), tableDescriptor.name(),
                            partAssignments);

                    break;
                case PLANNED_KEY_UPDATED:
                    LOG.info(
                            "Update metastore planned partitions key [key={}, partition={}, table={}/{}, newVal={}]",
                            partAssignmentsPlannedKey, partNum, tableDescriptor.id(), tableDescriptor.name(),
                            partAssignments
                    );

                    break;
                case PLANNED_KEY_REMOVED_EQUALS_PENDING:
                    LOG.info(
                            "Remove planned key because current pending key has the same value [key={}, partition={}, table={}/{}, val={}]",
                            partAssignmentsPlannedKey.toString(), partNum, tableDescriptor.id(), tableDescriptor.name(),
                            partAssignments
                    );

                    break;
                case PLANNED_KEY_REMOVED_EMPTY_PENDING:
                    LOG.info(
                            "Remove planned key because pending is empty and calculated assignments are equal to current assignments "
                                    + "[key={}, partition={}, table={}/{}, val={}]",
                            partAssignmentsPlannedKey.toString(), partNum, tableDescriptor.id(), tableDescriptor.name(),
                            partAssignments
                    );

                    break;
                case ASSIGNMENT_NOT_UPDATED:
                    LOG.debug(
                            "Assignments are not updated [key={}, partition={}, table={}/{}, val={}]",
                            partAssignmentsPlannedKey.toString(), partNum, tableDescriptor.id(), tableDescriptor.name(),
                            partAssignments
                    );

                    break;
                case OUTDATED_UPDATE_RECEIVED:
                    LOG.debug(
                            "Received outdated rebalance trigger event [revision={}, partition={}, table={}/{}]",
                            revision, partNum, tableDescriptor.id(), tableDescriptor.name());

                    break;
                default:
                    throw new IllegalStateException("Unknown return code for rebalance metastore multi-invoke");
            }
        });
    }

    /**
     * Triggers rebalance on all partitions of the provided table: that is, reads table assignments from
     * the MetaStorage, computes new ones based on the current properties of the table, its zone and the
     * provided data nodes, and, if the calculated assignments are different from the ones loaded from the
     * MetaStorages, writes them as pending assignments.
     *
     * @param tableDescriptor Table descriptor.
     * @param zoneDescriptor Zone descriptor.
     * @param dataNodes Data nodes to use.
     * @param storageRevision MetaStorage revision corresponding to this request.
     * @param metaStorageManager MetaStorage manager used to read/write assignments.
     * @return Array of futures, one per partition of the table; the futures complete when the described
     *     rebalance triggering completes.
     */
    public static CompletableFuture<Void> triggerAllTablePartitionsRebalance(
            CatalogTableDescriptor tableDescriptor,
            CatalogZoneDescriptor zoneDescriptor,
            Set<String> dataNodes,
            long storageRevision,
            MetaStorageManager metaStorageManager,
            long assignmentsTimestamp,
            Set<String> aliveNodes
    ) {
        int[] partitionIds = partitionIds(zoneDescriptor.partitions());

        return tableStableAssignments(metaStorageManager, tableDescriptor.id(), partitionIds)
                .thenCompose(stableAssignments -> {
                    if (stableAssignments.isEmpty()) {
                        return nullCompletedFuture();
                    }

                    return tablePartitionAssignment(
                            tableDescriptor,
                            zoneDescriptor,
                            dataNodes,
                            storageRevision,
                            metaStorageManager,
                            assignmentsTimestamp,
                            stableAssignments,
                            aliveNodes
                    );
                });
    }

    private static CompletableFuture<Void> tablePartitionAssignment(
            CatalogTableDescriptor tableDescriptor,
            CatalogZoneDescriptor zoneDescriptor,
            Set<String> dataNodes,
            long storageRevision,
            MetaStorageManager metaStorageManager,
            long assignmentsTimestamp,
            Map<Integer, Assignments> tableAssignments,
            Set<String> aliveNodes
    ) {
        // tableAssignments should not be empty. It is checked for emptiness before calling this method.
        CompletableFuture<?>[] futures = new CompletableFuture[zoneDescriptor.partitions()];

        for (int partId = 0; partId < zoneDescriptor.partitions(); partId++) {
            TablePartitionId replicaGrpId = new TablePartitionId(tableDescriptor.id(), partId);

            // TODO https://issues.apache.org/jira/browse/IGNITE-19763 We should distinguish empty stable assignments on
            // TODO node recovery in case of interrupted table creation, and moving from empty assignments to non-empty.
            futures[partId] = updatePendingAssignmentsKeys(
                    tableDescriptor,
                    replicaGrpId,
                    dataNodes,
                    zoneDescriptor.partitions(),
                    zoneDescriptor.replicas(),
                    storageRevision,
                    metaStorageManager,
                    partId,
                    tableAssignments.get(partId).nodes(),
                    assignmentsTimestamp,
                    aliveNodes,
                    zoneDescriptor.consistencyMode()
            );
        }

        // This set is used to deduplicate exceptions (if there is an exception from upstream, for instance,
        // when reading from MetaStorage, it will be encountered by every partition future) to avoid noise
        // in the logs.
        Set<Throwable> unwrappedCauses = ConcurrentHashMap.newKeySet();

        for (int partId = 0; partId < futures.length; partId++) {
            int finalPartId = partId;

            futures[partId].exceptionally(e -> {
                Throwable cause = ExceptionUtils.unwrapCause(e);

                if (unwrappedCauses.add(cause)) {
                    // The exception is specific to this partition.
                    LOG.error(
                            "Exception on updating assignments for [tableId={}, name={}, partition={}]",
                            e,
                            tableDescriptor.id(), tableDescriptor.name(), finalPartId
                    );
                } else {
                    // The exception is from upstream and not specific for this partition, so don't log the partition index.
                    LOG.error(
                            "Exception on updating assignments for [tableId={}, name={}]",
                            e,
                            tableDescriptor.id(), tableDescriptor.name()
                    );
                }

                return null;
            });
        }

        return allOf(futures);
    }

    /** Key prefix for planned assignments. */
    public static final String PLANNED_ASSIGNMENTS_PREFIX = "assignments.planned.";

    /** Key prefix for pending assignments. */
    public static final String PENDING_ASSIGNMENTS_PREFIX = "assignments.pending.";

    public static final byte[] PENDING_ASSIGNMENTS_PREFIX_BYTES = "assignments.pending.".getBytes(UTF_8);

    /** Key prefix for stable assignments. */
    public static final String STABLE_ASSIGNMENTS_PREFIX = "assignments.stable.";

    public static final byte[] STABLE_ASSIGNMENTS_PREFIX_BYTES = STABLE_ASSIGNMENTS_PREFIX.getBytes(UTF_8);

    /** Key prefix for switch reduce assignments. */
    public static final String ASSIGNMENTS_SWITCH_REDUCE_PREFIX = "assignments.switch.reduce.";

    public static final byte[] ASSIGNMENTS_SWITCH_REDUCE_PREFIX_BYTES = ASSIGNMENTS_SWITCH_REDUCE_PREFIX.getBytes(UTF_8);

    /** Key prefix for switch append assignments. */
    public static final String ASSIGNMENTS_SWITCH_APPEND_PREFIX = "assignments.switch.append.";

    /** Key prefix for change trigger keys. */
    public static final String PENDING_CHANGE_TRIGGER_PREFIX = "pending.change.trigger.";

    public static final byte[] PENDING_CHANGE_TRIGGER_PREFIX_BYTES = PENDING_CHANGE_TRIGGER_PREFIX.getBytes(UTF_8);

    public static final String ASSIGNMENTS_CHAIN_PREFIX = "assignments.chain.";

    public static final byte[] ASSIGNMENTS_CHAIN_PREFIX_BYTES = ASSIGNMENTS_CHAIN_PREFIX.getBytes(UTF_8);

    /**
     * Key that is needed for skipping stale events of pending key change.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalance documentation</a>
     */
    public static ByteArray pendingChangeTriggerKey(TablePartitionId partId) {
        return new ByteArray(PENDING_CHANGE_TRIGGER_PREFIX + partId);
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalance documentation</a>
     */
    public static ByteArray pendingPartAssignmentsKey(TablePartitionId partId) {
        return new ByteArray(PENDING_ASSIGNMENTS_PREFIX + partId);
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalance documentation</a>
     */
    public static ByteArray plannedPartAssignmentsKey(TablePartitionId partId) {
        return new ByteArray(PLANNED_ASSIGNMENTS_PREFIX + partId);
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalance documentation</a>
     */
    public static ByteArray stablePartAssignmentsKey(TablePartitionId partId) {
        return new ByteArray(STABLE_ASSIGNMENTS_PREFIX + partId);
    }

    /**
     * Key for the graceful restart in HA mode.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-131%3A+Partition+Majority+Unavailability+Handling">HA mode</a>
     */
    public static ByteArray assignmentsChainKey(TablePartitionId partId) {
        return new ByteArray(ASSIGNMENTS_CHAIN_PREFIX + partId);
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalance documentation</a>
     */
    public static ByteArray switchReduceKey(TablePartitionId partId) {
        return new ByteArray(ASSIGNMENTS_SWITCH_REDUCE_PREFIX + partId);
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalance documentation</a>
     */
    public static ByteArray switchAppendKey(TablePartitionId partId) {
        return new ByteArray(ASSIGNMENTS_SWITCH_APPEND_PREFIX + partId);
    }

    /**
     * Converts the given {@code key}, stripping it off the given {@code prefix}, into a {@link TablePartitionId}.
     *
     * @param key Metastorage key.
     * @param prefix Key prefix.
     * @return {@link TablePartitionId} that was encoded in the key.
     */
    public static TablePartitionId extractTablePartitionId(byte[] key, byte[] prefix) {
        var tablePartitionIdString = toStringWithoutPrefix(key, prefix.length);

        return TablePartitionId.fromString(tablePartitionIdString);
    }

    /**
     * Extract table id from a metastorage key of partition.
     *
     * @param key Key.
     * @param prefix Key prefix.
     * @return Table id.
     */
    public static int extractZoneId(byte[] key, byte[] prefix) {
        return Integer.parseInt(toStringWithoutPrefix(key, prefix.length));
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
     * Removes nodes from set of nodes.
     *
     * @param minuend Set to remove nodes from.
     * @param subtrahend Set of nodes to be removed.
     * @return Result of the subtraction.
     */
    public static <T> Set<T> subtract(Set<T> minuend, Set<T> subtrahend) {
        return minuend.stream().filter(v -> !subtrahend.contains(v)).collect(toSet());
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
        return op1.stream().filter(op2::contains).collect(toSet());
    }

    /**
     * Returns partition assignments from meta storage.
     *
     * @param metaStorageManager Meta storage manager.
     * @param tableId Table ID.
     * @param partitionId Partition ID.
     * @return Future with partition assignments as a value.
     */
    public static CompletableFuture<Set<Assignment>> partitionAssignments(
            MetaStorageManager metaStorageManager,
            int tableId,
            int partitionId
    ) {
        return metaStorageManager
                .get(stablePartAssignmentsKey(new TablePartitionId(tableId, partitionId)))
                .thenApply(e -> (e.value() == null) ? null : Assignments.fromBytes(e.value()).nodes());
    }

    /**
     * Returns partition assignments from meta storage locally.
     *
     * @param metaStorageManager Meta storage manager.
     * @param tablePartitionId Table partition id.
     * @param revision Revision.
     * @return Returns partition assignments from meta storage locally or {@code null} if assignments is absent.
     */
    public static @Nullable Assignments stableAssignmentsGetLocally(
            MetaStorageManager metaStorageManager,
            TablePartitionId tablePartitionId,
            long revision
    ) {
        Entry entry = metaStorageManager.getLocally(stablePartAssignmentsKey(tablePartitionId), revision);

        return (entry == null || entry.empty() || entry.tombstone()) ? null : Assignments.fromBytes(entry.value());
    }

    /**
     * Returns partition assignments from meta storage locally.
     *
     * @param metaStorageManager Meta storage manager.
     * @param tableId Table id.
     * @param partitionNumber Partition number.
     * @param revision Revision.
     * @return Returns partition assignments from meta storage locally or {@code null} if assignments is absent.
     */
    @Nullable
    public static Set<Assignment> partitionAssignmentsGetLocally(
            MetaStorageManager metaStorageManager,
            int tableId,
            int partitionNumber,
            long revision
    ) {
        Assignments assignments = stableAssignmentsGetLocally(metaStorageManager, new TablePartitionId(tableId, partitionNumber), revision);

        return assignments == null ? null : assignments.nodes();
    }

    /**
     * Returns stable table assignments for table partitions from meta storage.
     *
     * @param metaStorageManager Meta storage manager.
     * @param tableId Table id.
     * @param partitionIds IDs of partitions to get assignments for.
     * @return Future with table assignments as a value.
     */
    public static CompletableFuture<Map<Integer, Assignments>> tableStableAssignments(
            MetaStorageManager metaStorageManager,
            int tableId,
            int[] partitionIds
    ) {
        return metastoreAssignments(
                metaStorageManager,
                partitionIds,
                partitionId -> stablePartAssignmentsKey(new TablePartitionId(tableId, partitionId))
        ).whenComplete((assignmentsMap, throwable) -> {
            if (throwable == null) {
                int numberOfMsPartitions = assignmentsMap.size();

                assert numberOfMsPartitions == 0 || numberOfMsPartitions == partitionIds.length
                        : "Invalid number of partition entries received from meta storage [received="
                        + numberOfMsPartitions + ", numberOfPartitions=" + partitionIds.length + ", tableId=" + tableId + "].";
            }
        });
    }

    /**
     * Returns table assignments for all table partitions from meta storage locally. Assignments must be present.
     *
     * @param metaStorageManager Meta storage manager.
     * @param tableId Table id.
     * @param numberOfPartitions Number of partitions.
     * @param revision Revision.
     * @return Future with table assignments as a value.
     */
    public static List<Assignments> tableAssignmentsGetLocally(
            MetaStorageManager metaStorageManager,
            int tableId,
            int numberOfPartitions,
            long revision
    ) {
        return IntStream.range(0, numberOfPartitions)
                .mapToObj(p -> {
                    Assignments assignments = stableAssignmentsGetLocally(metaStorageManager, new TablePartitionId(tableId, p), revision);

                    assert assignments != null;

                    return assignments;
                })
                .collect(toList());
    }

    /**
     * Returns table pending assignments for all table partitions from meta storage locally.
     *
     * @param metaStorageManager Meta storage manager.
     * @param tableId Table id.
     * @param numberOfPartitions Number of partitions.
     * @param revision Revision.
     * @return Future with table assignments as a value.
     */
    public static List<Assignments> tablePendingAssignmentsGetLocally(
            MetaStorageManager metaStorageManager,
            int tableId,
            int numberOfPartitions,
            long revision
    ) {
        return IntStream.range(0, numberOfPartitions)
                .mapToObj(p -> {
                    Entry e = metaStorageManager.getLocally(pendingPartAssignmentsKey(new TablePartitionId(tableId, p)), revision);

                    return e != null ? Assignments.fromBytes(e.value()) : null;
                })
                .collect(toList());
    }

    /**
     * Returns assignments chains for all table partitions from meta storage locally.
     *
     * @param metaStorageManager Meta storage manager.
     * @param tableId Table id.
     * @param numberOfPartitions Number of partitions.
     * @param revision Revision.
     * @return Future with table assignments as a value.
     */
    public static List<AssignmentsChain> tableAssignmentsChainGetLocally(
            MetaStorageManager metaStorageManager,
            int tableId,
            int numberOfPartitions,
            long revision
    ) {
        return IntStream.range(0, numberOfPartitions)
                .mapToObj(p -> assignmentsChainGetLocally(metaStorageManager, new TablePartitionId(tableId, p), revision))
                .collect(toList());
    }

    /**
     * Returns assignments chain from meta storage locally.
     *
     * @param metaStorageManager Meta storage manager.
     * @param tablePartitionId Table partition id.
     * @param revision Revision.
     * @return Returns assignments chain from meta storage locally or {@code null} if assignments is absent.
     */
    public static @Nullable AssignmentsChain assignmentsChainGetLocally(
            MetaStorageManager metaStorageManager,
            TablePartitionId tablePartitionId,
            long revision
    ) {
        Entry e = metaStorageManager.getLocally(assignmentsChainKey(tablePartitionId), revision);

        return e != null ? AssignmentsChain.fromBytes(e.value()) : null;
    }
}
