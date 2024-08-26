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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
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
import static org.apache.ignite.internal.util.ByteUtils.longToBytesKeepingOrder;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.replicator.TablePartitionId;
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
     * Status values for methods like
     * {@link #updatePendingAssignmentsKeys(CatalogTableDescriptor, TablePartitionId, Collection, int, long, MetaStorageManager, int, Set,
     * HybridTimestamp)}.
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
            int replicas,
            long revision,
            MetaStorageManager metaStorageMgr,
            int partNum,
            Set<Assignment> tableCfgPartAssignments,
            long assignmentsTimestamp
    ) {
        ByteArray partChangeTriggerKey = pendingChangeTriggerKey(partId);

        ByteArray partAssignmentsPendingKey = pendingPartAssignmentsKey(partId);

        ByteArray partAssignmentsPlannedKey = plannedPartAssignmentsKey(partId);

        ByteArray partAssignmentsStableKey = stablePartAssignmentsKey(partId);

        Set<Assignment> partAssignments = AffinityUtils.calculateAssignmentForPartition(dataNodes, partNum, replicas);

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
        //                message after the metastorage invoke:
        //                "Remove planned key because current pending key has the same value."
        //            else if empty(partition.assignments.pending)
        //                remove(partition.assignments.planned)
        //                message after the metastorage invoke:
        //                "Remove planned key because pending is empty and calculated assignments are equal to current assignments."
        //    else:
        //        skip

        Condition newAssignmentsCondition = exists(partAssignmentsStableKey).and(value(partAssignmentsStableKey).ne(partAssignmentsBytes));

        if (isNewAssignments) {
            newAssignmentsCondition = notExists(partAssignmentsStableKey).or(newAssignmentsCondition);
        }

        Iif iif = iif(or(notExists(partChangeTriggerKey), value(partChangeTriggerKey).lt(longToBytesKeepingOrder(revision))),
                iif(and(notExists(partAssignmentsPendingKey), newAssignmentsCondition),
                        ops(
                                put(partAssignmentsPendingKey, partAssignmentsBytes),
                                put(partChangeTriggerKey, longToBytesKeepingOrder(revision))
                        ).yield(PENDING_KEY_UPDATED.ordinal()),
                        iif(and(value(partAssignmentsPendingKey).ne(partAssignmentsBytes), exists(partAssignmentsPendingKey)),
                                ops(
                                        put(partAssignmentsPlannedKey, partAssignmentsBytes),
                                        put(partChangeTriggerKey, longToBytesKeepingOrder(revision))
                                ).yield(PLANNED_KEY_UPDATED.ordinal()),
                                iif(value(partAssignmentsPendingKey).eq(partAssignmentsBytes),
                                        ops(remove(partAssignmentsPlannedKey)).yield(PLANNED_KEY_REMOVED_EQUALS_PENDING.ordinal()),
                                        iif(notExists(partAssignmentsPendingKey),
                                                ops(remove(partAssignmentsPlannedKey)).yield(PLANNED_KEY_REMOVED_EMPTY_PENDING.ordinal()),
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
    public static CompletableFuture<?>[] triggerAllTablePartitionsRebalance(
            CatalogTableDescriptor tableDescriptor,
            CatalogZoneDescriptor zoneDescriptor,
            Set<String> dataNodes,
            long storageRevision,
            MetaStorageManager metaStorageManager,
            long assignmentsTimestamp
    ) {
        CompletableFuture<Map<Integer, Assignments>> tableAssignmentsFut = tableAssignments(
                metaStorageManager,
                tableDescriptor.id(),
                Set.of(),
                zoneDescriptor.partitions()
        );

        CompletableFuture<?>[] futures = new CompletableFuture[zoneDescriptor.partitions()];

        for (int partId = 0; partId < zoneDescriptor.partitions(); partId++) {
            TablePartitionId replicaGrpId = new TablePartitionId(tableDescriptor.id(), partId);

            int finalPartId = partId;

            futures[partId] = tableAssignmentsFut.thenCompose(tableAssignments ->
                    // TODO https://issues.apache.org/jira/browse/IGNITE-19763 We should distinguish empty stable assignments on
                    // TODO node recovery in case of interrupted table creation, and moving from empty assignments to non-empty.
                    tableAssignments.isEmpty() ? nullCompletedFuture() : updatePendingAssignmentsKeys(
                            tableDescriptor,
                            replicaGrpId,
                            dataNodes,
                            zoneDescriptor.replicas(),
                            storageRevision,
                            metaStorageManager,
                            finalPartId,
                            tableAssignments.get(finalPartId).nodes(),
                            assignmentsTimestamp
                    ));
        }

        return futures;
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
     * Key that is needed for skipping stale events of pending key change.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalance documentation</a>
     */
    public static ByteArray pendingChangeTriggerKey(TablePartitionId partId) {
        return new ByteArray(partId + "pending.change.trigger");
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
        return new ByteArray("assignments.planned." + partId);
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
     * Extract table id from a metastorage key of partition.
     *
     * @param key Key.
     * @param prefix Key prefix.
     * @return Table id.
     */
    public static int extractZoneId(byte[] key, String prefix) {
        String strKey = new String(key, StandardCharsets.UTF_8);

        return Integer.parseInt(strKey.substring(prefix.length()));
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
        Entry entry = metaStorageManager.getLocally(stablePartAssignmentsKey(new TablePartitionId(tableId, partitionNumber)), revision);

        return (entry == null || entry.empty() || entry.tombstone()) ? null : Assignments.fromBytes(entry.value()).nodes();
    }

    /**
     * Returns table assignments for table partitions from meta storage.
     *
     * @param metaStorageManager Meta storage manager.
     * @param tableId Table id.
     * @param partitionIds IDs of partitions to get assignments for. If empty, get all partition assignments.
     * @param numberOfPartitions Number of partitions. Ignored if partition IDs are specified.
     * @return Future with table assignments as a value.
     */
    public static CompletableFuture<Map<Integer, Assignments>> tableAssignments(
            MetaStorageManager metaStorageManager,
            int tableId,
            Set<Integer> partitionIds,
            int numberOfPartitions
    ) {
        IntStream partitionIdsStream = partitionIds.isEmpty()
                ? IntStream.range(0, numberOfPartitions)
                : partitionIds.stream().mapToInt(Integer::intValue);

        Map<ByteArray, Integer> partitionKeysToPartitionNumber = partitionIdsStream.collect(
                HashMap::new,
                (map, partId) -> map.put(stablePartAssignmentsKey(new TablePartitionId(tableId, partId)), partId),
                Map::putAll
        );

        return metaStorageManager.getAll(partitionKeysToPartitionNumber.keySet())
                .thenApply(entries -> {
                    if (entries.isEmpty()) {
                        return Map.of();
                    }

                    Map<Integer, Assignments> result = new HashMap<>();
                    int numberOfMsPartitions = 0;

                    for (var mapEntry : entries.entrySet()) {
                        Entry entry = mapEntry.getValue();

                        if (!entry.empty() && !entry.tombstone()) {
                            result.put(partitionKeysToPartitionNumber.get(mapEntry.getKey()), Assignments.fromBytes(entry.value()));
                            numberOfMsPartitions++;
                        }
                    }

                    assert numberOfMsPartitions == 0 || numberOfMsPartitions == entries.size()
                            : "Invalid number of stable partition entries received from meta storage [received="
                            + numberOfMsPartitions + ", numberOfPartitions=" + entries.size() + ", tableId=" + tableId + "].";

                    return numberOfMsPartitions == 0 ? Map.of() : result;
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
                    Entry e = metaStorageManager.getLocally(stablePartAssignmentsKey(new TablePartitionId(tableId, p)), revision);

                    assert e != null && !e.empty() && !e.tombstone() : e;

                    return Assignments.fromBytes(e.value());
                })
                .collect(toList());
    }
}
