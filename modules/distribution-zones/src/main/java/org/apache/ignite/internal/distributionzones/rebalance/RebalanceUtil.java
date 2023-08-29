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

import static java.util.Collections.emptySet;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.and;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.exists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;

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
    private static final int PLANNED_KEY_REMOVED_EQUALS_PENDING = 2;

    /** Return code of metastore multi-invoke which identifies,
     * that planned key was removed, because current assignment is empty.
     */
    private static final int PLANNED_KEY_REMOVED_EMPTY_PENDING = 3;

    /** Return code of metastore multi-invoke which identifies,
     * that assignments do not need to be updated.
     */
    private static final int ASSIGNMENT_NOT_UPDATED = 4;

    /** Return code of metastore multi-invoke which identifies,
     * that this trigger event was already processed by another node and must be skipped.
     */
    private static final int OUTDATED_UPDATE_RECEIVED = 5;

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
            Set<Assignment> tableCfgPartAssignments
    ) {
        ByteArray partChangeTriggerKey = partChangeTriggerKey(partId);

        ByteArray partAssignmentsPendingKey = pendingPartAssignmentsKey(partId);

        ByteArray partAssignmentsPlannedKey = plannedPartAssignmentsKey(partId);

        ByteArray partAssignmentsStableKey = stablePartAssignmentsKey(partId);

        Set<Assignment> partAssignments = AffinityUtils.calculateAssignmentForPartition(dataNodes, partNum, replicas);

        boolean isNewAssignments = !tableCfgPartAssignments.equals(partAssignments);

        byte[] partAssignmentsBytes = ByteUtils.toBytes(partAssignments);

        //    if empty(partition.change.trigger.revision) || partition.change.trigger.revision < event.revision:
        //        if empty(partition.assignments.pending)
        //              && ((isNewAssignments && empty(partition.assignments.stable))
        //                  || (partition.assignments.stable != calcPartAssighments() && !empty(partition.assignments.stable))):
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

        Condition newAssignmentsCondition;

        if (isNewAssignments) {
            newAssignmentsCondition = or(
                    notExists(partAssignmentsStableKey),
                    and(value(partAssignmentsStableKey).ne(partAssignmentsBytes), exists(partAssignmentsStableKey))
            );
        } else {
            newAssignmentsCondition = and(value(partAssignmentsStableKey).ne(partAssignmentsBytes), exists(partAssignmentsStableKey));
        }

        var iif = iif(or(notExists(partChangeTriggerKey), value(partChangeTriggerKey).lt(ByteUtils.longToBytes(revision))),
                iif(and(notExists(partAssignmentsPendingKey), newAssignmentsCondition),
                        ops(
                                put(partAssignmentsPendingKey, partAssignmentsBytes),
                                put(partChangeTriggerKey, ByteUtils.longToBytes(revision))
                        ).yield(PENDING_KEY_UPDATED),
                        iif(and(value(partAssignmentsPendingKey).ne(partAssignmentsBytes), exists(partAssignmentsPendingKey)),
                                ops(
                                        put(partAssignmentsPlannedKey, partAssignmentsBytes),
                                        put(partChangeTriggerKey, ByteUtils.longToBytes(revision))
                                ).yield(PLANNED_KEY_UPDATED),
                                iif(value(partAssignmentsPendingKey).eq(partAssignmentsBytes),
                                        ops(remove(partAssignmentsPlannedKey)).yield(PLANNED_KEY_REMOVED_EQUALS_PENDING),
                                        iif(notExists(partAssignmentsPendingKey),
                                                ops(remove(partAssignmentsPlannedKey)).yield(PLANNED_KEY_REMOVED_EMPTY_PENDING),
                                                ops().yield(ASSIGNMENT_NOT_UPDATED))
                                ))),
                ops().yield(OUTDATED_UPDATE_RECEIVED));

        return metaStorageMgr.invoke(iif).thenAccept(sr -> {
            switch (sr.getAsInt()) {
                case PENDING_KEY_UPDATED:
                    LOG.info(
                            "Update metastore pending partitions key [key={}, partition={}, table={}/{}, newVal={}]",
                            partAssignmentsPendingKey.toString(), partNum, tableDescriptor.id(), tableDescriptor.name(),
                            ByteUtils.fromBytes(partAssignmentsBytes));

                    break;
                case PLANNED_KEY_UPDATED:
                    LOG.info(
                            "Update metastore planned partitions key [key={}, partition={}, table={}/{}, newVal={}]",
                            partAssignmentsPlannedKey, partNum, tableDescriptor.id(), tableDescriptor.name(),
                            ByteUtils.fromBytes(partAssignmentsBytes)
                    );

                    break;
                case PLANNED_KEY_REMOVED_EQUALS_PENDING:
                    LOG.info(
                            "Remove planned key because current pending key has the same value [key={}, partition={}, table={}/{}, val={}]",
                            partAssignmentsPlannedKey.toString(), partNum, tableDescriptor.id(), tableDescriptor.name(),
                            ByteUtils.fromBytes(partAssignmentsBytes)
                    );

                    break;
                case PLANNED_KEY_REMOVED_EMPTY_PENDING:
                    LOG.info(
                            "Remove planned key because pending is empty and calculated assignments are equal to current assignments "
                                    + "[key={}, partition={}, table={}/{}, val={}]",
                            partAssignmentsPlannedKey.toString(), partNum, tableDescriptor.id(), tableDescriptor.name(),
                            ByteUtils.fromBytes(partAssignmentsBytes)
                    );

                    break;
                case ASSIGNMENT_NOT_UPDATED:
                    LOG.debug(
                            "Assignments are not updated [key={}, partition={}, table={}/{}, val={}]",
                            partAssignmentsPlannedKey.toString(), partNum, tableDescriptor.id(), tableDescriptor.name(),
                            ByteUtils.fromBytes(partAssignmentsBytes)
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
            MetaStorageManager metaStorageManager
    ) {
        CompletableFuture<List<Set<Assignment>>> tableAssignmentsFut = tableAssignments(
                metaStorageManager,
                tableDescriptor.id(),
                zoneDescriptor.partitions()
        );

        CompletableFuture<?>[] futures = new CompletableFuture[zoneDescriptor.partitions()];

        for (int partId = 0; partId < zoneDescriptor.partitions(); partId++) {
            TablePartitionId replicaGrpId = new TablePartitionId(tableDescriptor.id(), partId);

            int finalPartId = partId;

            futures[partId] = tableAssignmentsFut.thenCompose(tableAssignments ->
                    updatePendingAssignmentsKeys(
                            tableDescriptor,
                            replicaGrpId,
                            dataNodes,
                            zoneDescriptor.replicas(),
                            storageRevision,
                            metaStorageManager,
                            finalPartId,
                            tableAssignments.isEmpty() ? emptySet() : tableAssignments.get(finalPartId)
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
    public static UUID extractTableId(byte[] key) {
        return extractTableId(key, "");
    }

    /**
     * Extract table id from a metastorage key of partition.
     *
     * @param key Key.
     * @param prefix Key prefix.
     * @return Table id.
     */
    public static UUID extractTableId(byte[] key, String prefix) {
        String strKey = new String(key, StandardCharsets.UTF_8);

        return UUID.fromString(strKey.substring(prefix.length(), strKey.indexOf("_part_")));
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

    /**
     * Returns partition assignments from meta storage.
     *
     * @param metaStorageManager Meta storage manager.
     * @param tableId Table id.
     * @param partitionNumber Partition number.
     * @return Future with partition assignments as a value.
     */
    public static CompletableFuture<Set<Assignment>> partitionAssignments(
            MetaStorageManager metaStorageManager,
            int tableId,
            int partitionNumber
    ) {
        return metaStorageManager
                .get(stablePartAssignmentsKey(new TablePartitionId(tableId, partitionNumber)))
                .thenApply(e -> (e.value() == null) ? null : ByteUtils.fromBytes(e.value()));
    }

    /**
     * Returns partition assignments from vault.
     *
     * @param vaultManager Vault manager.
     * @param tableId Table id.
     * @param partitionNumber Partition number.
     * @return Returns partition assignments from vault or {@code null} if assignments is absent.
     */
    public static Set<Assignment> partitionAssignments(
            VaultManager vaultManager, int tableId, int partitionNumber) {
        VaultEntry entry =
                vaultManager.get(stablePartAssignmentsKey(new TablePartitionId(tableId, partitionNumber))).join();

        return (entry == null) ? null : ByteUtils.fromBytes(entry.value());
    }

    /**
     * Returns table assignments for all table partitions from meta storage.
     *
     * @param metaStorageManager Meta storage manager.
     * @param tableId Table id.
     * @param numberOfPartitions Number of partitions.
     * @return Future with table assignments as a value.
     */
    static CompletableFuture<List<Set<Assignment>>> tableAssignments(
            MetaStorageManager metaStorageManager,
            int tableId,
            int numberOfPartitions
    ) {
        Map<ByteArray, Integer> partitionKeysToPartitionNumber = new HashMap<>();

        for (int i = 0; i < numberOfPartitions; i++) {
            partitionKeysToPartitionNumber.put(stablePartAssignmentsKey(new TablePartitionId(tableId, i)), i);
        }

        return metaStorageManager.getAll(partitionKeysToPartitionNumber.keySet())
                .thenApply(entries -> {
                    if (entries.isEmpty()) {
                        return Collections.emptyList();
                    }

                    Set<Assignment>[] result = new Set[numberOfPartitions];

                    for (var entry : entries.entrySet()) {
                        result[partitionKeysToPartitionNumber.get(entry.getKey())] = ByteUtils.fromBytes(entry.getValue().value());
                    }

                    return Arrays.asList(result);
                });
    }

    /**
     * Returns table assignments for all table partitions from vault.
     *
     * @param vaultManager Vault manager.
     * @param tableId Table id.
     * @param numberOfPartitions Number of partitions.
     * @return Future with table assignments as a value.
     */
    public static List<Set<Assignment>> tableAssignments(
            VaultManager vaultManager,
            int tableId,
            int numberOfPartitions
    ) {
        return IntStream.range(0, numberOfPartitions)
                .mapToObj(i ->
                        (Set<Assignment>) ByteUtils.fromBytes(
                                vaultManager.get(
                                        stablePartAssignmentsKey(new TablePartitionId(tableId, i))
                                ).join().value())
                )
                .collect(Collectors.toList());
    }
}
