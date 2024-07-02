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

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DISTRIBUTION_ZONE_DATA_NODES_VALUE_PREFIX;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Util class for methods needed for the rebalance process.
 */
public class ZoneRebalanceUtil {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ZoneRebalanceUtil.class);

    /**
     * Status values for methods like
     * {@link #updatePendingAssignmentsKeys(CatalogZoneDescriptor, ZonePartitionId, Collection, int, long, MetaStorageManager, int, Set)}.
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

    /**
     * Update keys that related to rebalance algorithm in Meta Storage. Keys are specific for partition.
     *
     * @param zoneDescriptor Zone descriptor.
     * @param zonePartitionId Unique aggregate identifier of a partition of a zone.
     * @param dataNodes Data nodes.
     * @param replicas Number of replicas for a zone.
     * @param revision Revision of Meta Storage that is specific for the assignment update.
     * @param metaStorageMgr Meta Storage manager.
     * @param partNum Partition id.
     * @param zoneCfgPartAssignments Zone configuration assignments.
     * @return Future representing result of updating keys in {@code metaStorageMgr}
     */
    public static CompletableFuture<Void> updatePendingAssignmentsKeys(
            CatalogZoneDescriptor zoneDescriptor,
            ZonePartitionId zonePartitionId,
            Collection<String> dataNodes,
            int replicas,
            long revision,
            MetaStorageManager metaStorageMgr,
            int partNum,
            Set<Assignment> zoneCfgPartAssignments
    ) {
        ByteArray partChangeTriggerKey = pendingChangeTriggerKey(zonePartitionId);

        ByteArray partAssignmentsPendingKey = pendingPartAssignmentsKey(zonePartitionId);

        ByteArray partAssignmentsPlannedKey = plannedPartAssignmentsKey(zonePartitionId);

        ByteArray partAssignmentsStableKey = stablePartAssignmentsKey(zonePartitionId);

        Set<Assignment> partAssignments = AffinityUtils.calculateAssignmentForPartition(dataNodes, partNum, replicas);

        boolean isNewAssignments = !zoneCfgPartAssignments.equals(partAssignments);

        byte[] partAssignmentsBytes = Assignments.toBytes(partAssignments);

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
                            "Update metastore pending partitions key [key={}, partition={}, zone={}/{}, newVal={}]",
                            partAssignmentsPendingKey.toString(), partNum, zoneDescriptor.id(), zoneDescriptor.name(),
                            partAssignments);

                    break;
                case PLANNED_KEY_UPDATED:
                    LOG.info(
                            "Update metastore planned partitions key [key={}, partition={}, zone={}/{}, newVal={}]",
                            partAssignmentsPlannedKey, partNum, zoneDescriptor.id(), zoneDescriptor.name(),
                            partAssignments
                    );

                    break;
                case PLANNED_KEY_REMOVED_EQUALS_PENDING:
                    LOG.info(
                            "Remove planned key because current pending key has the same value [key={}, partition={}, zone={}/{}, val={}]",
                            partAssignmentsPlannedKey.toString(), partNum, zoneDescriptor.id(), zoneDescriptor.name(),
                            partAssignments
                    );

                    break;
                case PLANNED_KEY_REMOVED_EMPTY_PENDING:
                    LOG.info(
                            "Remove planned key because pending is empty and calculated assignments are equal to current assignments "
                                    + "[key={}, partition={}, zone={}/{}, val={}]",
                            partAssignmentsPlannedKey.toString(), partNum, zoneDescriptor.id(), zoneDescriptor.name(),
                            partAssignments
                    );

                    break;
                case ASSIGNMENT_NOT_UPDATED:
                    LOG.debug(
                            "Assignments are not updated [key={}, partition={}, zone={}/{}, val={}]",
                            partAssignmentsPlannedKey.toString(), partNum, zoneDescriptor.id(), zoneDescriptor.name(),
                            partAssignments
                    );

                    break;
                case OUTDATED_UPDATE_RECEIVED:
                    LOG.debug(
                            "Received outdated rebalance trigger event [revision={}, partition={}, zone={}/{}]",
                            revision, partNum, zoneDescriptor.id(), zoneDescriptor.name());

                    break;
                default:
                    throw new IllegalStateException("Unknown return code for rebalance metastore multi-invoke");
            }
        });
    }

    /**
     * Triggers rebalance on all partitions of the provided zone: that is, reads zone assignments from
     * the MetaStorage, computes new ones based on the current properties of the zone, the
     * provided data nodes, and, if the calculated assignments are different from the ones loaded from the
     * MetaStorages, writes them as pending assignments.
     *
     * @param zoneDescriptor Zone descriptor.
     * @param dataNodes Data nodes to use.
     * @param storageRevision MetaStorage revision corresponding to this request.
     * @param metaStorageManager MetaStorage manager used to read/write assignments.
     * @return Array of futures, one per partition of the zone; the futures complete when the described
     *     rebalance triggering completes.
     */
    static CompletableFuture<Void> triggerZonePartitionsRebalance(
            CatalogZoneDescriptor zoneDescriptor,
            Set<String> dataNodes,
            long storageRevision,
            MetaStorageManager metaStorageManager
    ) {
        CompletableFuture<Map<Integer, Assignments>> zoneAssignmentsFut = zoneAssignments(
                metaStorageManager,
                zoneDescriptor.id(),
                Set.of(),
                zoneDescriptor.partitions()
        );

        CompletableFuture<?>[] partitionFutures = new CompletableFuture[zoneDescriptor.partitions()];

        for (int partId = 0; partId < zoneDescriptor.partitions(); partId++) {
            ZonePartitionId replicaGrpId = new ZonePartitionId(zoneDescriptor.id(), partId);

            int finalPartId = partId;

            partitionFutures[partId] = zoneAssignmentsFut.thenCompose(zoneAssignments ->
                    // TODO https://issues.apache.org/jira/browse/IGNITE-19763 We should distinguish empty stable assignments on
                    // TODO node recovery in case of interrupted table creation, and moving from empty assignments to non-empty.
                    zoneAssignments.isEmpty() ? nullCompletedFuture() : updatePendingAssignmentsKeys(
                            zoneDescriptor,
                            replicaGrpId,
                            dataNodes,
                            zoneDescriptor.replicas(),
                            storageRevision,
                            metaStorageManager,
                            finalPartId,
                            zoneAssignments.get(finalPartId).nodes()
                    ));
        }

        // This set is used to deduplicate exceptions (if there is an exception from upstream, for instance,
        // when reading from MetaStorage, it will be encountered by every partition future) to avoid noise
        // in the logs.
        Set<Throwable> unwrappedCauses = ConcurrentHashMap.newKeySet();

        for (int partId = 0; partId < partitionFutures.length; partId++) {
            int finalPartId = partId;

            partitionFutures[partId].exceptionally(e -> {
                Throwable cause = ExceptionUtils.unwrapCause(e);

                if (unwrappedCauses.add(cause)) {
                    // The exception is specific to this partition.
                    LOG.error(
                            "Exception on updating assignments for [zone={}, partition={}]",
                            e,
                            zoneInfo(zoneDescriptor), finalPartId
                    );
                } else {
                    // The exception is from upstream and not specific for this partition, so don't log the partition index.
                    LOG.error(
                            "Exception on updating assignments for [zone={}]",
                            e,
                            zoneInfo(zoneDescriptor)
                    );
                }

                return null;
            });
        }

        return allOf(partitionFutures);
    }

    private static String zoneInfo(CatalogZoneDescriptor zoneDescriptor) {
        return zoneDescriptor.id() + "/" + zoneDescriptor.name();
    }

    /** Key prefix for pending assignments. */
    public static final String PENDING_ASSIGNMENTS_PREFIX = "zone.assignments.pending.";

    /** Key prefix for stable assignments. */
    public static final String STABLE_ASSIGNMENTS_PREFIX = "zone.assignments.stable.";

    /** Key prefix for planned assignments. */
    public static final String PLANNED_ASSIGNMENTS_PREFIX = "zone.assignments.planned.";

    /** Key prefix for switch reduce assignments. */
    public static final String ASSIGNMENTS_SWITCH_REDUCE_PREFIX = "zone.assignments.switch.reduce.";

    /** Key prefix for switch append assignments. */
    public static final String ASSIGNMENTS_SWITCH_APPEND_PREFIX = "zone.assignments.switch.append.";

    /**
     * Key that is needed for skipping stale events of pending key change.
     *
     * @param zonePartitionId Unique aggregate identifier of a partition of a zone.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalance documentation</a>
     */
    public static ByteArray pendingChangeTriggerKey(ZonePartitionId zonePartitionId) {
        return new ByteArray(zonePartitionId + "zone.pending.change.trigger");
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param zonePartitionId Unique aggregate identifier of a partition of a zone.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalance documentation</a>
     */
    public static ByteArray pendingPartAssignmentsKey(ZonePartitionId zonePartitionId) {
        return new ByteArray(PENDING_ASSIGNMENTS_PREFIX + zonePartitionId);
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param zonePartitionId Unique aggregate identifier of a partition of a zone.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalance documentation</a>
     */
    public static ByteArray plannedPartAssignmentsKey(ZonePartitionId zonePartitionId) {
        return new ByteArray(PLANNED_ASSIGNMENTS_PREFIX + zonePartitionId);
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param zonePartitionId Unique aggregate identifier of a partition of a zone.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalance documentation</a>
     */
    public static ByteArray stablePartAssignmentsKey(ZonePartitionId zonePartitionId) {
        return new ByteArray(STABLE_ASSIGNMENTS_PREFIX + zonePartitionId);
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param zonePartitionId Unique aggregate identifier of a partition of a zone.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalance documentation</a>
     */
    public static ByteArray switchReduceKey(ZonePartitionId zonePartitionId) {
        return new ByteArray(ASSIGNMENTS_SWITCH_REDUCE_PREFIX + zonePartitionId);
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param zonePartitionId Unique aggregate identifier of a partition of a zone.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalance documentation</a>
     */
    public static ByteArray switchAppendKey(ZonePartitionId zonePartitionId) {
        return new ByteArray(ASSIGNMENTS_SWITCH_APPEND_PREFIX + zonePartitionId);
    }

    /**
     * Extract zone id from a metastorage key of partition.
     *
     * @param key Key.
     * @param prefix Key prefix.
     * @return Table id.
     */
    public static int extractZoneId(byte[] key, String prefix) {
        String strKey = new String(key, StandardCharsets.UTF_8);

        return Integer.parseInt(strKey.substring(prefix.length(), strKey.indexOf("_part_")));
    }

    /**
     * Extract zone id from a metastorage key {@link RebalanceUtil#tablesCounterKey}.
     *
     * @param key Key.
     * @return Table id.
     */
    public static int extractZoneIdDataNodes(byte[] key) {
        String strKey = new String(key, StandardCharsets.UTF_8);

        return Integer.parseInt(strKey.substring(DISTRIBUTION_ZONE_DATA_NODES_VALUE_PREFIX.length()));
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
     * Returns partition assignments from meta storage locally.
     *
     * @param metaStorageManager Meta storage manager.
     * @param zoneId Zone id.
     * @param partitionNumber Partition number.
     * @param revision Revision.
     * @return Returns partition assignments from meta storage locally or {@code null} if assignments is absent.
     */
    @Nullable
    public static Set<Assignment> zonePartitionAssignmentsGetLocally(
            MetaStorageManager metaStorageManager,
            int zoneId,
            int partitionNumber,
            long revision
    ) {
        Entry entry = metaStorageManager.getLocally(stablePartAssignmentsKey(new ZonePartitionId(zoneId, partitionNumber)), revision);

        return (entry == null || entry.empty() || entry.tombstone()) ? null : Assignments.fromBytes(entry.value()).nodes();
    }

    /**
     * Returns zone assignments for all zone partitions from meta storage locally. Assignments must be present.
     *
     * @param metaStorageManager Meta storage manager.
     * @param zoneId Zone id.
     * @param numberOfPartitions Number of partitions.
     * @param revision Revision.
     * @return Future with zone assignments as a value.
     */
    public static List<Assignments> zoneAssignmentsGetLocally(
            MetaStorageManager metaStorageManager,
            int zoneId,
            int numberOfPartitions,
            long revision
    ) {
        return IntStream.range(0, numberOfPartitions)
                .mapToObj(p -> {
                    Entry e = metaStorageManager.getLocally(stablePartAssignmentsKey(new ZonePartitionId(zoneId, p)), revision);

                    assert e != null && !e.empty() && !e.tombstone() : e;

                    return Assignments.fromBytes(e.value());
                })
                .collect(toList());
    }

    /**
     * Returns zone assignments for zone partitions from meta storage.
     *
     * @param metaStorageManager Meta storage manager.
     * @param zoneId Zone id.
     * @param partitionIds IDs of partitions to get assignments for. If empty, get all partition assignments.
     * @param numberOfPartitions Number of partitions. Ignored if partition IDs are specified.
     * @return Future with zone assignments as a value.
     */
    private static CompletableFuture<Map<Integer, Assignments>> zoneAssignments(
            MetaStorageManager metaStorageManager,
            int zoneId,
            Set<Integer> partitionIds,
            int numberOfPartitions
    ) {
        Map<ByteArray, Integer> partitionKeysToPartitionNumber = new HashMap<>();

        Collection<Integer> ids = partitionIds.isEmpty()
                ? IntStream.range(0, numberOfPartitions).boxed().collect(toList())
                : partitionIds;

        for (Integer partId : ids) {
            partitionKeysToPartitionNumber.put(stablePartAssignmentsKey(new ZonePartitionId(zoneId, partId)), partId);
        }

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
                            + numberOfMsPartitions + ", numberOfPartitions=" + entries.size() + ", zoneId=" + zoneId + "].";

                    return numberOfMsPartitions == 0 ? Map.of() : result;
                });
    }
}
