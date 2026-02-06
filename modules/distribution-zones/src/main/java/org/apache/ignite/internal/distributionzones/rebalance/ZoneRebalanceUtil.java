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
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.UpdateStatus.ASSIGNMENT_NOT_UPDATED;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.UpdateStatus.OUTDATED_UPDATE_RECEIVED;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.UpdateStatus.PENDING_KEY_UPDATED;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.UpdateStatus.PLANNED_KEY_REMOVED_EMPTY_PENDING;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.UpdateStatus.PLANNED_KEY_REMOVED_EQUALS_PENDING;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.UpdateStatus.PLANNED_KEY_UPDATED;
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
import static org.apache.ignite.internal.partitiondistribution.PendingAssignmentsCalculator.pendingAssignmentsCalculator;
import static org.apache.ignite.internal.util.ByteUtils.longToBytesKeepingOrder;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.StringUtils.toStringWithoutPrefix;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.hlc.HybridTimestamp;
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
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Util class for methods needed for the rebalance process.
 */
public class ZoneRebalanceUtil {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ZoneRebalanceUtil.class);

    /** Key prefix for pending assignments. */
    public static final String PENDING_ASSIGNMENTS_QUEUE_PREFIX = "zone.assignments.pending.";

    public static final byte[] PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES = PENDING_ASSIGNMENTS_QUEUE_PREFIX.getBytes(UTF_8);

    /** Key prefix for stable assignments. */
    public static final String STABLE_ASSIGNMENTS_PREFIX = "zone.assignments.stable.";

    public static final byte[] STABLE_ASSIGNMENTS_PREFIX_BYTES = STABLE_ASSIGNMENTS_PREFIX.getBytes(UTF_8);

    /** Key prefix for planned assignments. */
    public static final String PLANNED_ASSIGNMENTS_PREFIX = "zone.assignments.planned.";

    /** Key prefix for switch reduce assignments. */
    public static final String ASSIGNMENTS_SWITCH_REDUCE_PREFIX = "zone.assignments.switch.reduce.";

    public static final byte[] ASSIGNMENTS_SWITCH_REDUCE_PREFIX_BYTES = ASSIGNMENTS_SWITCH_REDUCE_PREFIX.getBytes(UTF_8);

    /** Key prefix for switch append assignments. */
    public static final String ASSIGNMENTS_SWITCH_APPEND_PREFIX = "zone.assignments.switch.append.";

    /** Key prefix for change trigger keys. */
    private static final String ZONE_PENDING_CHANGE_TRIGGER_PREFIX = "zone.pending.change.trigger.";

    static final byte[] ZONE_PENDING_CHANGE_TRIGGER_PREFIX_BYTES = ZONE_PENDING_CHANGE_TRIGGER_PREFIX.getBytes(UTF_8);

    public static final String ZONE_ASSIGNMENTS_CHAIN_PREFIX = "zone.assignments.chain.";

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

    /**
     * Update keys that related to rebalance algorithm in Meta Storage. Keys are specific for partition.
     *
     * @param zoneDescriptor Zone descriptor.
     * @param zonePartitionId Unique aggregate identifier of a partition of a zone.
     * @param dataNodes Data nodes.
     * @param partitions Number of partitions in a zone.
     * @param replicas Number of replicas for a zone.
     * @param consensusGroupSize Number of nodes in a consensus group.
     * @param revision Revision of Meta Storage that is specific for the assignment update.
     * @param timestamp Timestamp of Meta Storage that is specific for the assignment update.
     * @param metaStorageMgr Meta Storage manager.
     * @param partNum Partition id.
     * @param zoneCfgPartAssignments Zone configuration assignments.
     * @param assignmentsTimestamp Time when the catalog version that the assignments were calculated against becomes active.
     * @return Future representing result of updating keys in {@code metaStorageMgr}
     */
    public static CompletableFuture<Void> updatePendingAssignmentsKeys(
            CatalogZoneDescriptor zoneDescriptor,
            ZonePartitionId zonePartitionId,
            Collection<String> dataNodes,
            int partitions,
            int replicas,
            int consensusGroupSize,
            long revision,
            HybridTimestamp timestamp,
            MetaStorageManager metaStorageMgr,
            int partNum,
            Set<Assignment> zoneCfgPartAssignments,
            long assignmentsTimestamp,
            Set<String> aliveNodes,
            ConsistencyMode consistencyMode
    ) {
        ByteArray partChangeTriggerKey = pendingChangeTriggerKey(zonePartitionId);

        ByteArray partAssignmentsPendingKey = pendingPartAssignmentsQueueKey(zonePartitionId);

        ByteArray partAssignmentsPlannedKey = plannedPartAssignmentsKey(zonePartitionId);

        ByteArray partAssignmentsStableKey = stablePartAssignmentsKey(zonePartitionId);

        Set<Assignment> calculatedAssignments = calculateAssignmentForPartition(
                dataNodes,
                partNum,
                partitions,
                replicas,
                consensusGroupSize
        );

        Set<Assignment> targetAssignmentSet;

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
            for (Assignment assignment : zoneCfgPartAssignments) {
                if (calculatedAssignments.contains(assignment)) {
                    resultingAssignments.add(assignment);
                }
            }

            targetAssignmentSet = resultingAssignments;
        } else {
            targetAssignmentSet = calculatedAssignments;
        }

        boolean isNewAssignments = !zoneCfgPartAssignments.equals(targetAssignmentSet);

        Assignments targetAssignments = Assignments.of(targetAssignmentSet, assignmentsTimestamp);
        AssignmentsQueue partAssignmentsPendingQueue = pendingAssignmentsCalculator()
                .stable(Assignments.of(zoneCfgPartAssignments, assignmentsTimestamp))
                .target(targetAssignments)
                .toQueue();

        byte[] partAssignmentsPlannedBytes = targetAssignments.toBytes();
        byte[] partAssignmentsPendingBytes = partAssignmentsPendingQueue.toBytes();

        //    if empty(partition.change.trigger) || partition.change.trigger < event.timestamp:
        //        if empty(partition.assignments.pending)
        //              && ((isNewAssignments && empty(partition.assignments.stable))
        //                  || (partition.assignments.stable != calcPartAssignments() && !empty(partition.assignments.stable))):
        //            partition.assignments.pending = partAssignmentsPendingQueue
        //            partition.change.trigger = event.timestamp
        //        else:
        //            if partition.assignments.pending != partAssignmentsPendingQueue && !empty(partition.assignments.pending)
        //                partition.assignments.planned = calcPartAssignments()
        //                partition.change.trigger = event.timestamp
        //            else if partition.assignments.pending == partAssignmentsPendingQueue
        //                remove(partition.assignments.planned)
        //                partition.change.trigger = event.timestamp
        //                message after the metastorage invoke:
        //                "Remove planned key because current pending key has the same value."
        //            else if empty(partition.assignments.pending)
        //                remove(partition.assignments.planned)
        //                partition.change.trigger = event.timestamp
        //                message after the metastorage invoke:
        //                "Remove planned key because pending is empty and calculated assignments are equal to current assignments."
        //    else:
        //        skip

        Condition newAssignmentsCondition = exists(partAssignmentsStableKey)
                .and(value(partAssignmentsStableKey).ne(partAssignmentsPlannedBytes));

        if (isNewAssignments) {
            newAssignmentsCondition = notExists(partAssignmentsStableKey).or(newAssignmentsCondition);
        }

        byte[] timestampBytes = longToBytesKeepingOrder(timestamp.longValue());

        Iif iif = iif(
                or(notExists(partChangeTriggerKey), value(partChangeTriggerKey).lt(timestampBytes)),
                iif(and(notExists(partAssignmentsPendingKey), newAssignmentsCondition),
                        ops(
                                put(partAssignmentsPendingKey, partAssignmentsPendingBytes),
                                put(partChangeTriggerKey, timestampBytes)
                        ).yield(PENDING_KEY_UPDATED.ordinal()),
                        iif(and(value(partAssignmentsPendingKey).ne(partAssignmentsPendingBytes), exists(partAssignmentsPendingKey)),
                                ops(
                                        put(partAssignmentsPlannedKey, partAssignmentsPlannedBytes),
                                        put(partChangeTriggerKey, timestampBytes)
                                ).yield(PLANNED_KEY_UPDATED.ordinal()),
                                iif(value(partAssignmentsPendingKey).eq(partAssignmentsPendingBytes),
                                        ops(
                                                remove(partAssignmentsPlannedKey),
                                                put(partChangeTriggerKey, timestampBytes)
                                        ).yield(PLANNED_KEY_REMOVED_EQUALS_PENDING.ordinal()),
                                        iif(notExists(partAssignmentsPendingKey),
                                                ops(
                                                        remove(partAssignmentsPlannedKey),
                                                        put(partChangeTriggerKey, timestampBytes)
                                                ).yield(PLANNED_KEY_REMOVED_EMPTY_PENDING.ordinal()),
                                                ops().yield(ASSIGNMENT_NOT_UPDATED.ordinal()))
                                ))),
                ops().yield(OUTDATED_UPDATE_RECEIVED.ordinal()));

        return metaStorageMgr.invoke(iif).thenAccept(sr -> {
            switch (UpdateStatus.valueOf(sr.getAsInt())) {
                case PENDING_KEY_UPDATED:
                    LOG.info(
                            "Update metastore pending partitions key [key={}, partition={}, zone={}/{}, newVal={}, timestamp={}]",
                            partAssignmentsPendingKey.toString(), partNum, zoneDescriptor.id(), zoneDescriptor.name(),
                            partAssignmentsPendingQueue, timestamp);

                    break;
                case PLANNED_KEY_UPDATED:
                    LOG.info(
                            "Update metastore planned partitions key [key={}, partition={}, zone={}/{}, newVal={}]",
                            partAssignmentsPlannedKey, partNum, zoneDescriptor.id(), zoneDescriptor.name(),
                            targetAssignmentSet
                    );

                    break;
                case PLANNED_KEY_REMOVED_EQUALS_PENDING:
                    LOG.info(
                            "Remove planned key because current pending key has the same value [key={}, partition={}, zone={}/{}, val={}]",
                            partAssignmentsPlannedKey.toString(), partNum, zoneDescriptor.id(), zoneDescriptor.name(),
                            targetAssignmentSet
                    );

                    break;
                case PLANNED_KEY_REMOVED_EMPTY_PENDING:
                    LOG.info(
                            "Remove planned key because pending is empty and calculated assignments are equal to current assignments "
                                    + "[key={}, partition={}, zone={}/{}, val={}]",
                            partAssignmentsPlannedKey.toString(), partNum, zoneDescriptor.id(), zoneDescriptor.name(),
                            targetAssignmentSet
                    );

                    break;
                case ASSIGNMENT_NOT_UPDATED:
                    LOG.debug(
                            "Assignments are not updated [key={}, partition={}, zone={}/{}, val={}]",
                            partAssignmentsPlannedKey.toString(), partNum, zoneDescriptor.id(), zoneDescriptor.name(),
                            targetAssignmentSet
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
     * @param storageTimestamp MetaStorage timestamp corresponding to this request.
     * @param metaStorageManager MetaStorage manager used to read/write assignments.
     * @param busyLock Busy lock to use.
     * @param assignmentsTimestamp Time when the catalog version that the assignments were calculated against becomes active.
     * @return Array of futures, one per partition of the zone; the futures complete when the described
     *     rebalance triggering completes.
     */
    static CompletableFuture<Void> triggerZonePartitionsRebalance(
            CatalogZoneDescriptor zoneDescriptor,
            Set<String> dataNodes,
            long storageRevision,
            HybridTimestamp storageTimestamp,
            MetaStorageManager metaStorageManager,
            IgniteSpinBusyLock busyLock,
            long assignmentsTimestamp,
            Set<String> aliveNodes
    ) {
        CompletableFuture<Map<Integer, Assignments>> zoneAssignmentsFut = zoneStableAssignments(
                metaStorageManager,
                zoneDescriptor.id(),
                partitionIds(zoneDescriptor.partitions())
        );

        CompletableFuture<?>[] partitionFutures = new CompletableFuture[zoneDescriptor.partitions()];

        for (int partId = 0; partId < zoneDescriptor.partitions(); partId++) {
            ZonePartitionId replicaGrpId = new ZonePartitionId(zoneDescriptor.id(), partId);

            int finalPartId = partId;

            partitionFutures[partId] = zoneAssignmentsFut.thenCompose(zoneAssignments -> inBusyLockAsync(busyLock, () -> {
                // In case of empty assignments due to initially empty data nodes, assignments will be recalculated
                // after the transition to non-empty data nodes.
                // In case of empty assignments due to interrupted zone creation, assignments will be written
                // during the node recovery and then replicas will be started.
                // In case when data nodes become empty, assignments are not recalculated
                // (see DistributionZoneRebalanceEngineV2.createDistributionZonesDataNodesListener).
                return zoneAssignments.isEmpty() ? nullCompletedFuture() : updatePendingAssignmentsKeys(
                        zoneDescriptor,
                        replicaGrpId,
                        dataNodes,
                        zoneDescriptor.partitions(),
                        zoneDescriptor.replicas(),
                        zoneDescriptor.consensusGroupSize(),
                        storageRevision,
                        storageTimestamp,
                        metaStorageManager,
                        finalPartId,
                        zoneAssignments.get(finalPartId).nodes(),
                        assignmentsTimestamp,
                        aliveNodes,
                        zoneDescriptor.consistencyMode()
                );
            }));
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

    /**
     * Key that is needed for skipping stale events of pending key change.
     *
     * @param zonePartitionId Unique aggregate identifier of a partition of a zone.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalance documentation</a>
     */
    public static ByteArray pendingChangeTriggerKey(ZonePartitionId zonePartitionId) {
        return new ByteArray(ZONE_PENDING_CHANGE_TRIGGER_PREFIX + zonePartitionId);
    }

    /**
     * Key for the graceful restart in HA mode.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-131%3A+Partition+Majority+Unavailability+Handling">HA mode</a>
     */
    public static ByteArray assignmentsChainKey(ZonePartitionId partId) {
        return new ByteArray(ZONE_ASSIGNMENTS_CHAIN_PREFIX + partId);
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param zonePartitionId Unique aggregate identifier of a partition of a zone.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalance documentation</a>
     */
    public static ByteArray pendingPartAssignmentsQueueKey(ZonePartitionId zonePartitionId) {
        return new ByteArray(PENDING_ASSIGNMENTS_QUEUE_PREFIX + zonePartitionId);
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
     * Converts the given {@code key}, stripping it off the given {@code prefix}, into a {@link ZonePartitionId}.
     *
     * @param key Metastorage key.
     * @param prefix Key prefix.
     * @return {@link ZonePartitionId} that was encoded in the key.
     */
    public static ZonePartitionId extractZonePartitionId(byte[] key, byte[] prefix) {
        var zonePartitionIdString = toStringWithoutPrefix(key, prefix.length);

        return ZonePartitionId.fromString(zonePartitionIdString);
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
     * Returns stable partition assignments from meta storage locally.
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
        Assignments assignments =
                zoneStableAssignmentsGetLocally(metaStorageManager, new ZonePartitionId(zoneId, partitionNumber), revision);
        return assignments == null ? null : assignments.nodes();
    }

    /**
     * Returns zone stable assignments for all zone partitions from meta storage locally. Assignments must be present.
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
                    Assignments assignments =
                            zoneStableAssignmentsGetLocally(metaStorageManager, new ZonePartitionId(zoneId, p), revision);

                    assert assignments != null : "No assignments found for " + new ZonePartitionId(zoneId, p);

                    return assignments;
                })
                .collect(toList());
    }

    /**
     * Returns zone pending assignments for all zone partitions from meta storage locally. Assignments must be present.
     *
     * @param metaStorageManager Meta storage manager.
     * @param zoneId Zone id.
     * @param numberOfPartitions Number of partitions.
     * @param revision Revision.
     * @return Future with zone assignments as a value.
     */
    public static List<@Nullable Assignments> zonePendingAssignmentsGetLocally(
            MetaStorageManager metaStorageManager,
            int zoneId,
            int numberOfPartitions,
            long revision
    ) {
        return IntStream.range(0, numberOfPartitions)
                .mapToObj(p -> {
                    Entry e = metaStorageManager.getLocally(pendingPartAssignmentsQueueKey(new ZonePartitionId(zoneId, p)), revision);

                    return e != null && !e.empty() && !e.tombstone() ? AssignmentsQueue.fromBytes(e.value()).poll() : null;
                })
                .collect(toList());
    }

    /**
     * Returns stable partition assignments from meta storage.
     *
     * @param metaStorageManager Meta storage manager.
     * @param zoneId Table ID.
     * @param partitionId Partition ID.
     * @return Future with partition assignments as a value.
     */
    public static CompletableFuture<Set<Assignment>> stablePartitionAssignments(
            MetaStorageManager metaStorageManager,
            int zoneId,
            int partitionId
    ) {
        return metaStorageManager
                .get(stablePartAssignmentsKey(new ZonePartitionId(zoneId, partitionId)))
                .thenApply(e -> (e.value() == null) ? null : Assignments.fromBytes(e.value()).nodes());
    }

    /**
     * Returns pending partition assignments.
     *
     * @param metaStorageManager Meta storage manager.
     * @param zoneId Zone identifier.
     * @param partitionId Partition identifier.
     * @return Pending partition assignments.
     */
    @TestOnly
    public static CompletableFuture<Set<Assignment>> pendingPartitionAssignments(
            MetaStorageManager metaStorageManager,
            int zoneId,
            int partitionId
    ) {
        return metaStorageManager
                .get(pendingPartAssignmentsQueueKey(new ZonePartitionId(zoneId, partitionId)))
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
                .get(plannedPartAssignmentsKey(new ZonePartitionId(zoneId, partitionId)))
                .thenApply(e -> e.value() == null ? null : Assignments.fromBytes(e.value()).nodes());
    }

    /**
     * Returns zone assignments for zone partitions from meta storage.
     *
     * @param metaStorageManager Meta storage manager.
     * @param zoneId Zone id.
     * @param partitionIds IDs of partitions to get assignments for.
     * @return Future with zone assignments as a value.
     */
    public static CompletableFuture<Map<Integer, Assignments>> zoneStableAssignments(
            MetaStorageManager metaStorageManager,
            int zoneId,
            int[] partitionIds
    ) {
        return metastoreAssignments(
                metaStorageManager,
                partitionIds,
                partitionId -> stablePartAssignmentsKey(new ZonePartitionId(zoneId, partitionId))
        ).whenComplete((assignmentsMap, throwable) -> {
            if (throwable == null) {
                int numberOfMsPartitions = assignmentsMap.size();

                assert numberOfMsPartitions == 0 || numberOfMsPartitions == partitionIds.length
                        : "Invalid number of partition entries received from meta storage [received="
                        + numberOfMsPartitions + ", numberOfPartitions=" + partitionIds.length + ", zoneId=" + zoneId + "].";
            }
        });
    }

    /**
     * Returns partition assignments from meta storage locally.
     *
     * @param metaStorageManager Meta storage manager.
     * @param zonePartitionId Zone partition id.
     * @param revision Revision.
     * @return Returns partition assignments from meta storage locally or {@code null} if assignments is absent.
     */
    public static @Nullable Assignments zoneStableAssignmentsGetLocally(
            MetaStorageManager metaStorageManager,
            ZonePartitionId zonePartitionId,
            long revision
    ) {
        Entry entry = metaStorageManager.getLocally(stablePartAssignmentsKey(zonePartitionId), revision);

        return (entry == null || entry.empty() || entry.tombstone()) ? null : Assignments.fromBytes(entry.value());
    }

    /**
     * Returns assignments chains for all table partitions from meta storage locally.
     *
     * @param metaStorageManager Meta storage manager.
     * @param zoneId Zone id.
     * @param numberOfPartitions Number of partitions.
     * @param revision Revision.
     * @return Future with table assignments as a value.
     */
    public static List<AssignmentsChain> zoneAssignmentsChainGetLocally(
            MetaStorageManager metaStorageManager,
            int zoneId,
            int numberOfPartitions,
            long revision
    ) {
        return IntStream.range(0, numberOfPartitions)
                .mapToObj(p -> assignmentsChainGetLocally(metaStorageManager, new ZonePartitionId(zoneId, p), revision))
                .collect(toList());
    }

    /**
     * Returns assignments chain from meta storage locally.
     *
     * @param metaStorageManager Meta storage manager.
     * @param zonePartitionId Zone partition id.
     * @param revision Revision.
     * @return Returns assignments chain from meta storage locally or {@code null} if assignments is absent.
     */
    public static @Nullable AssignmentsChain assignmentsChainGetLocally(
            MetaStorageManager metaStorageManager,
            ZonePartitionId zonePartitionId,
            long revision
    ) {
        Entry e = metaStorageManager.getLocally(assignmentsChainKey(zonePartitionId), revision);

        return e != null ? AssignmentsChain.fromBytes(e.value()) : null;
    }

    /**
     * Returns stable partition assignments from meta storage.
     *
     * @param metaStorageManager Meta storage manager.
     * @param zonePartitionId Zone partition id.
     * @return Future with partition assignments as a value.
     */
    public static CompletableFuture<Assignments> zonePartitionStableAssignments(
            MetaStorageManager metaStorageManager,
            ZonePartitionId zonePartitionId
    ) {
        return metaStorageManager
                .get(stablePartAssignmentsKey(zonePartitionId))
                .thenApply(e -> e == null || e.value() == null || e.empty() || e.tombstone()
                        ? Assignments.EMPTY
                        : Assignments.fromBytes(e.value())
                );
    }
}
