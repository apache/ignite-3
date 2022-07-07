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
import static org.apache.ignite.internal.metastorage.client.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.client.Conditions.value;
import static org.apache.ignite.internal.metastorage.client.Operations.ops;
import static org.apache.ignite.internal.metastorage.client.Operations.put;
import static org.apache.ignite.internal.metastorage.client.Operations.remove;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.If;
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
     * @param partitions Number of partitions in a table.
     * @param replicas Number of replicas for a table.
     * @param revision Revision of Meta Storage that is specific for the assignment update.
     * @param metaStorageMgr Meta Storage manager.
     * @return Future representing result of updating keys in {@code metaStorageMgr}
     */
    public static @NotNull CompletableFuture<Void> updatePendingAssignmentsKeys(
            String tableName, String partId, Collection<ClusterNode> baselineNodes,
            int partitions, int replicas, long revision, MetaStorageManager metaStorageMgr, int partNum) {
        ByteArray partChangeTriggerKey = partChangeTriggerKey(partId);

        ByteArray partAssignmentsPendingKey = pendingPartAssignmentsKey(partId);

        ByteArray partAssignmentsPlannedKey = plannedPartAssignmentsKey(partId);

        ByteArray partAssignmentsStableKey = stablePartAssignmentsKey(partId);

        byte[] partAssignmentsBytes = ByteUtils.toBytes(
                AffinityUtils.calculateAssignments(baselineNodes, partitions, replicas).get(partNum));

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
        var iif = If.iif(or(notExists(partChangeTriggerKey), value(partChangeTriggerKey).lt(ByteUtils.longToBytes(revision))),
                If.iif(and(notExists(partAssignmentsPendingKey), value(partAssignmentsStableKey).ne(partAssignmentsBytes)),
                        ops(
                                put(partAssignmentsPendingKey, partAssignmentsBytes),
                                put(partChangeTriggerKey, ByteUtils.longToBytes(revision))
                        ).yield(PENDING_KEY_UPDATED),
                        If.iif(value(partAssignmentsPendingKey).ne(partAssignmentsBytes),
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
                            "Update metastore pending partitions key={} for partition={}, table={} to {}",
                            partAssignmentsPendingKey.toString(), partNum, tableName,
                            ByteUtils.fromBytes(partAssignmentsBytes));

                    break;
                case PLANNED_KEY_UPDATED:
                    LOG.info(
                            "Update metastore planned partitions key={} for partition={}, table={} to {}",
                            partAssignmentsPlannedKey, partNum, tableName, ByteUtils.fromBytes(partAssignmentsBytes));

                    break;
                case PLANNED_KEY_REMOVED:
                    LOG.info(
                            "Remove planned key={} for partition={}, table={} due to the fact, "
                                    + "that current pending key has the same value as planned={}",
                            partAssignmentsPlannedKey.toString(), partNum, tableName, ByteUtils.fromBytes(partAssignmentsBytes));

                    break;
                case OUTDATED_UPDATE_RECEIVED:
                    LOG.debug(
                            "Received outdated rebalance trigger event with revision={} for partition={}, table={}",
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
    public static ByteArray pendingPartAssignmentsKey(String partId) {
        return new ByteArray(PENDING_ASSIGNMENTS_PREFIX + partId);
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalnce documentation</a>
     */
    public static ByteArray plannedPartAssignmentsKey(String partId) {
        return new ByteArray("assignments.planned." + partId);
    }

    /**
     * Key that is needed for the rebalance algorithm.
     *
     * @param partId Unique identifier of a partition.
     * @return Key for a partition.
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/table/tech-notes/rebalance.md">Rebalnce documentation</a>
     */
    public static ByteArray stablePartAssignmentsKey(String partId) {
        return new ByteArray(STABLE_ASSIGNMENTS_PREFIX + partId);
    }

    /**
     * Extract table id from pending key of partition.
     *
     * @param key Key.
     * @return Table id.
     */
    public static UUID extractTableId(ByteArray key, String prefix) {
        var strKey = key.toString();

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
}
