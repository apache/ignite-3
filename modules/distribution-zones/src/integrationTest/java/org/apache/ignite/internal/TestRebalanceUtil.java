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

package org.apache.ignite.internal;

import static org.apache.ignite.internal.lang.IgniteSystemProperties.enabledColocation;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableViewInternal;

// TODO https://issues.apache.org/jira/browse/IGNITE-22522 Remove this class and change its usages to {@link ZoneRebalanceUtil}.
/**
 * Helper util class for rebalance tests.
 */
public class TestRebalanceUtil {
    public static PartitionGroupId partitionReplicationGroupId(TableViewInternal table, int partitionId) {
        return partitionReplicationGroupId(table.internalTable(), partitionId);
    }

    public static PartitionGroupId partitionReplicationGroupId(InternalTable table, int partitionId) {
        if (enabledColocation()) {
            return new ZonePartitionId(table.zoneId(), partitionId);
        } else {
            return new TablePartitionId(table.tableId(), partitionId);
        }
    }

    public static PartitionGroupId partitionReplicationGroupId(CatalogTableDescriptor tableDescriptor, int partitionId) {
        if (enabledColocation()) {
            return new ZonePartitionId(tableDescriptor.zoneId(), partitionId);
        } else {
            return new TablePartitionId(tableDescriptor.id(), partitionId);
        }
    }

    public static ByteArray stablePartitionAssignmentsKey(PartitionGroupId partitionGroupId) {
        if (enabledColocation()) {
            return ZoneRebalanceUtil.stablePartAssignmentsKey((ZonePartitionId) partitionGroupId);
        } else {
            return RebalanceUtil.stablePartAssignmentsKey((TablePartitionId) partitionGroupId);
        }
    }

    public static ByteArray pendingPartitionAssignmentsKey(PartitionGroupId partitionGroupId) {
        if (enabledColocation()) {
            return ZoneRebalanceUtil.pendingPartAssignmentsQueueKey((ZonePartitionId) partitionGroupId);
        } else {
            return RebalanceUtil.pendingPartAssignmentsQueueKey((TablePartitionId) partitionGroupId);
        }
    }

    public static ByteArray plannedPartitionAssignmentsKey(PartitionGroupId partitionGroupId) {
        if (enabledColocation()) {
            return ZoneRebalanceUtil.plannedPartAssignmentsKey((ZonePartitionId) partitionGroupId);
        } else {
            return RebalanceUtil.plannedPartAssignmentsKey((TablePartitionId) partitionGroupId);
        }
    }

    public static CompletableFuture<Set<Assignment>> stablePartitionAssignments(
            MetaStorageManager metaStorageManager,
            TableViewInternal table,
            int partitionId
    ) {
        if (enabledColocation()) {
            return ZoneRebalanceUtil.zonePartitionAssignments(metaStorageManager, table.zoneId(), partitionId);
        } else {
            return RebalanceUtil.stablePartitionAssignments(metaStorageManager, table.tableId(), partitionId);
        }
    }

    public static CompletableFuture<Set<Assignment>> pendingPartitionAssignments(
            MetaStorageManager metaStorageManager,
            TableViewInternal table,
            int partitionId
    ) {
        return metaStorageManager
                .get(pendingPartitionAssignmentsKey(partitionReplicationGroupId(table, partitionId)))
                .thenApply(e -> e.value() == null ? null : AssignmentsQueue.fromBytes(e.value()).poll().nodes());
    }

    public static CompletableFuture<Set<Assignment>> plannedPartitionAssignments(
            MetaStorageManager metaStorageManager,
            TableViewInternal table,
            int partitionId
    ) {
        return metaStorageManager
                .get(plannedPartitionAssignmentsKey(partitionReplicationGroupId(table, partitionId)))
                .thenApply(e -> e.value() == null ? null : AssignmentsQueue.fromBytes(e.value()).poll().nodes());
    }
}
