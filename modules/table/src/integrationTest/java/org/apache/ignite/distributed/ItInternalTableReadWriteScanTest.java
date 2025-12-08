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

package org.apache.ignite.distributed;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.OperationContext;
import org.apache.ignite.internal.table.RollbackTxOnErrorPublisher;
import org.apache.ignite.internal.table.TxContext;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.InternalTxOptions;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link InternalTable#partitionScan(int, InternalTransaction)}.
 */
public class ItInternalTableReadWriteScanTest extends ItAbstractInternalTableScanTest {
    /** Timestamp tracker. */
    private static final HybridTimestampTracker HYBRID_TIMESTAMP_TRACKER = HybridTimestampTracker.atomicTracker(null);

    @Override
    protected Publisher<BinaryRow> scan(int part, @Nullable InternalTransaction tx) {
        if (tx == null) {
            return internalTbl.partitionScan(part, null);
        }

        PendingTxPartitionEnlistment enlistment = tx.enlistedPartition(new ZonePartitionId(zoneId, part));

        InternalClusterNode primaryNode = clusterNodeResolver.getByConsistentId(enlistment.primaryNodeConsistentId());

        TxContext txContext = TxContext.readWrite(tx, enlistment.consistencyToken());

        return new RollbackTxOnErrorPublisher<>(
                tx,
                internalTbl.scan(part, primaryNode, OperationContext.create(txContext))
        );
    }

    @Test
    public void testInvalidPartitionParameterScan() {
        assertThrows(
                IllegalArgumentException.class,
                () -> scan(-1, null)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> scan(1, null)
        );
    }

    @Override
    protected InternalTransaction startTx() {
        InternalTransaction tx = internalTbl.txManager().beginExplicitRw(HYBRID_TIMESTAMP_TRACKER, InternalTxOptions.defaults());

        int partId = ((PartitionGroupId) internalTbl.groupId()).partitionId();
        ReplicationGroupId tblPartId = new ZonePartitionId(zoneId, partId);

        long term = 1L;

        tx.assignCommitPartition(tblPartId);

        InternalClusterNode primaryReplicaNode = getPrimaryReplica(tblPartId);

        tx.enlist(tblPartId, internalTbl.tableId(), primaryReplicaNode.name(), term);

        return tx;
    }
}
