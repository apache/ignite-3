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
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.RollbackTxOnErrorPublisher;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link InternalTable#scan(int, InternalTransaction)}.
 */
public class ItInternalTableReadWriteScanTest extends ItAbstractInternalTableScanTest {
    /** Timestamp tracker. */
    private static final HybridTimestampTracker HYBRID_TIMESTAMP_TRACKER = new HybridTimestampTracker();

    @Override
    protected Publisher<BinaryRow> scan(int part, @Nullable InternalTransaction tx) {
        if (tx == null) {
            return internalTbl.scan(part, null);
        }

        IgniteBiTuple<ClusterNode, Long> leaderWithConsistencyToken =
                tx.enlistedNodeAndConsistencyToken(new TablePartitionId(internalTbl.tableId(), part));

        PrimaryReplica recipient = new PrimaryReplica(leaderWithConsistencyToken.get1(), leaderWithConsistencyToken.get2());

        return new RollbackTxOnErrorPublisher<>(
                tx,
                internalTbl.scan(part, tx.id(), tx.commitPartition(), tx.coordinatorId(), recipient, null, null, null, 0, null)
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
        InternalTransaction tx = internalTbl.txManager().begin(HYBRID_TIMESTAMP_TRACKER);

        TablePartitionId tblPartId = new TablePartitionId(internalTbl.tableId(), ((TablePartitionId) internalTbl.groupId()).partitionId());

        long term = 1L;

        tx.assignCommitPartition(tblPartId);

        ClusterNode primaryReplicaNode = getPrimaryReplica(tblPartId);

        tx.enlist(tblPartId, new IgniteBiTuple<>(primaryReplicaNode, term));

        return tx;
    }
}
