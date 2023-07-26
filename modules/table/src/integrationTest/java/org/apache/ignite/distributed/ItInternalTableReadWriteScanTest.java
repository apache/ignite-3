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

import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.RollbackTxOnErrorPublisher;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.InternalTransaction;

/**
 * Tests for {@link InternalTable#scan(int, InternalTransaction)}.
 */
public class ItInternalTableReadWriteScanTest extends ItAbstractInternalTableScanTest {
    @Override
    protected Publisher<BinaryRow> scan(int part, InternalTransaction tx) {
        if (tx == null) {
            return internalTbl.scan(part, null);
        }

        ReplicaMeta recipient = tx.enlistedReplica(new TablePartitionId(internalTbl.tableId(), part));

        return new RollbackTxOnErrorPublisher<>(
                tx,
                internalTbl.scan(part, tx.id(), recipient, null, null, null, 0, null)
        );
    }

    @Override
    protected InternalTransaction startTx() {
        InternalTransaction tx = internalTbl.txManager().begin();

        TablePartitionId tblPartId = new TablePartitionId(internalTbl.tableId(), ((TablePartitionId) internalTbl.groupId()).partitionId());
        ReplicaMeta primaryReplica = IgniteTestUtils.await(placementDriver.awaitPrimaryReplica(tblPartId, clock.now()));

        tx.assignCommitPartition(tblPartId);
        tx.enlist(tblPartId, primaryReplica);

        return tx;
    }
}
