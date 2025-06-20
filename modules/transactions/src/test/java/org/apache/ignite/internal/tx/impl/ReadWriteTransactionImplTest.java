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

package org.apache.ignite.internal.tx.impl;

import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.colocationEnabled;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;

import java.util.HashSet;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.tx.TransactionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@WithSystemProperty(key = IgniteSystemProperties.COLOCATION_FEATURE_FLAG, value = "false")
class ReadWriteTransactionImplTest extends BaseIgniteAbstractTest {
    private static final ClusterNode CLUSTER_NODE = new ClusterNodeImpl(
            randomUUID(),
            "test-node",
            new NetworkAddress("localhost", 1234)
    );

    private static final int TABLE_ID = 1;
    private static final int ZONE_ID = 2;

    /** Transaction commit partition id. */
    private final ReplicationGroupId txCommitPart = targetReplicationGroupId(ZONE_ID, 0);

    @Mock
    private TxManager txManager;

    private final HybridClock clock = new HybridClockImpl();

    /** The state is assigned to the transaction after a finalize method (commit or rollback) is called. */
    private TxState txState = null;

    // TODO: IGNITE-22522 - inline this after switching to ZonePartitionId.
    ReplicationGroupId targetReplicationGroupId(int tableOrZoneId, int partId) {
        return new TablePartitionId(tableOrZoneId, partId);
    }

    @Test
    public void effectiveSchemaTimestampIsBeginTimestamp() {
        HybridTimestamp beginTs = clock.now();

        UUID txId = TestTransactionIds.TRANSACTION_ID_GENERATOR.transactionIdFor(beginTs);

        var tx = new ReadWriteTransactionImpl(
                txManager, HybridTimestampTracker.atomicTracker(null), txId, CLUSTER_NODE.id(), false, 10_000, colocationEnabled()
        );

        assertThat(tx.schemaTimestamp(), is(beginTs));
    }

    /**
     * Starts a transaction, enlists some partitions, finalizes the transaction, and again tries to enlist.
     *
     * @param commit True for committing the transaction, false for rolling back.
     */
    private void startTxAndTryToEnlist(boolean commit) {
        HashSet<UUID> finishedTxs = new HashSet<>();

        Mockito.when(txManager.finish(any(), any(), anyBoolean(), anyBoolean(), any(), any())).thenAnswer(invocation -> {
            finishedTxs.add(invocation.getArgument(5));

            return nullCompletedFuture();
        });

        Mockito.when(txManager.stateMeta(any())).thenAnswer(invocation -> {
            if (finishedTxs.contains(invocation.getArgument(0))) {
                return new TxStateMeta(txState, randomUUID(), txCommitPart, null, null, null);
            }

            return null;
        });

        HybridTimestamp beginTs = clock.now();

        UUID txId = TestTransactionIds.TRANSACTION_ID_GENERATOR.transactionIdFor(beginTs);

        var tx = new ReadWriteTransactionImpl(
                txManager, HybridTimestampTracker.atomicTracker(null), txId, CLUSTER_NODE.id(), false, 10_000, colocationEnabled()
        );

        tx.assignCommitPartition(txCommitPart);

        tx.enlist(targetReplicationGroupId(ZONE_ID, 0), TABLE_ID, CLUSTER_NODE.name(), 0L);
        tx.enlist(targetReplicationGroupId(ZONE_ID, 2), TABLE_ID, CLUSTER_NODE.name(), 0L);

        if (commit) {
            if (txState == null) {
                txState = COMMITTED;
            }

            tx.commit();
        } else {
            if (txState == null) {
                txState = ABORTED;
            }

            tx.rollback();
        }

        TransactionException ex = assertThrows(TransactionException.class,
                () -> tx.enlist(targetReplicationGroupId(ZONE_ID, 5), TABLE_ID, CLUSTER_NODE.name(), 0L));

        assertTrue(ex.getMessage().contains(txState.toString()));

        ex = assertThrows(
                TransactionException.class,
                () -> tx.enlist(targetReplicationGroupId(ZONE_ID, 0), TABLE_ID, CLUSTER_NODE.name(), 0L)
        );

        assertTrue(ex.getMessage().contains(txState.toString()));
    }

    @Test
    void testEnlistOnCommit() {
        startTxAndTryToEnlist(true);

        txState = FINISHING;

        startTxAndTryToEnlist(true);
    }

    @Test
    void testEnlistOnAbort() {
        startTxAndTryToEnlist(false);

        txState = FINISHING;

        startTxAndTryToEnlist(false);
    }
}
