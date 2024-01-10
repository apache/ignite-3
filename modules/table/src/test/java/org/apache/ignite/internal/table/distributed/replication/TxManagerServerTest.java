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

package org.apache.ignite.internal.table.distributed.replication;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.replicator.ReplicaManager.DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.function.LongSupplier;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.TestReplicaMetaImpl;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.replicator.IncompatibleSchemaAbortException;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for a transaction manager with server-side specifics.
 */
@ExtendWith({MockitoExtension.class, ConfigurationExtension.class})
public class TxManagerServerTest {
    private static final ClusterNode LOCAL_NODE = new ClusterNodeImpl("local_id", "local", new NetworkAddress("127.0.0.1", 2004), null);

    /** Timestamp tracker. */
    private HybridTimestampTracker hybridTimestampTracker = new HybridTimestampTracker();

    private final LongSupplier idleSafeTimePropagationPeriodMsSupplier = () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;

    private TxManager txManager;

    @Mock
    private ClusterService clusterService;

    @Mock
    private ReplicaService replicaService;

    private final HybridClock clock = spy(new HybridClockImpl());

    @Mock
    private PlacementDriver placementDriver;

    @InjectConfiguration
    private TransactionConfiguration txConfiguration;

    /** Init test callback. */
    @BeforeEach
    public void setup() {
        clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);

        when(clusterService.topologyService().localMember().address()).thenReturn(LOCAL_NODE.address());

        replicaService = mock(ReplicaService.class, RETURNS_DEEP_STUBS);

        when(replicaService.invoke(any(ClusterNode.class), any())).thenReturn(nullCompletedFuture());

        when(replicaService.invoke(anyString(), any())).thenReturn(nullCompletedFuture());

        txManager = new TxManagerImpl(
                txConfiguration,
                clusterService,
                replicaService,
                new HeapLockManager(),
                clock,
                new TransactionIdGenerator(0xdeadbeef),
                placementDriver,
                idleSafeTimePropagationPeriodMsSupplier
        );

        txManager.start();
    }

    @AfterEach
    public void tearDown() throws Exception {
        txManager.beforeNodeStop();
        txManager.stop();
    }

    @Disabled("Looks like we have a bug")
    @Test
    public void testServerChangesCommitToAbort() {
        when(placementDriver.getPrimaryReplica(any(), any())).thenReturn(completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), HybridTimestamp.MAX_VALUE)));
        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), HybridTimestamp.MAX_VALUE)));

        // Server changes the outcome to ABORTED because of a schema error.
        when(replicaService.invoke(anyString(), any(TxFinishReplicaRequest.class)))
                .thenReturn(failedFuture(new IncompatibleSchemaAbortException("Test schema error")));
        // Ensure that commit doesn't throw exceptions.
        InternalTransaction committedTransaction = prepareTransaction();

        assertThrowsWithCause(committedTransaction::commit, IncompatibleSchemaAbortException.class);

        assertEquals(TxState.ABORTED, txManager.stateMeta(committedTransaction.id()).txState());
    }

    private InternalTransaction prepareTransaction() {
        InternalTransaction tx = txManager.begin(hybridTimestampTracker);

        ClusterNode node = mock(ClusterNode.class);

        TablePartitionId tablePartitionId1 = new TablePartitionId(1, 0);
        tx.enlist(tablePartitionId1, new IgniteBiTuple<>(node, 1L));
        tx.assignCommitPartition(tablePartitionId1);

        return tx;
    }
}
