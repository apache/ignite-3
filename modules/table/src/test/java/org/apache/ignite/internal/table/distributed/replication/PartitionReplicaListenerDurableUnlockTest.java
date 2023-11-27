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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.tx.TxState.isFinalState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.TransactionStateResolver;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.table.distributed.schema.ValidationSchemasSource;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateStorage;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Class for testing durable unlock on commit partition recovery.
 */
@ExtendWith(MockitoExtension.class)
public class PartitionReplicaListenerDurableUnlockTest extends IgniteAbstractTest {
    private static final ClusterNode LOCAL_NODE = new ClusterNodeImpl("node1", "node1", NetworkAddress.from("127.0.0.1:127"));

    private static final int PART_ID = 0;

    private static final int TABLE_ID = 1;

    /** Hybrid clock. */
    private final HybridClock clock = new HybridClockImpl();

    private final TestPlacementDriver placementDriver = new TestPlacementDriver(LOCAL_NODE);

    /** The storage stores transaction states. */
    private final TestTxStateStorage txStateStorage = new TestTxStateStorage();

    @Mock
    private TxManager txManager;

    /** Partition replication listener to test. */
    private PartitionReplicaListener partitionReplicaListener;

    private BiFunction<UUID, TablePartitionId, CompletableFuture<Void>> cleanupCallback = (a, b) -> completedFuture(null);

    @BeforeEach
    public void beforeTest() {
        doAnswer(invocation -> {
            Runnable r = invocation.getArgument(0);
            r.run();
            return completedFuture(null);
        }).when(txManager).executeCleanupAsync(any(Runnable.class));

        doAnswer(invocation -> {
            Supplier<CompletableFuture<?>> s = invocation.getArgument(0);
            return s.get();
        }).when(txManager).executeCleanupAsync(any(Supplier.class));

        doAnswer(invocation -> {
            UUID txId = invocation.getArgument(2);
            TablePartitionId partitionId = invocation.getArgument(1);
            return cleanupCallback.apply(txId, partitionId);
        }).when(txManager).cleanup(anyString(), any(), any(), anyBoolean(), any());

        partitionReplicaListener = new PartitionReplicaListener(
                new TestMvPartitionStorage(PART_ID),
                mock(RaftGroupService.class),
                txManager,
                new HeapLockManager(),
                Runnable::run,
                PART_ID,
                TABLE_ID,
                Map::of,
                new Lazy<>(null),
                Map::of,
                clock,
                new PendingComparableValuesTracker<>(new HybridTimestamp(1, 1)),
                txStateStorage,
                mock(TransactionStateResolver.class),
                mock(StorageUpdateHandler.class),
                mock(ValidationSchemasSource.class),
                LOCAL_NODE,
                mock(SchemaSyncService.class),
                mock(CatalogService.class),
                placementDriver
        );
    }

    @Test
    public void testOnlyFinishedAreCleanedUp() {
        UUID tx0 = TestTransactionIds.newTransactionId();
        UUID tx1 = TestTransactionIds.newTransactionId();
        UUID tx2 = TestTransactionIds.newTransactionId();

        TablePartitionId part0 = new TablePartitionId(TABLE_ID, PART_ID);
        TablePartitionId part1 = new TablePartitionId(TABLE_ID, 1);

        txStateStorage.put(tx0, new TxMeta(TxState.PENDING, List.of(part0, part1), null));
        txStateStorage.put(tx1, new TxMeta(TxState.COMMITED, List.of(part0), clock.now()));
        txStateStorage.put(tx2, new TxMeta(TxState.ABORTED, List.of(part0), null));

        cleanupCallback = (tx, partId) -> {
            assertTrue(isFinalState(txStateStorage.get(tx).txState()));

            return completedFuture(null);
        };

        PrimaryReplicaEventParameters parameters = new PrimaryReplicaEventParameters(0, part0, LOCAL_NODE.name(), clock.now());

        assertThat(placementDriver.fireEvent(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, parameters), willSucceedIn(1, SECONDS));

        for (IgniteBiTuple<UUID, TxMeta> tx : txStateStorage.scan()) {
            if (isFinalState(tx.getValue().txState())) {
                assertTrue(tx.getValue().locksReleased());
            }
        }
    }

    @Test
    public void testRepeatedUntilSuccess() {
        UUID tx0 = TestTransactionIds.newTransactionId();
        TablePartitionId part0 = new TablePartitionId(TABLE_ID, PART_ID);
        txStateStorage.put(tx0, new TxMeta(TxState.COMMITED, List.of(part0), null));

        int exCnt = 3;
        AtomicInteger exceptionCounter = new AtomicInteger(exCnt);

        cleanupCallback = (tx, partId) -> {
            assertThat(exceptionCounter.get(), greaterThan(0));

            if (exceptionCounter.decrementAndGet() > 0) {
                throw new RuntimeException("test exception");
            }

            return completedFuture(null);
        };

        PrimaryReplicaEventParameters parameters = new PrimaryReplicaEventParameters(0, part0, LOCAL_NODE.name(), clock.now());

        assertThat(placementDriver.fireEvent(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, parameters), willSucceedIn(1, SECONDS));

        assertTrue(txStateStorage.get(tx0).locksReleased());

        assertEquals(0, exceptionCounter.get());
    }

    @Test
    public void testCantGetPrimaryReplicaForEnlistedPartition() throws InterruptedException {
        UUID tx0 = TestTransactionIds.newTransactionId();
        TablePartitionId part0 = new TablePartitionId(TABLE_ID, PART_ID);
        txStateStorage.put(tx0, new TxMeta(TxState.COMMITED, List.of(part0), null));

        cleanupCallback = (tx, partId) -> completedFuture(null);

        CompletableFuture<ReplicaMeta> primaryReplicaFuture = new CompletableFuture<>();
        placementDriver.setAwaitPrimaryReplicaFunction((groupId, timestamp) -> primaryReplicaFuture);

        PrimaryReplicaEventParameters parameters = new PrimaryReplicaEventParameters(0, part0, LOCAL_NODE.name(), clock.now());
        assertThat(placementDriver.fireEvent(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, parameters), willSucceedIn(1, SECONDS));

        assertFalse(txStateStorage.get(tx0).locksReleased());

        Thread primaryReplicaFutureCompleteThread =
                new Thread(() -> primaryReplicaFuture.completeExceptionally(new RuntimeException("test exception")));
        primaryReplicaFutureCompleteThread.start();

        assertFalse(txStateStorage.get(tx0).locksReleased());

        placementDriver.setAwaitPrimaryReplicaFunction(null);

        primaryReplicaFutureCompleteThread.join();

        assertTrue(txStateStorage.get(tx0).locksReleased());
    }
}
