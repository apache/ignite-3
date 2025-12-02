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

package org.apache.ignite.internal.partition.replicator.handlers;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toZonePartitionIdMessage;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.impl.EnlistedPartitionGroup;
import org.apache.ignite.internal.tx.message.TxCleanupRecoveryRequest;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageClosedException;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageDestroyedException;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageException;
import org.apache.ignite.internal.util.Cursor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TxCleanupRecoveryRequestHandlerTest extends BaseIgniteAbstractTest {
    private final TxMessagesFactory txMessagesFactory = new TxMessagesFactory();
    private final ReplicaMessagesFactory replicaMessagesFactory = new ReplicaMessagesFactory();

    private final ZonePartitionId replicationGroupId = new ZonePartitionId(0, 0);

    @Mock
    private TxStatePartitionStorage txStatePartitionStorage;

    @Mock
    private TxManager txManager;

    @Mock
    private FailureProcessor failureProcessor;

    private TxCleanupRecoveryRequestHandler handler;

    private final HybridClock clock = new HybridClockImpl();

    private final ZonePartitionId partition1Id = new ZonePartitionId(0, 1);
    private final ZonePartitionId partition2Id = new ZonePartitionId(0, 2);

    @BeforeEach
    void setUp() {
        handler = new TxCleanupRecoveryRequestHandler(txStatePartitionStorage, txManager, failureProcessor, replicationGroupId);
    }

    @Test
    void interruptsCleanupGracefullyOnStorageClose() {
        testInterruptsCleanupGracefullyOnStorageClosed(TxStateStorageClosedException::new);
    }

    @Test
    void interruptsCleanupGracefullyOnStorageDestruction() {
        testInterruptsCleanupGracefullyOnStorageClosed(TxStateStorageDestroyedException::new);
    }

    @SuppressWarnings("unchecked")
    private void testInterruptsCleanupGracefullyOnStorageClosed(Supplier<? extends TxStateStorageException> exceptionFactory) {
        Cursor<IgniteBiTuple<UUID, TxMeta>> cursor = mock(Cursor.class);
        Iterator<IgniteBiTuple<UUID, TxMeta>> iterator = mock(Iterator.class);

        when(txStatePartitionStorage.scan()).thenReturn(cursor);
        when(cursor.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenThrow(exceptionFactory.get());

        TxCleanupRecoveryRequest request = txMessagesFactory.txCleanupRecoveryRequest()
                .groupId(toZonePartitionIdMessage(replicaMessagesFactory, replicationGroupId))
                .build();
        assertThat(handler.handle(request), willCompleteSuccessfully());

        verify(failureProcessor, never()).process(any());
    }

    private static List<EnlistedPartitionGroup> tableEnlistedPartitions(ZonePartitionId... zonePartitionIds) {
        return Arrays.stream(zonePartitionIds)
                // Assume that partition ID equals to table ID and both tables belong to the same distribution zone.
                .map(zonePartitionId -> new EnlistedPartitionGroup(zonePartitionId, Set.of(zonePartitionId.partitionId())))
                .collect(toUnmodifiableList());
    }

    @Test
    void testCleanupThrottling() throws InterruptedException {
        handler = new TxCleanupRecoveryRequestHandler(txStatePartitionStorage, txManager, failureProcessor, replicationGroupId);

        Cursor<IgniteBiTuple<UUID, TxMeta>> cursorMock = mockTxCursor(1_500);
        when(txStatePartitionStorage.scan()).thenReturn(cursorMock);

        CopyOnWriteArrayList<CompletableFuture<?>> futures = new CopyOnWriteArrayList<>();
        when(txManager.cleanup(any(), any(Collection.class), anyBoolean(), any(), any()))
                .thenAnswer(invocation -> {
                    CompletableFuture<Void> cleanupFuture = new CompletableFuture<>();

                    futures.add(cleanupFuture);

                    return cleanupFuture;
                });

        TxCleanupRecoveryRequest request = txMessagesFactory.txCleanupRecoveryRequest()
                .groupId(toZonePartitionIdMessage(replicaMessagesFactory, replicationGroupId))
                .build();
        assertThat(handler.handle(request), willCompleteSuccessfully());

        assertTrue(waitForCondition(() -> futures.size() == 1000, 10_000),
                () -> "Cleanup batch is  " + futures.size());

        // Now "finish' the cleanup - the next batch should be processed.
        List<CompletableFuture<?>> toComplete = new ArrayList<>(futures);
        futures.clear();
        toComplete.forEach(f -> f.complete(null));

        assertTrue(waitForCondition(() -> futures.size() == 500, 10_000),
                () -> "Cleanup batch is  " + futures.size());

        futures.forEach(f -> f.complete(null));
    }

    private Cursor<IgniteBiTuple<UUID, TxMeta>> mockTxCursor(int txCount) {
        List<IgniteBiTuple<UUID, TxMeta>> tasks = new ArrayList<>();
        for (int i = 0; i < txCount; i++) {
            TxMeta txMeta = new TxMeta(TxState.COMMITTED, tableEnlistedPartitions(partition1Id, partition2Id), clock.now());
            tasks.add(new IgniteBiTuple<>(UUID.randomUUID(), txMeta));
        }

        Cursor<IgniteBiTuple<UUID, TxMeta>> cursor = mock(Cursor.class);
        when(cursor.iterator()).thenReturn(tasks.iterator());
        return cursor;
    }
}
