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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.TestReplicaMetaImpl;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.message.TransactionMetaMessage;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxStateResponse;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link TransactionStateResolver}.
 */
@ExtendWith(MockitoExtension.class)
public class TransactionStateResolverTest extends BaseIgniteAbstractTest {
    private static final TxMessagesFactory TX_MESSAGES_FACTORY = new TxMessagesFactory();

    private static final InternalClusterNode COORDINATOR_NODE =
            new ClusterNodeImpl(UUID.randomUUID(), "coordinator", new NetworkAddress("127.0.0.1", 10800), null);

    @Mock
    private TxManager txManager;

    @Mock
    private ClusterNodeResolver clusterNodeResolver;

    @Mock
    private MessagingService messagingService;

    @Mock
    private PlacementDriver placementDriver;

    @Mock
    private TxMessageSender txMessageSender;

    private final HybridClock clock = new HybridClockImpl();

    private final ClockService clockService = new TestClockService(clock);

    private TransactionStateResolver resolver;

    @BeforeEach
    void setUp() {
        resolver = new TransactionStateResolver(
                txManager,
                clockService,
                clusterNodeResolver,
                messagingService,
                placementDriver,
                txMessageSender
        );

        // Setup default mock for PlacementDriver to avoid timeouts.
        lenient().when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any()))
                .thenReturn(completedFuture(new TestReplicaMetaImpl(
                        COORDINATOR_NODE,
                        hybridTimestamp(1),
                        HybridTimestamp.MAX_VALUE
                )));
    }

    @Test
    void testResolveTxStateFromCoordinatorWithNullResponse() {
        UUID txId = UUID.randomUUID();
        UUID coordinatorId = COORDINATOR_NODE.id();
        ZonePartitionId commitPartitionId = new ZonePartitionId(1, 0);
        HybridTimestamp timestamp = clock.now();

        TxStateMeta pendingMeta = new TxStateMeta(TxState.PENDING, coordinatorId, commitPartitionId, null, null, null);

        when(txManager.stateMeta(txId)).thenReturn(pendingMeta);
        when(clusterNodeResolver.getById(coordinatorId)).thenReturn(COORDINATOR_NODE);

        // Mock TxStateResponse with null txStateMeta.
        TxStateResponse response = TX_MESSAGES_FACTORY.txStateResponse()
                .txStateMeta(null)
                .timestamp(timestamp)
                .build();

        when(txMessageSender.resolveTxStateFromCoordinator(
                    any(InternalClusterNode.class),
                    eq(txId),
                    any(HybridTimestamp.class),
                    any(Long.class),
                    any(ZonePartitionId.class)
                ))
                .thenReturn(completedFuture(response));

        // Mock the commit partition resolution to complete the future.
        when(txMessageSender.resolveTxStateFromCommitPartition(
                    any(String.class),
                    eq(txId),
                    eq(commitPartitionId),
                    any(Long.class),
                    any(Long.class),
                    any(ZonePartitionId.class))
                )
                .thenReturn(completedFuture(pendingMeta));

        CompletableFuture<TransactionMeta> result = resolver.resolveTxState(
                txId,
                commitPartitionId,
                timestamp,
                timestamp.longValue(),
                commitPartitionId
        );

        assertThat(result, willCompleteSuccessfully());

        // Verify that coordinator was called with InternalClusterNode.
        ArgumentCaptor<InternalClusterNode> nodeCaptor = ArgumentCaptor.forClass(InternalClusterNode.class);
        verify(txMessageSender).resolveTxStateFromCoordinator(
                nodeCaptor.capture(),
                eq(txId),
                any(HybridTimestamp.class),
                any(Long.class),
                any()
        );
        assertEquals(COORDINATOR_NODE, nodeCaptor.getValue());

        // Verify fallback to commit partition was called.
        verify(txMessageSender).resolveTxStateFromCommitPartition(
                any(String.class),
                eq(txId),
                eq(commitPartitionId),
                any(Long.class),
                any(Long.class),
                any()
        );
    }

    @Test
    void testResolveTxStateFromRestartedCoordinator() {
        UUID txId = UUID.randomUUID();
        UUID coordinatorId = COORDINATOR_NODE.id();
        ZonePartitionId commitPartitionId = new ZonePartitionId(1, 0);
        HybridTimestamp timestamp = clock.now();

        TxStateMeta pendingMeta = new TxStateMeta(TxState.PENDING, coordinatorId, commitPartitionId, null, null, null);

        when(txManager.stateMeta(txId)).thenReturn(pendingMeta);
        when(clusterNodeResolver.getById(coordinatorId)).thenReturn(COORDINATOR_NODE);

        CompletableFuture<TxStateResponse> failedFuture = CompletableFuture.failedFuture(new RecipientLeftException());

        when(txMessageSender.resolveTxStateFromCoordinator(
                    any(InternalClusterNode.class),
                    eq(txId),
                    any(HybridTimestamp.class),
                    any(Long.class),
                    any(ZonePartitionId.class))
                )
                .thenReturn(failedFuture);
        // Mock the commit partition resolution to complete the future.
        TxStateMeta abandonedMeta = new TxStateMeta(TxState.ABANDONED, coordinatorId, commitPartitionId, null, null, null);
        when(txMessageSender.resolveTxStateFromCommitPartition(
                    any(String.class),
                    eq(txId),
                    eq(commitPartitionId),
                    any(Long.class),
                    any(Long.class),
                    any(ZonePartitionId.class)
                ))
                .thenReturn(completedFuture(abandonedMeta));

        CompletableFuture<TransactionMeta> result = resolver.resolveTxState(
                txId,
                commitPartitionId,
                timestamp,
                timestamp.longValue(),
                commitPartitionId
        );

        assertThat(result, willCompleteSuccessfully());
        // Verify that transaction was marked as abandoned.
        ArgumentCaptor<Function<TxStateMeta, TxStateMeta>> updaterCaptor = ArgumentCaptor.forClass(Function.class);
        verify(txManager, atLeastOnce()).updateTxMeta(eq(txId), updaterCaptor.capture());
        // Passed state updater should result in abandon state.
        TxStateMeta afterUpdateState = updaterCaptor.getValue().apply(pendingMeta);
        assertEquals(TxState.ABANDONED, afterUpdateState.txState());
        // Verify fallback to commit partition was called.
        verify(txMessageSender).resolveTxStateFromCommitPartition(
                any(String.class),
                eq(txId),
                eq(commitPartitionId),
                any(Long.class),
                any(Long.class),
                any()
        );
    }

    @Test
    void testResolveTxStateFromCoordinatorWithValidResponse() {
        UUID txId = UUID.randomUUID();
        UUID coordinatorId = COORDINATOR_NODE.id();
        ZonePartitionId commitPartitionId = new ZonePartitionId(1, 0);
        HybridTimestamp timestamp = clock.now();

        TxStateMeta pendingMeta = new TxStateMeta(TxState.PENDING, coordinatorId, commitPartitionId, null, null, null);
        TxStateMeta committedMeta = new TxStateMeta(TxState.COMMITTED, coordinatorId, commitPartitionId, timestamp, null, null);

        when(txManager.stateMeta(txId)).thenReturn(pendingMeta);
        when(clusterNodeResolver.getById(coordinatorId)).thenReturn(COORDINATOR_NODE);

        // Create a valid TransactionMetaMessage.
        TransactionMetaMessage metaMessage = committedMeta.toTransactionMetaMessage(
                new ReplicaMessagesFactory(),
                TX_MESSAGES_FACTORY
        );

        TxStateResponse response = TX_MESSAGES_FACTORY.txStateResponse()
                .txStateMeta(metaMessage)
                .timestamp(timestamp)
                .build();

        when(txMessageSender.resolveTxStateFromCoordinator(
                    any(InternalClusterNode.class),
                    eq(txId),
                    any(HybridTimestamp.class),
                    any(Long.class),
                    any(ZonePartitionId.class)
                ))
                .thenReturn(completedFuture(response));

        CompletableFuture<TransactionMeta> result = resolver.resolveTxState(
                txId,
                commitPartitionId,
                timestamp,
                timestamp.longValue(),
                commitPartitionId
        );

        assertThat(result, willCompleteSuccessfully());

        // Verify that coordinator was called with InternalClusterNode.
        ArgumentCaptor<InternalClusterNode> nodeCaptor = ArgumentCaptor.forClass(InternalClusterNode.class);
        verify(txMessageSender).resolveTxStateFromCoordinator(
                nodeCaptor.capture(),
                eq(txId),
                any(HybridTimestamp.class),
                any(Long.class),
                any()
        );
        assertEquals(COORDINATOR_NODE, nodeCaptor.getValue());

        // Verify that commit partition fallback was NOT called.
        verify(txMessageSender, never())
                .resolveTxStateFromCommitPartition(
                        any(String.class),
                        eq(txId),
                        eq(commitPartitionId),
                        any(Long.class),
                        any(Long.class),
                        any()
                );

        // Verify that the transaction meta was updated.
        verify(txManager).updateTxMeta(eq(txId), any());
    }
}
