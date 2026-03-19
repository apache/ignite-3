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

package org.apache.ignite.internal.tx;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testSyncExecutorService;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testSyncScheduledExecutorService;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.TestReplicaMetaImpl;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.PlacementDriverHelper;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TxCleanupRequestSender;
import org.apache.ignite.internal.tx.impl.TxMessageSender;
import org.apache.ignite.internal.tx.impl.VolatileTxStateMetaStorage;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for a transaction cleanup.
 */
@ExtendWith({MockitoExtension.class, ConfigurationExtension.class})
public class TxCleanupTest extends IgniteAbstractTest {

    private static final InternalClusterNode LOCAL_NODE =
            new ClusterNodeImpl(randomUUID(), "local", new NetworkAddress("127.0.0.1", 2024), null);

    private static final InternalClusterNode REMOTE_NODE =
            new ClusterNodeImpl(randomUUID(), "remote", new NetworkAddress("127.1.1.1", 2024), null);

    private static final ZonePartitionId ZONE_PARTITION_ID_1 = new ZonePartitionId(1, 0);
    private static final ZonePartitionId ZONE_PARTITION_ID_2 = new ZonePartitionId(2, 0);
    private static final ZonePartitionId ZONE_PARTITION_ID_3 = new ZonePartitionId(3, 0);

    private static final int TABLE_ID_1 = 10;
    private static final int TABLE_ID_2 = 20;
    private static final int TABLE_ID_3 = 30;

    private static final Map<ZonePartitionId, PartitionEnlistment> PARTITIONS = Map.of(
            ZONE_PARTITION_ID_1, new PartitionEnlistment(LOCAL_NODE.name(), Set.of(TABLE_ID_1)),
            ZONE_PARTITION_ID_2, new PartitionEnlistment(LOCAL_NODE.name(), Set.of(TABLE_ID_2)),
            ZONE_PARTITION_ID_3, new PartitionEnlistment(LOCAL_NODE.name(), Set.of(TABLE_ID_3))
    );

    @InjectConfiguration
    private TransactionConfiguration transactionConfiguration;

    @Mock(answer = RETURNS_DEEP_STUBS)
    private MessagingService messagingService;

    @Mock(answer = RETURNS_DEEP_STUBS)
    private TopologyService topologyService;

    @Mock(answer = RETURNS_DEEP_STUBS)
    private ReplicaService replicaService;

    @Mock
    private PlacementDriver placementDriver;

    private final HybridClock clock = new HybridClockImpl();

    private final ClockService clockService = new TestClockService(clock);

    private TxCleanupRequestSender cleanupRequestSender;

    private TransactionIdGenerator idGenerator;

    private TxMessageSender txMessageSender;

    /** Init test callback. */
    @BeforeEach
    public void setup() {
        when(topologyService.localMember().address()).thenReturn(LOCAL_NODE.address());

        when(messagingService.invoke(anyString(), any(), anyLong())).thenReturn(nullCompletedFuture());

        idGenerator = new TransactionIdGenerator(LOCAL_NODE.name().hashCode());

        txMessageSender = spy(
                new TxMessageSender(
                        messagingService,
                        replicaService,
                        clockService
                )
        );

        PlacementDriverHelper placementDriverHelper = new PlacementDriverHelper(placementDriver, clockService);

        cleanupRequestSender = new TxCleanupRequestSender(
                txMessageSender,
                placementDriverHelper,
                mock(VolatileTxStateMetaStorage.class),
                testSyncExecutorService(),
                testSyncScheduledExecutorService()
        );
    }

    @Test
    void testCleanupAllNodes() {
        HybridTimestamp beginTimestamp = clock.now();
        UUID txId = idGenerator.transactionIdFor(beginTimestamp);

        HybridTimestamp commitTimestamp = clock.now();

        CompletableFuture<Void> cleanup = cleanupRequestSender.cleanup(ZONE_PARTITION_ID_1, PARTITIONS, true, commitTimestamp, txId);

        assertThat(cleanup, willCompleteSuccessfully());

        verify(txMessageSender, times(1)).cleanup(any(), any(), any(), anyBoolean(), any());
        verifyNoMoreInteractions(txMessageSender);
    }

    @Test
    void testPrimaryNotFoundForSomeAfterException() {
        // First cleanup fails:
        when(messagingService.invoke(anyString(), any(), anyLong()))
                .thenReturn(failedFuture(new IOException("Test failure")), nullCompletedFuture());

        when(placementDriver.getPrimaryReplica(any(), any()))
                .thenReturn(completedFuture(new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), HybridTimestamp.MAX_VALUE)));
        when(placementDriver.getPrimaryReplica(eq(ZONE_PARTITION_ID_1), any()))
                .thenReturn(nullCompletedFuture());

        when(placementDriver.awaitPrimaryReplica(eq(ZONE_PARTITION_ID_1), any(), anyLong(), any()))
                .thenReturn(completedFuture(new TestReplicaMetaImpl(REMOTE_NODE, hybridTimestamp(1), HybridTimestamp.MAX_VALUE)));

        HybridTimestamp beginTimestamp = clock.now();
        UUID txId = idGenerator.transactionIdFor(beginTimestamp);

        HybridTimestamp commitTimestamp = clock.now();

        CompletableFuture<Void> cleanup = cleanupRequestSender.cleanup(ZONE_PARTITION_ID_1, PARTITIONS, true, commitTimestamp, txId);

        assertThat(cleanup, willCompleteSuccessfully());

        verify(txMessageSender, times(3)).cleanup(any(), any(), any(), anyBoolean(), any());
        verifyNoMoreInteractions(txMessageSender);
    }

    @Test
    void testPrimaryNotFoundForAll() {
        // First cleanup fails:
        when(messagingService.invoke(anyString(), any(), anyLong()))
                .thenReturn(failedFuture(new IOException("Test failure")), nullCompletedFuture());

        when(placementDriver.getPrimaryReplica(any(), any()))
                .thenReturn(nullCompletedFuture());

        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any()))
                .thenReturn(completedFuture(new TestReplicaMetaImpl(REMOTE_NODE, hybridTimestamp(1), HybridTimestamp.MAX_VALUE)));

        HybridTimestamp beginTimestamp = clock.now();
        UUID txId = idGenerator.transactionIdFor(beginTimestamp);

        HybridTimestamp commitTimestamp = clock.now();

        CompletableFuture<Void> cleanup = cleanupRequestSender.cleanup(ZONE_PARTITION_ID_1, PARTITIONS, true, commitTimestamp, txId);

        assertThat(cleanup, willCompleteSuccessfully());

        verify(txMessageSender, times(2)).cleanup(any(), any(), any(), anyBoolean(), any());

        verifyNoMoreInteractions(txMessageSender);
    }
}
