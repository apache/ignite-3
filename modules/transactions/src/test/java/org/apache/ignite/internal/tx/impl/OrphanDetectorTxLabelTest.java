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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests tx labels usage in exception messages / logs on OrphanDetector-related flows.
 */
@ExtendWith(MockitoExtension.class)
public class OrphanDetectorTxLabelTest extends BaseIgniteAbstractTest {
    private static final UUID LOCAL_NODE_ID = randomUUID();
    private static final String LOCAL_NODE_NAME = "local";

    private static final String REMOTE_NODE_NAME = "remote";

    @Mock(answer = RETURNS_DEEP_STUBS)
    private TopologyService topologyService;

    @Mock(answer = RETURNS_DEEP_STUBS)
    private ReplicaService replicaService;

    @Mock
    private PlacementDriver placementDriver;

    private final HybridClock clock = new HybridClockImpl();

    private final ClockService clockService = new TestClockService(clock);

    private VolatileTxStateMetaStorage txStateMetaStorage;

    private HeapLockManager lockManager;

    private TransactionIdGenerator idGenerator;

    private OrphanDetector orphanDetector;

    private InternalClusterNode remoteNode;

    @BeforeEach
    void setUp() {
        idGenerator = new TransactionIdGenerator(LOCAL_NODE_NAME.hashCode());

        remoteNode = mock(InternalClusterNode.class);

        txStateMetaStorage = VolatileTxStateMetaStorage.createStarted();

        lockManager = new HeapLockManager(1024, txStateMetaStorage);
        lockManager.start(new WaitDieDeadlockPreventionPolicy());

        PlacementDriverHelper placementDriverHelper = new PlacementDriverHelper(placementDriver, clockService);

        orphanDetector = new OrphanDetector(
                topologyService,
                replicaService,
                placementDriverHelper,
                lockManager,
                Runnable::run
        );

        orphanDetector.start(txStateMetaStorage, () -> 30_000L);
    }

    @AfterEach
    void tearDown() {
        orphanDetector.stop();
    }

    @Test
    void txLabelIsPresentInDeadlockExceptionMessageWhenLockIsMarkedAbandoned() {
        UUID orphanTxId = idGenerator.transactionIdFor(clock.now());
        UUID concurrentTxId = idGenerator.transactionIdFor(clock.now());

        String orphanLabel = "orphan-label";
        String concurrentLabel = "concurrent-label";

        ZonePartitionId zonePartitionId = new ZonePartitionId(1, 0);
        RowId rowId = new RowId(zonePartitionId.partitionId());

        // Put labels into the storage used by both the lock manager and OrphanDetector.
        txStateMetaStorage.updateMeta(
                orphanTxId,
                old -> new TxStateMeta(PENDING, LOCAL_NODE_ID, zonePartitionId, null, null, null, null, null, orphanLabel)
        );
        txStateMetaStorage.updateMeta(
                concurrentTxId,
                old -> new TxStateMeta(PENDING, LOCAL_NODE_ID, zonePartitionId, null, null, null, null, null, concurrentLabel)
        );

        ReplicaMeta replicaMeta = mock(ReplicaMeta.class);
        when(replicaMeta.getLeaseholder()).thenReturn(REMOTE_NODE_NAME);
        when(replicaMeta.getStartTime()).thenReturn(hybridTimestamp(1));

        when(placementDriver.awaitPrimaryReplica(eq(zonePartitionId), any(), anyLong(), any()))
                .thenReturn(completedFuture(replicaMeta));

        // Coordinator is dead.
        when(topologyService.getById(eq(LOCAL_NODE_ID))).thenReturn(null);

        int tableId = 2;

        lockManager.acquire(orphanTxId, new LockKey(tableId, rowId), LockMode.X);

        CompletableFuture<Lock> acquire = lockManager.acquire(concurrentTxId, new LockKey(tableId, rowId), LockMode.X);

        assertThat(acquire, willThrow(LockException.class, "Failed to acquire the abandoned lock due to a possible deadlock"));
        assertThat(acquire, willThrow(LockException.class, "txLabel=" + concurrentLabel));
        assertThat(acquire, willThrow(LockException.class, "txLabel=" + orphanLabel));
    }

    @Test
    void txLabelIsPresentInOrphanDetectorRecoveryFailureLog() throws Exception {
        UUID orphanTxId = idGenerator.transactionIdFor(clock.now());
        UUID concurrentTxId = idGenerator.transactionIdFor(clock.now());

        String orphanLabel = "orphan-log-label";

        ZonePartitionId zonePartitionId = new ZonePartitionId(1, 0);
        RowId rowId = new RowId(zonePartitionId.partitionId());

        txStateMetaStorage.updateMeta(
                orphanTxId,
                old -> new TxStateMeta(PENDING, LOCAL_NODE_ID, zonePartitionId, null, null, null, null, null, orphanLabel)
        );

        ReplicaMeta replicaMeta = mock(ReplicaMeta.class);
        when(replicaMeta.getLeaseholder()).thenReturn(REMOTE_NODE_NAME);
        when(replicaMeta.getStartTime()).thenReturn(hybridTimestamp(1));

        when(placementDriver.awaitPrimaryReplica(eq(zonePartitionId), any(), anyLong(), any()))
                .thenReturn(completedFuture(replicaMeta));

        // Coordinator is dead.
        when(topologyService.getById(eq(LOCAL_NODE_ID))).thenReturn(null);

        // Ensure we can resolve the commit partition's primary, and then fail sending a recovery message.
        when(topologyService.getByConsistentId(eq(REMOTE_NODE_NAME))).thenReturn(remoteNode);
        when(replicaService.invoke(eq(remoteNode), any())).thenReturn(failedFuture(new RuntimeException("boom")));

        LogInspector logInspector = new LogInspector(
                OrphanDetector.class.getName(),
                evt -> evt.getLevel() == Level.WARN
                        && evt.getMessage().getFormattedMessage()
                        .contains("A recovery message for the transaction was handled with the error")
                        && evt.getMessage().getFormattedMessage()
                        .contains("txLabel=" + orphanLabel)
        );

        logInspector.start();

        try {
            int tableId = 2;

            lockManager.acquire(orphanTxId, new LockKey(tableId, rowId), LockMode.X);

            lockManager.acquire(concurrentTxId, new LockKey(tableId, rowId), LockMode.X);

            assertTrue(IgniteTestUtils.waitForCondition(logInspector::isMatched, 1_000));
        } finally {
            logInspector.stop();
        }
    }
}


