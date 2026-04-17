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
import static org.apache.ignite.internal.tx.impl.PersistentTxStateVacuumizer.VACUUM_BATCH_SIZE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.TestReplicaMetaImpl;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.impl.PersistentTxStateVacuumizer.PersistentTxStateVacuumResult;
import org.apache.ignite.internal.tx.impl.PersistentTxStateVacuumizer.VacuumizableTx;
import org.apache.ignite.internal.tx.message.VacuumTxStateReplicaRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PersistentTxStateVacuumizerTest extends BaseIgniteAbstractTest {
    @Mock
    private ReplicaService replicaService;

    @Mock
    private InternalClusterNode localNode;

    @Mock
    private PlacementDriver placementDriver;

    @Captor
    private ArgumentCaptor<VacuumTxStateReplicaRequest> requestCaptor;

    private PersistentTxStateVacuumizer vacuumizer;

    @BeforeEach
    void setUp() {
        UUID localNodeId = UUID.randomUUID();
        when(localNode.id()).thenReturn(localNodeId);
        when(localNode.name()).thenReturn("test-node");

        TestReplicaMetaImpl replicaMeta = new TestReplicaMetaImpl(localNode.name(), localNodeId);

        when(placementDriver.getPrimaryReplica(any(), any()))
                .thenReturn(completedFuture(replicaMeta));
        lenient().when(replicaService.invoke(any(InternalClusterNode.class), requestCaptor.capture()))
                .thenReturn(completedFuture(null));

        vacuumizer = new PersistentTxStateVacuumizer(
                replicaService,
                localNode,
                new TestClockService(new HybridClockImpl()),
                placementDriver
        );
    }

    @Test
    void smallBatchSentAsSingleRequest() {
        int count = 10;
        Map<ZonePartitionId, Set<VacuumizableTx>> txIds = createTxIds(count);

        PersistentTxStateVacuumResult result = vacuumizer.vacuumPersistentTxStates(txIds).join();

        assertEquals(count, result.vacuumizedPersistentTxnStatesCount);
        assertEquals(count, result.txnsToVacuum.size());
        assertEquals(1, requestCaptor.getAllValues().size());
        assertEquals(count, requestCaptor.getValue().transactionIds().size());
    }

    @Test
    void largeBatchIsSplitIntoMultipleRequests() {
        int count = VACUUM_BATCH_SIZE * 3 + 1;
        Map<ZonePartitionId, Set<VacuumizableTx>> txIds = createTxIds(count);

        PersistentTxStateVacuumResult result = vacuumizer.vacuumPersistentTxStates(txIds).join();

        assertEquals(count, result.vacuumizedPersistentTxnStatesCount);
        assertEquals(count, result.txnsToVacuum.size());

        // Should be split into 4 requests.
        assertEquals(4, requestCaptor.getAllValues().size());

        // Each request should have at most VACUUM_BATCH_SIZE tx IDs.
        assertThat(
                requestCaptor.getAllValues().stream().map(r -> r.transactionIds().size()).toList(),
                everyItem(lessThanOrEqualTo(VACUUM_BATCH_SIZE))
        );

        // All tx IDs should be covered.
        Set<UUID> allSentIds = new HashSet<>();
        for (VacuumTxStateReplicaRequest req : requestCaptor.getAllValues()) {
            allSentIds.addAll(req.transactionIds());
        }
        assertEquals(count, allSentIds.size());
    }

    @Test
    void partialBatchFailureDoesNotAffectOtherBatches() {
        int count = VACUUM_BATCH_SIZE * 2;
        Map<ZonePartitionId, Set<VacuumizableTx>> txIds = createTxIds(count);

        // First invocation succeeds, second fails.
        when(replicaService.invoke(any(InternalClusterNode.class), any(VacuumTxStateReplicaRequest.class)))
                .thenReturn(completedFuture(null))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("test failure")));

        PersistentTxStateVacuumResult result = vacuumizer.vacuumPersistentTxStates(txIds).join();

        // Only the first batch's tx IDs should be in the successful set.
        assertEquals(VACUUM_BATCH_SIZE, result.vacuumizedPersistentTxnStatesCount);
        assertEquals(VACUUM_BATCH_SIZE, result.txnsToVacuum.size());
    }

    @Test
    void txsWithoutCleanupTimestampAreSuccessfulWithoutRequest() {
        ZonePartitionId partitionId = new ZonePartitionId(1, 0);
        Set<VacuumizableTx> txs = new HashSet<>();

        // Tx without cleanup timestamp — should go directly to successful.
        UUID txWithoutCleanup = UUID.randomUUID();
        txs.add(new VacuumizableTx(txWithoutCleanup, null));

        Map<ZonePartitionId, Set<VacuumizableTx>> txIds = Map.of(partitionId, txs);

        PersistentTxStateVacuumResult result = vacuumizer.vacuumPersistentTxStates(txIds).join();

        assertTrue(result.txnsToVacuum.contains(txWithoutCleanup));
        assertEquals(0, result.vacuumizedPersistentTxnStatesCount);
        // No requests should be sent since the only tx has no cleanup timestamp.
        assertEquals(0, requestCaptor.getAllValues().size());
    }

    private static Map<ZonePartitionId, Set<VacuumizableTx>> createTxIds(int count) {
        ZonePartitionId partitionId = new ZonePartitionId(1, 0);
        Set<VacuumizableTx> txs = new HashSet<>();

        for (int i = 0; i < count; i++) {
            txs.add(new VacuumizableTx(UUID.randomUUID(), 1L));
        }

        return Map.of(partitionId, txs);
    }
}
