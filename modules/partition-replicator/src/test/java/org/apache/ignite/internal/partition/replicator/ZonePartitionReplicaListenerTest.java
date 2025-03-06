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

package org.apache.ignite.internal.partition.replicator;

import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.placementdriver.TestReplicaMetaImpl;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaRequest;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.message.TxStateCommitPartitionRequest;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStatePartitionStorage;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ZonePartitionReplicaListenerTest extends BaseIgniteAbstractTest {
    private final ClusterNode localNode = new ClusterNodeImpl(nodeId(1), "node1", NetworkAddress.from("127.0.0.1:127"));

    private final ClusterNode anotherNode = new ClusterNodeImpl(nodeId(2), "node2", NetworkAddress.from("127.0.0.2:127"));

    @Spy
    private final TxStatePartitionStorage txStateStorage = new TestTxStatePartitionStorage();

    private final HybridClock clock = new HybridClockImpl();

    private final ClockService clockService = new TestClockService(clock);

    @Mock
    private TxManager txManager;

    @Mock
    private ValidationSchemasSource validationSchemasSource;

    @Mock
    private SchemaSyncService schemaSyncService;

    @Mock
    private CatalogService catalogService;

    @Spy
    private final TestPlacementDriver placementDriver = new TestPlacementDriver(localNode);

    @Mock
    private ClusterNodeResolver clusterNodeResolver;

    @Mock
    private RaftCommandRunner raftClient;

    private final ZonePartitionId groupId = new ZonePartitionId(1, 2);

    private ZonePartitionReplicaListener partitionReplicaListener;

    private static UUID nodeId(int id) {
        return new UUID(0, id);
    }

    @BeforeEach
    void setUp() {
        partitionReplicaListener = new ZonePartitionReplicaListener(
                txStateStorage,
                clockService,
                txManager,
                validationSchemasSource,
                schemaSyncService,
                catalogService,
                placementDriver,
                clusterNodeResolver,
                raftClient,
                localNode,
                groupId
        );
    }

    @ParameterizedTest
    @ValueSource(classes = {PrimaryReplicaRequest.class, TxStateCommitPartitionRequest.class})
    void primaryReplicaRequestsAreRejectedWhenPrimaryIsNotKnown(Class<? extends PrimaryReplicaRequest> requestClass) {
        doReturn(null).when(placementDriver).getCurrentPrimaryReplica(any(), any());
        doReturn(nullCompletedFuture()).when(placementDriver).getPrimaryReplica(any(), any());

        PrimaryReplicaRequest request = mock(requestClass);

        assertThat(partitionReplicaListener.invoke(request, localNode.id()), willThrow(PrimaryReplicaMissException.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {PrimaryReplicaRequest.class, TxStateCommitPartitionRequest.class})
    void primaryReplicaRequestsAreRejectedWhenPrimaryDoesNotMatchLeaseStartTime(Class<? extends PrimaryReplicaRequest> requestClass) {
        long leaseStartTime = clock.nowLong();
        placementDriver.setPrimaryReplicaSupplier(
                () -> new TestReplicaMetaImpl(localNode, hybridTimestamp(leaseStartTime), HybridTimestamp.MAX_VALUE)
        );

        PrimaryReplicaRequest request = mock(requestClass);
        when(request.enlistmentConsistencyToken()).thenReturn(leaseStartTime - 1000);

        assertThat(partitionReplicaListener.invoke(request, localNode.id()), willThrow(PrimaryReplicaMissException.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {PrimaryReplicaRequest.class, TxStateCommitPartitionRequest.class})
    void primaryReplicaRequestsAreRejectedWhenLeaseIsExpired(Class<? extends PrimaryReplicaRequest> requestClass) {
        long leaseStartTime = clock.nowLong();
        placementDriver.setPrimaryReplicaSupplier(
                () -> new TestReplicaMetaImpl(localNode, hybridTimestamp(leaseStartTime), HybridTimestamp.MIN_VALUE)
        );

        PrimaryReplicaRequest request = mock(requestClass);
        when(request.enlistmentConsistencyToken()).thenReturn(leaseStartTime);

        assertThat(partitionReplicaListener.invoke(request, localNode.id()), willThrow(PrimaryReplicaMissException.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {PrimaryReplicaRequest.class, TxStateCommitPartitionRequest.class})
    void primaryReplicaRequestsAreRejectedWhenLeaseholderIsDifferent(Class<? extends PrimaryReplicaRequest> requestClass) {
        long leaseStartTime = clock.nowLong();
        placementDriver.setPrimaryReplicaSupplier(
                () -> new TestReplicaMetaImpl(anotherNode, hybridTimestamp(leaseStartTime), HybridTimestamp.MAX_VALUE)
        );

        PrimaryReplicaRequest request = mock(requestClass);
        when(request.enlistmentConsistencyToken()).thenReturn(leaseStartTime);

        assertThat(partitionReplicaListener.invoke(request, localNode.id()), willThrow(PrimaryReplicaMissException.class));
    }
}
