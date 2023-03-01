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

package org.apache.ignite.internal.table;

import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.table.distributed.replicator.PlacementDriver;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxStateReplicaRequest;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;

/**
 * Local table tests.
 */
public class TxLocalTest extends TxAbstractTest {
    private HeapLockManager lockManager;

    private TxManagerImpl txManager;

    /**
     * Initialize the test state.
     */
    @Override
    @BeforeEach
    public void before() {
        ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);
        when(clusterService.topologyService().localMember().address()).thenReturn(DummyInternalTableImpl.ADDR);

        lockManager = new HeapLockManager();

        ReplicaService replicaSvc = mock(ReplicaService.class, RETURNS_DEEP_STUBS);
        PlacementDriver placementDriver = mock(PlacementDriver.class, RETURNS_DEEP_STUBS);

        Map<ReplicationGroupId, DummyInternalTableImpl> tables = new HashMap<>();

        lenient().doAnswer(
            invocationOnMock -> {
                    ReplicaRequest request = invocationOnMock.getArgument(1);
                    ReplicaListener replicaListener = tables.get(request.groupId()).getReplicaListener();

                    CompletableFuture<Object> invoke = replicaListener.invoke(request);
                    return invoke;
            }
        ).when(replicaSvc).invoke(any(ClusterNode.class), any());

        doAnswer(invocationOnMock -> {
            TxStateReplicaRequest request = invocationOnMock.getArgument(1);

            return CompletableFuture.completedFuture(
                    tables.get(request.groupId()).txStateStorage().getTxStateStorage(0).get(request.txId()));
        }).when(placementDriver).sendMetaRequest(any(), any());

        txManager = new TxManagerImpl(replicaSvc, lockManager, new HybridClockImpl());

        igniteTransactions = new IgniteTransactionsImpl(txManager);

        DummyInternalTableImpl table = new DummyInternalTableImpl(replicaSvc, txManager, true, placementDriver, ACCOUNTS_SCHEMA);

        accounts = new TableImpl(table, new DummySchemaManagerImpl(ACCOUNTS_SCHEMA), lockManager);

        DummyInternalTableImpl table2 = new DummyInternalTableImpl(replicaSvc, txManager, true, placementDriver, CUSTOMERS_SCHEMA);

        customers = new TableImpl(table2, new DummySchemaManagerImpl(CUSTOMERS_SCHEMA), lockManager);

        when(clusterService.messagingService()).thenReturn(mock(MessagingService.class, RETURNS_DEEP_STUBS));

        tables.put(table.groupId(), table);
        tables.put(table2.groupId(), table2);
    }

    @Disabled
    @Override
    public void testScan() throws InterruptedException {
        // TODO asch IGNITE-15928 implement local scan
    }

    @Override
    protected TxManager clientTxManager() {
        return txManager;
    }

    @Override
    protected TxManager txManager(Table t) {
        return txManager;
    }

    @Override
    protected LockManager lockManager(Table t) {
        return lockManager;
    }

    @Override
    protected boolean assertPartitionsSame(TableImpl table, int partId) {
        return true;
    }
}
