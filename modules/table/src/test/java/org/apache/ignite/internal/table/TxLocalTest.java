/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.mockito.Mockito;

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
        ClusterService clusterService = Mockito.mock(ClusterService.class, RETURNS_DEEP_STUBS);
        Mockito.when(clusterService.topologyService().localMember().address()).thenReturn(DummyInternalTableImpl.ADDR);

        ReplicaService replicaService = Mockito.mock(ReplicaService.class, RETURNS_DEEP_STUBS);

        lockManager = new HeapLockManager();

        txManager = new TxManagerImpl(clusterService, replicaService, lockManager);

        igniteTransactions = new IgniteTransactionsImpl(txManager);

        AtomicLong accountsRaftIndex = new AtomicLong();

        DummyInternalTableImpl table = new DummyInternalTableImpl(
                txManager,
                accountsRaftIndex
        );

        accounts = new TableImpl(table, new DummySchemaManagerImpl(ACCOUNTS_SCHEMA));

        AtomicLong customersRaftIndex = new AtomicLong();
        DummyInternalTableImpl table2 = new DummyInternalTableImpl(
                txManager,
                customersRaftIndex
        );

        customers = new TableImpl(table2, new DummySchemaManagerImpl(CUSTOMERS_SCHEMA));

        List<PartitionListener> partitionListeners = List.of(table.getPartitionListener(), table2.getPartitionListener());

        Function<PartitionListener, AtomicLong> raftIndexfactory = pl ->
                pl == table.getPartitionListener() ? accountsRaftIndex : customersRaftIndex;

        MessagingService messagingService = MessagingServiceTestUtils.mockMessagingService(txManager, partitionListeners, raftIndexfactory);
        Mockito.when(clusterService.messagingService()).thenReturn(messagingService);
    }

    @Disabled
    @Override
    public void testScan() throws InterruptedException {
        // TODO asch IGNITE-15928 implement local scan
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
    protected boolean assertPartitionsSame(Table table, int partId) {
        return true;
    }
}
