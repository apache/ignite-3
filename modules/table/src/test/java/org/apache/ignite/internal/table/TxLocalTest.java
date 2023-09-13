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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.TimestampAware;
import org.apache.ignite.internal.table.distributed.replicator.TransactionStateResolver;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
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
    @BeforeEach
    public void before() {
        ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);
        when(clusterService.topologyService().localMember().address()).thenReturn(DummyInternalTableImpl.ADDR);

        lockManager = new HeapLockManager();

        ReplicaMessagesFactory replicaMessagesFactory = new ReplicaMessagesFactory();

        HybridClockImpl localClock = new HybridClockImpl();
        MessagingService msgSvc = mock(MessagingService.class, RETURNS_DEEP_STUBS);
        ReplicaService replicaSvc = new ReplicaService(msgSvc, localClock);

        Map<ReplicationGroupId, DummyInternalTableImpl> tables = new HashMap<>();
        doAnswer(invocationOnMock -> {
            ReplicaRequest request = invocationOnMock.getArgument(1);
            ReplicaListener replicaListener = tables.get(request.groupId()).getReplicaListener();

            if (request instanceof TimestampAware) {
                TimestampAware aware = (TimestampAware) request;
                HybridTimestamp updated = DummyInternalTableImpl.CLOCK.update(aware.timestamp());

                return replicaListener.invoke(request, "local").handle((res, err) -> err == null ? replicaMessagesFactory
                        .timestampAwareReplicaResponse()
                        .result(res)
                        .timestampLong(updated.longValue())
                        .build() :
                        replicaMessagesFactory
                                .errorTimestampAwareReplicaResponse()
                                .throwable(err)
                                .timestampLong(updated.longValue())
                                .build());
            } else {
                return replicaListener.invoke(request, "local").handle((res, err) -> err == null ? replicaMessagesFactory
                        .replicaResponse()
                        .result(res)
                        .build() : replicaMessagesFactory
                        .errorReplicaResponse()
                        .throwable(err)
                        .build());
            }

        }).when(msgSvc).invoke(anyString(), any(), anyLong());

        txManager = new TxManagerImpl(replicaSvc, lockManager, localClock, new TransactionIdGenerator(0xdeadbeef), () -> "local");

        Function<String, ClusterNode> anyNodeResolver = a -> clusterService.topologyService().localMember();

        TransactionStateResolver transactionStateResolver = new TransactionStateResolver(replicaSvc, txManager, localClock,
                anyNodeResolver, anyNodeResolver, () -> "local", msgSvc);

        igniteTransactions = new IgniteTransactionsImpl(txManager, timestampTracker);

        DummyInternalTableImpl table = new DummyInternalTableImpl(
                replicaSvc,
                txManager,
                true,
                transactionStateResolver,
                ACCOUNTS_SCHEMA,
                timestampTracker
        );

        accounts = new TableImpl(table, new DummySchemaManagerImpl(ACCOUNTS_SCHEMA), lockManager);

        DummyInternalTableImpl table2 = new DummyInternalTableImpl(
                replicaSvc,
                txManager,
                true,
                transactionStateResolver,
                CUSTOMERS_SCHEMA,
                timestampTracker
        );

        customers = new TableImpl(table2, new DummySchemaManagerImpl(CUSTOMERS_SCHEMA), lockManager);

        tables.put(table.groupId(), table);
        tables.put(table2.groupId(), table2);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15928")
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

    // TODO: https://issues.apache.org/jira/browse/IGNITE-20355
    @Override
    public void testReadOnlyGet() {
        // No-op
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-20355
    @Override
    public void testReadOnlyScan() throws Exception {
        // No-op
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-20355
    @Override
    public void testReadOnlyGetWriteIntentResolutionUpdate() {
        // No-op
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-20355
    @Override
    public void testReadOnlyGetWriteIntentResolutionRemove() {
        // No-op
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-20355
    @Override
    public void testReadOnlyGetAll() {
        // No-op
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-20355
    @Override
    public void testReadOnlyPendingWriteIntentSkippedCombined() {
        super.testReadOnlyPendingWriteIntentSkippedCombined();
    }
}
