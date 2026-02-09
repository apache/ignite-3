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

package org.apache.ignite.distributed;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.replicator.ReplicatorConstants.DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.metrics.TestMetricManager;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.partition.replicator.FuturesCleanupResult;
import org.apache.ignite.internal.partition.replicator.ReplicaPrimacy;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.TxAbstractTest;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.metrics.TableMetricSource;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.impl.TransactionStateResolver;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.impl.VolatileTxStateMetaStorage;
import org.apache.ignite.internal.tx.message.TableWriteIntentSwitchReplicaRequest;
import org.apache.ignite.internal.tx.test.TestLocalRwTxCounter;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.TransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Test to Simulate missing cleanup action.
 */
public class ItTxDistributedTestSingleNodeNoCleanupMessage extends TxAbstractTest {
    /** A list of background cleanup futures. */
    private final List<CompletableFuture<?>> cleanupFutures = new CopyOnWriteArrayList<>();

    @InjectConfiguration
    private TransactionConfiguration txConfiguration;

    @InjectConfiguration
    private SystemLocalConfiguration systemLocalConfiguration;

    @InjectConfiguration("mock.properties.txnLockRetryCount=\"0\"")
    private SystemDistributedConfiguration systemDistributedConfiguration;

    /**
     * The constructor.
     *
     * @param testInfo Test info.
     */
    public ItTxDistributedTestSingleNodeNoCleanupMessage(TestInfo testInfo) {
        super(testInfo);
    }

    @BeforeEach
    @Override
    public void before() throws Exception {
        txTestCluster = new ItTxTestCluster(
                testInfo,
                raftConfiguration,
                txConfiguration,
                systemLocalConfiguration,
                systemDistributedConfiguration,
                workDir,
                nodes(),
                replicas(),
                startClient(),
                timestampTracker,
                replicationConfiguration
        ) {
            @Override
            protected TxManagerImpl newTxManager(
                    ClusterService clusterService,
                    ReplicaService replicaSvc,
                    ClockService clockService,
                    TransactionIdGenerator generator,
                    InternalClusterNode node,
                    PlacementDriver placementDriver,
                    RemotelyTriggeredResourceRegistry resourcesRegistry,
                    VolatileTxStateMetaStorage txStateVolatileStorage,
                    TransactionInflights transactionInflights,
                    LowWatermark lowWatermark
            ) {
                return new TxManagerImpl(
                        txConfiguration,
                        systemDistributedConfiguration,
                        clusterService,
                        replicaSvc,
                        new HeapLockManager(systemLocalConfiguration, txStateVolatileStorage),
                        txStateVolatileStorage,
                        clockService,
                        generator,
                        placementDriver,
                        () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS,
                        new TestLocalRwTxCounter(),
                        resourcesRegistry,
                        transactionInflights,
                        lowWatermark,
                        commonExecutor,
                        new TestMetricManager()
                ) {
                    @Override
                    public CompletableFuture<Void> executeWriteIntentSwitchAsync(Runnable runnable) {
                        CompletableFuture<Void> cleanupFuture = super.executeWriteIntentSwitchAsync(runnable);

                        cleanupFutures.add(cleanupFuture);

                        return cleanupFuture;
                    }
                };
            }

            @Override
            protected PartitionReplicaListener newReplicaListener(
                    MvPartitionStorage mvDataStorage,
                    RaftGroupService raftClient,
                    TxManager txManager,
                    Executor scanRequestExecutor,
                    ZonePartitionId replicationGroupId,
                    int tableId,
                    Supplier<Map<Integer, IndexLocker>> indexesLockers,
                    Lazy<TableSchemaAwareIndexStorage> pkIndexStorage,
                    Supplier<Map<Integer, TableSchemaAwareIndexStorage>> secondaryIndexStorages,
                    ClockService clockService,
                    PendingComparableValuesTracker<HybridTimestamp, Void> safeTime,
                    TransactionStateResolver transactionStateResolver,
                    StorageUpdateHandler storageUpdateHandler,
                    ValidationSchemasSource validationSchemasSource,
                    InternalClusterNode localNode,
                    SchemaSyncService schemaSyncService,
                    CatalogService catalogService,
                    PlacementDriver placementDriver,
                    ClusterNodeResolver clusterNodeResolver,
                    RemotelyTriggeredResourceRegistry resourcesRegistry,
                    SchemaRegistry schemaRegistry
            ) {
                return new PartitionReplicaListener(
                        mvDataStorage,
                        raftClient,
                        txManager,
                        txManager.lockManager(),
                        Runnable::run,
                        replicationGroupId,
                        tableId,
                        indexesLockers,
                        pkIndexStorage,
                        secondaryIndexStorages,
                        clockService,
                        safeTime,
                        transactionStateResolver,
                        storageUpdateHandler,
                        validationSchemasSource,
                        localNode,
                        schemaSyncService,
                        catalogService,
                        placementDriver,
                        clusterNodeResolver,
                        resourcesRegistry,
                        schemaRegistry,
                        mock(IndexMetaStorage.class),
                        lowWatermark,
                        mock(FailureProcessor.class),
                        new TableMetricSource(QualifiedName.fromSimple("test_table"))
                ) {
                    @Override
                    public CompletableFuture<ReplicaResult> process(ReplicaRequest request, ReplicaPrimacy replicaPrimacy, UUID senderId) {
                        if (request instanceof TableWriteIntentSwitchReplicaRequest) {
                            logger().info("Dropping cleanup request: {}", request);

                            releaseTxLocks(
                                    ((TableWriteIntentSwitchReplicaRequest) request).txId(),
                                    txManager.lockManager()
                            );

                            FuturesCleanupResult cleanupResult = new FuturesCleanupResult(false, false);
                            return completedFuture(new ReplicaResult(cleanupResult, null));
                        }

                        return super.process(request, replicaPrimacy, senderId);
                    }
                };
            }
        };

        txTestCluster.prepareCluster();

        this.igniteTransactions = txTestCluster.igniteTransactions;

        accounts = txTestCluster.startTable(ACC_TABLE_NAME, ACCOUNTS_SCHEMA);
        customers = txTestCluster.startTable(CUST_TABLE_NAME, CUSTOMERS_SCHEMA);

        log.info("Tables have been started");
    }

    @Test
    public void testTwoReadWriteTransactions() throws TransactionException {
        Tuple key = makeKey(1);

        assertFalse(accounts.recordView().delete(null, key));
        assertNull(accounts.recordView().get(null, key));

        InternalTransaction tx1 = (InternalTransaction) igniteTransactions.begin();
        accounts.recordView().upsert(tx1, makeValue(1, 100.));
        tx1.commit();

        InternalTransaction tx2 = (InternalTransaction) igniteTransactions.begin();
        accounts.recordView().upsert(tx2, makeValue(1, 200.));
        tx2.commit();

        assertEquals(200., accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));
    }

    @Test
    public void testTwoReadWriteTransactionsWaitForCleanup() throws TransactionException {
        Tuple key = makeKey(1);

        assertFalse(accounts.recordView().delete(null, key));
        assertNull(accounts.recordView().get(null, key));

        // Start the first transaction. The values it changes will not be cleaned up.
        InternalTransaction tx1 = (InternalTransaction) igniteTransactions.begin();

        accounts.recordView().upsert(tx1, makeValue(1, 100.));

        tx1.commit();

        // Now start the seconds transaction and make sure write intent resolution is called  by adding a `get` operation.
        InternalTransaction tx2 = (InternalTransaction) igniteTransactions.begin();

        assertEquals(100., accounts.recordView().get(tx2, makeKey(1)).doubleValue("balance"));

        // Now wait for the background task to finish.
        cleanupFutures.forEach(completableFuture -> assertThat(completableFuture, willCompleteSuccessfully()));

        accounts.recordView().upsert(tx2, makeValue(1, 200.));

        tx2.commit();

        assertEquals(200., accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));
    }

    private static void releaseTxLocks(UUID txId, LockManager lockManager) {
        lockManager.releaseAll(txId);
    }

    @Override
    protected int nodes() {
        return 1;
    }
}
