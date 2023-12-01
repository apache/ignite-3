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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_COMMON_ERR;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.index.IndexChooser;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.TransactionStateResolver;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.table.distributed.schema.ValidationSchemasSource;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.message.TxCleanupReplicaRequest;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.TransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Durable cleanup test with successful recovery after the failures.
 */
public class ItTxDistributedCleanupRecoveryTest extends ItTxDistributedTestSingleNode {

    private AtomicInteger defaultRetryCount;

    /**
     * The constructor.
     *
     * @param testInfo Test info.
     */
    public ItTxDistributedCleanupRecoveryTest(TestInfo testInfo) {
        super(testInfo);
    }

    private void setDefaultRetryCount(int count) {
        defaultRetryCount = new AtomicInteger(count);
    }

    @BeforeEach
    @Override
    public void before() throws Exception {
        // The value of 3 is less than the allowed number of cleanup retries.
        setDefaultRetryCount(3);

        txTestCluster = new ItTxTestCluster(
                testInfo,
                raftConfiguration,
                workDir,
                nodes(),
                replicas(),
                startClient(),
                timestampTracker
        ) {

            @Override
            protected PartitionReplicaListener newReplicaListener(
                    MvPartitionStorage mvDataStorage,
                    RaftGroupService raftClient,
                    TxManager txManager,
                    Executor scanRequestExecutor,
                    int partId,
                    int tableId,
                    Supplier<Map<Integer, IndexLocker>> indexesLockers,
                    Lazy<TableSchemaAwareIndexStorage> pkIndexStorage,
                    Supplier<Map<Integer, TableSchemaAwareIndexStorage>> secondaryIndexStorages,
                    HybridClock hybridClock,
                    PendingComparableValuesTracker<HybridTimestamp, Void> safeTime,
                    TxStateStorage txStateStorage,
                    TransactionStateResolver transactionStateResolver,
                    StorageUpdateHandler storageUpdateHandler,
                    ValidationSchemasSource validationSchemasSource,
                    ClusterNode localNode,
                    SchemaSyncService schemaSyncService,
                    CatalogService catalogService,
                    PlacementDriver placementDriver,
                    IndexChooser indexChooser
            ) {
                return new PartitionReplicaListener(
                        mvDataStorage,
                        raftClient,
                        txManager,
                        txManager.lockManager(),
                        Runnable::run,
                        partId,
                        tableId,
                        indexesLockers,
                        pkIndexStorage,
                        secondaryIndexStorages,
                        hybridClock,
                        safeTime,
                        txStateStorage,
                        transactionStateResolver,
                        storageUpdateHandler,
                        validationSchemasSource,
                        localNode,
                        schemaSyncService,
                        catalogService,
                        placementDriver,
                        indexChooser
                ) {
                    @Override
                    public CompletableFuture<ReplicaResult> invoke(ReplicaRequest request, String senderId) {
                        if (request instanceof TxCleanupReplicaRequest && defaultRetryCount.getAndDecrement() > 0) {
                            logger().info("Dropping cleanup request: {}", request);

                            return failedFuture(new ReplicationException(
                                    REPLICA_COMMON_ERR,
                                    "Test Tx Cleanup exception [replicaGroupId=" + request.groupId() + ']'));
                        }
                        return super.invoke(request, senderId);
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
    @Override
    public void testDeleteUpsertCommit() throws TransactionException {
        // The value of 6 is higher than the default retry count.
        // So we should give up retrying and crash.
        setDefaultRetryCount(6);

        assertThrows(TransactionException.class, () -> deleteUpsert().commit());
    }

    @Test
    @Override
    public void testTransactionAlreadyRolledback() {
        // The value of 6 is higher than the default retry count.
        // So we should give up retrying and crash.
        setDefaultRetryCount(6);

        // Do not check the locks since we have intentionally dropped the cleanup request thus the locks are not released yet.
        testTransactionAlreadyFinished(false, false, (transaction, txId) -> {
            assertThrows(TransactionException.class, transaction::rollback);

            log.info("Rolled back transaction {}", txId);
        });
    }

    @Test
    @Override
    public void testTransactionAlreadyCommitted() {
        // The value of 6 is higher than the default retry count.
        // So we should give up retrying and crash.
        setDefaultRetryCount(6);

        // Do not check the locks since we have intentionally dropped the cleanup request thus the locks are not released yet.
        testTransactionAlreadyFinished(true, false, (transaction, txId) -> {
            assertThrows(TransactionException.class, transaction::commit);

            log.info("Committed transaction {}", txId);
        });
    }
}
