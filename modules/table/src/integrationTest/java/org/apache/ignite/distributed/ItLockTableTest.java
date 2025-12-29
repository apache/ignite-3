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

import static org.apache.ignite.internal.replicator.ReplicatorConstants.DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.metrics.TestMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager.LockState;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.impl.VolatileTxStateMetaStorage;
import org.apache.ignite.internal.tx.test.TestLocalRwTxCounter;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test lock table.
 */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
public class ItLockTableTest extends IgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItLockTableTest.class);

    private static final int CACHE_SIZE = 10;

    private static final String TABLE_NAME = "test";

    private static SchemaDescriptor TABLE_SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("id".toUpperCase(), NativeTypes.INT32, false)},
            new Column[]{
                    new Column("name".toUpperCase(), NativeTypes.STRING, true),
                    new Column("salary".toUpperCase(), NativeTypes.DOUBLE, true)
            }
    );

    protected TableViewInternal testTable;

    protected final TestInfo testInfo;

    // TODO fsync can be turned on again after https://issues.apache.org/jira/browse/IGNITE-20195
    @InjectConfiguration("mock: { fsync: false }")
    protected static RaftConfiguration raftConfiguration;

    @InjectConfiguration()
    protected static TransactionConfiguration txConfiguration;

    @InjectConfiguration
    protected static ReplicationConfiguration replicationConfiguration;

    @InjectConfiguration("mock.properties: { lockMapSize: \"" + CACHE_SIZE + "\" }")
    private static SystemLocalConfiguration systemLocalConfiguration;

    @InjectConfiguration("mock.properties.txnLockRetryCount=\"0\"")
    private static SystemDistributedConfiguration systemDistributedConfiguration;

    @InjectExecutorService
    protected ScheduledExecutorService commonExecutor;

    private ItTxTestCluster txTestCluster;

    private HybridTimestampTracker timestampTracker = HybridTimestampTracker.atomicTracker(null);

    /**
     * The constructor.
     *
     * @param testInfo Test info.
     */
    public ItLockTableTest(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @BeforeEach
    public void before() throws Exception {
        txTestCluster = new ItTxTestCluster(
                testInfo,
                raftConfiguration,
                txConfiguration,
                systemLocalConfiguration,
                systemDistributedConfiguration,
                workDir,
                1,
                1,
                false,
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
                    TransactionInflights transactionInflights,
                    LowWatermark lowWatermark
            ) {
                VolatileTxStateMetaStorage txStateVolatileStorage = new VolatileTxStateMetaStorage();
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
                );
            }
        };
        txTestCluster.prepareCluster();

        testTable = txTestCluster.startTable(TABLE_NAME, TABLE_SCHEMA);

        log.info("Tables have been started");
    }

    @AfterEach
    public void after() throws Exception {
        txTestCluster.shutdownCluster();
    }

    /**
     * Test that a lock table behaves correctly in case of lock cache overflow.
     */
    @Test
    public void testTakeMoreLocksThanAfford() {
        RecordView<Tuple> view = testTable.recordView();

        int i = 0;
        final int count = 100;
        List<Transaction> txns = new ArrayList<>();
        while (i++ < count) {
            Transaction tx = txTestCluster.igniteTransactions().begin();
            view.insertAsync(tx, tuple(i, "x-" + i));
            txns.add(tx);
        }

        assertTrue(TestUtils.waitForCondition(() -> {
            int total = 0;
            HeapLockManager lockManager = (HeapLockManager) txTestCluster.txManagers.get(txTestCluster.localNodeName).lockManager();
            for (int j = 0; j < lockManager.getSlots().length; j++) {
                LockState slot = lockManager.getSlots()[j];
                total += slot.waitersCount();
            }

            return total >= CACHE_SIZE && lockManager.available() == 0;
        }, 10_000), "Some lockers are missing");

        int empty = 0;
        int coll = 0;

        HeapLockManager lm = (HeapLockManager) txTestCluster.txManagers.get(txTestCluster.localNodeName).lockManager();
        for (int j = 0; j < lm.getSlots().length; j++) {
            LockState slot = lm.getSlots()[j];
            int cnt = slot.waitersCount();
            if (cnt == 0) {
                empty++;
            } else {
                coll += cnt;
            }
        }

        LOG.info("LockTable [emptySlots={} collisions={}]", empty, coll);

        assertTrue(coll > 0);

        List<CompletableFuture<?>> finishFuts = new ArrayList<>();
        for (Transaction txn : txns) {
            finishFuts.add(txn.rollbackAsync());
        }

        for (CompletableFuture<?> finishFut : finishFuts) {
            finishFut.join();
        }

        assertTrue(TestUtils.waitForCondition(() -> {
            int total = 0;
            HeapLockManager lockManager = (HeapLockManager) txTestCluster.txManagers.get(txTestCluster.localNodeName).lockManager();
            for (int j = 0; j < lockManager.getSlots().length; j++) {
                LockState slot = lockManager.getSlots()[j];
                total += slot.waitersCount();
            }

            return total == 0 && lockManager.available() == CACHE_SIZE;
        }, 10_000), "Illegal lock manager state");
    }

    private static Tuple tuple(int id, String name) {
        return Tuple.create()
                .set("id", id)
                .set("name", name);
    }

    private static Tuple keyTuple(int id) {
        return Tuple.create()
                .set("id", id);
    }
}
