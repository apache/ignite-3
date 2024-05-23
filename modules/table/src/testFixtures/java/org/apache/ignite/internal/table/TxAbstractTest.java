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

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCode;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.ignite.distributed.ItTxTestCluster;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.configuration.StorageUpdateConfiguration;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxPriority;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * TODO asch IGNITE-15928 validate zero locks after test commit.
 */
@ExtendWith({MockitoExtension.class, ConfigurationExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public abstract class TxAbstractTest extends IgniteAbstractTest {
    protected static final double BALANCE_1 = 500;

    protected static final double BALANCE_2 = 500;

    protected static final double DELTA = 100;

    protected static final String ACC_TABLE_NAME = "accounts";

    protected static final String CUST_TABLE_NAME = "customers";

    protected static SchemaDescriptor ACCOUNTS_SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("accountNumber".toUpperCase(), NativeTypes.INT64, false)},
            new Column[]{new Column("balance".toUpperCase(), NativeTypes.DOUBLE, false)}
    );

    protected static SchemaDescriptor CUSTOMERS_SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("accountNumber".toUpperCase(), NativeTypes.INT64, false)},
            new Column[]{new Column("name".toUpperCase(), NativeTypes.STRING, false)}
    );

    /** Accounts table id -> balance. */
    protected TableViewInternal accounts;

    /** Customers table id -> name. */
    protected TableViewInternal customers;

    protected HybridTimestampTracker timestampTracker = new HybridTimestampTracker();

    protected IgniteTransactions igniteTransactions;

    // TODO fsync can be turned on again after https://issues.apache.org/jira/browse/IGNITE-20195
    @InjectConfiguration("mock: { fsync: false }")
    protected RaftConfiguration raftConfiguration;

    @InjectConfiguration
    protected TransactionConfiguration txConfiguration;

    @InjectConfiguration
    protected StorageUpdateConfiguration storageUpdateConfiguration;

    @InjectConfiguration
    protected ReplicationConfiguration replicationConfiguration;

    protected final TestInfo testInfo;

    protected ItTxTestCluster txTestCluster;

    /**
     * Returns a count of nodes.
     *
     * @return Nodes.
     */
    protected abstract int nodes();

    /**
     * Returns a count of replicas.
     *
     * @return Replicas.
     */
    protected int replicas() {
        return 1;
    }

    /**
     * Returns {@code true} to disable collocation by using dedicated client node.
     *
     * @return {@code true} to disable collocation.
     */
    protected boolean startClient() {
        return true;
    }

    /**
     * The constructor.
     *
     * @param testInfo Test info.
     */
    public TxAbstractTest(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    /**
     * Initialize the test state.
     */
    @BeforeEach
    public void before() throws Exception {
        txTestCluster = new ItTxTestCluster(
                testInfo,
                raftConfiguration,
                txConfiguration,
                storageUpdateConfiguration,
                workDir,
                nodes(),
                replicas(),
                startClient(),
                timestampTracker,
                replicationConfiguration
        );
        txTestCluster.prepareCluster();

        this.igniteTransactions = txTestCluster.igniteTransactions();

        accounts = txTestCluster.startTable(ACC_TABLE_NAME, ACCOUNTS_SCHEMA);
        customers = txTestCluster.startTable(CUST_TABLE_NAME, CUSTOMERS_SCHEMA);

        log.info("Tables have been started");
    }

    /**
     * Shutdowns all cluster nodes after each test.
     *
     * @throws Exception If failed.
     */
    @AfterEach
    public void after() throws Exception {
        txTestCluster.shutdownCluster();
        Mockito.framework().clearInlineMocks();
    }

    /**
     * Starts a node.
     *
     * @param name Node name.
     * @param port Local port.
     * @param nodeFinder Node finder.
     * @return The client cluster view.
     */
    public static ClusterService startNode(TestInfo testInfo, String name, int port,
            NodeFinder nodeFinder) {
        var network = ClusterServiceTestUtils.clusterService(testInfo, port, nodeFinder);

        assertThat(network.startAsync(), willCompleteSuccessfully());

        return network;
    }

    /** {@inheritDoc} */
    protected TxManager clientTxManager() {
        return txTestCluster.clientTxManager();
    }

    /** {@inheritDoc} */
    protected TxManager txManager(TableViewInternal t) {
        CompletableFuture<ReplicaMeta> primaryReplicaFuture = txTestCluster.placementDriver().getPrimaryReplica(
                new TablePartitionId(t.tableId(), 0),
                txTestCluster.clocks().get(txTestCluster.localNodeName()).now());

        assertThat(primaryReplicaFuture, willCompleteSuccessfully());

        TxManager manager = txTestCluster.txManagers().get(primaryReplicaFuture.join().getLeaseholder());

        assertNotNull(manager);

        return manager;
    }

    /**
     * Check the storage of partition is the same across all nodes. The checking is based on {@link MvPartitionStorage#lastAppliedIndex()}
     * that is increased on all update storage operation.
     * TODO: IGNITE-18869 The method must be updated when a proper way to compare storages will be implemented.
     *
     * @param table The table.
     * @param partId Partition id.
     * @return True if {@link MvPartitionStorage#lastAppliedIndex()} is equivalent across all nodes, false otherwise.
     */
    protected boolean assertPartitionsSame(TableViewInternal table, int partId) {
        long storageIdx = 0;

        for (Map.Entry<String, Loza> entry : txTestCluster.raftServers().entrySet()) {
            Loza svc = entry.getValue();

            var server = (JraftServerImpl) svc.server();

            var groupId = new TablePartitionId(table.tableId(), partId);

            Peer serverPeer = server.localPeers(groupId).get(0);

            RaftGroupService grp = server.raftGroupService(new RaftNodeId(groupId, serverPeer));

            var fsm = (JraftServerImpl.DelegatingStateMachine) grp.getRaftNode().getOptions().getFsm();

            PartitionListener listener = (PartitionListener) fsm.getListener();

            MvPartitionStorage storage = listener.getMvStorage();

            if (storageIdx == 0) {
                storageIdx = storage.lastAppliedIndex();
            } else if (storageIdx != storage.lastAppliedIndex()) {
                return false;
            }
        }

        return true;
    }

    protected void injectFailureOnNextOperation(TableViewInternal accounts) {
        InternalTable internalTable = accounts.internalTable();
        ReplicaService replicaService = IgniteTestUtils.getFieldValue(internalTable, "replicaSvc");
        Mockito.doReturn(CompletableFuture.failedFuture(new Exception())).when(replicaService).invoke((String) any(), any());
        Mockito.doReturn(CompletableFuture.failedFuture(new Exception())).when(replicaService).invoke((ClusterNode) any(), any());
    }

    protected Collection<TxManager> txManagers() {
        return txTestCluster.txManagers().values();
    }

    @Test
    public void testCommitRollbackSameTxDoesNotThrow() throws TransactionException {
        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();

        accounts.recordView().upsert(tx, makeValue(1, 100.));

        tx.commit();

        assertDoesNotThrow(tx::rollback, "Unexpected exception was thrown.");
        assertDoesNotThrow(tx::commit, "Unexpected exception was thrown.");
        assertDoesNotThrow(tx::rollback, "Unexpected exception was thrown.");
    }

    @Test
    public void testRollbackCommitSameTxDoesNotThrow() throws TransactionException {
        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();

        accounts.recordView().upsert(tx, makeValue(1, 100.));

        tx.rollback();

        assertDoesNotThrow(tx::commit, "Unexpected exception was thrown.");
        assertDoesNotThrow(tx::rollback, "Unexpected exception was thrown.");
        assertDoesNotThrow(tx::commit, "Unexpected exception was thrown.");
    }

    @Test
    public void testRepeatedCommitRollbackAfterUpdateWithException() throws Exception {
        injectFailureOnNextOperation(accounts);

        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();

        CompletableFuture<Void> fut = accounts.recordView().upsertAsync(tx, makeValue(1, 100.));

        assertThrows(Exception.class, fut::join);

        tx.commitAsync().join();

        tx.rollbackAsync().join();

        tx.commitAsync().join();
    }

    @Test
    public void testRepeatedCommitRollbackAfterRollbackWithException() throws Exception {
        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();

        accounts.recordView().upsert(tx, makeValue(1, 100.));

        injectFailureOnNextOperation(accounts);

        CompletableFuture<Void> fut = tx.rollbackAsync();
        assertThrows(Exception.class, fut::join);

        tx.commitAsync().join();

        tx.rollbackAsync().join();

        tx.commitAsync().join();
    }

    @Test
    public void testDeleteUpsertCommit() throws TransactionException {
        deleteUpsert().commit();

        assertEquals(200., accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));
    }

    @Test
    public void testDeleteUpsertRollback() throws TransactionException {
        deleteUpsert().rollback();

        assertEquals(100., accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));
    }

    protected InternalTransaction deleteUpsert() {
        accounts.recordView().upsert(null, makeValue(1, 100.));

        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();

        accounts.recordView().delete(tx, makeKey(1));

        assertNull(accounts.recordView().get(tx, makeKey(1)));

        accounts.recordView().upsert(tx, makeValue(1, 200.));

        return tx;
    }

    @Test
    public void testDeleteUpsertAllCommit() throws TransactionException {
        deleteUpsertAll().commit();

        assertEquals(200., accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));
        assertEquals(200., accounts.recordView().get(null, makeKey(2)).doubleValue("balance"));
    }

    /**
     * Repeat to find errrs.
     *
     * @throws TransactionException Transaction Exception.
     */
    @RepeatedTest(100)
    public void testDeleteUpsertAllRollback() throws TransactionException {
        deleteUpsertAll().rollback();

        var res1 = accounts.recordView().get(null, makeKey(1));
        assertEquals(100., res1.doubleValue("balance"), "tuple =[" + res1 + "]");

        var res2 = accounts.recordView().get(null, makeKey(2));
        assertEquals(100., res2.doubleValue("balance"), "tuple =[" + res2 + "]");
    }

    private InternalTransaction deleteUpsertAll() {
        List<Tuple> tuples = new ArrayList<>();
        tuples.add(makeValue(1, 100.));
        tuples.add(makeValue(2, 100.));

        accounts.recordView().upsertAll(null, tuples);

        var res1 = accounts.recordView().get(null, makeKey(1));
        assertEquals(100., res1.doubleValue("balance"), "tuple =[" + res1 + "]");

        var res2 = accounts.recordView().get(null, makeKey(2));
        assertEquals(100., res2.doubleValue("balance"), "tuple =[" + res2 + "]");

        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();

        tuples.clear();
        tuples.add(makeKey(1));
        tuples.add(makeKey(2));

        accounts.recordView().deleteAll(tx, tuples);

        tuples.clear();
        tuples.add(makeValue(1, 200.));
        tuples.add(makeValue(2, 200.));

        accounts.recordView().upsertAll(tx, tuples);

        return tx;
    }

    @Test
    public void testMixedPutGet() throws TransactionException {
        accounts.recordView().upsert(null, makeValue(1, BALANCE_1));

        igniteTransactions.runInTransaction(
                tx -> {
                    var txAcc = accounts.recordView();

                    txAcc.getAsync(tx, makeKey(1)).thenCompose(r ->
                            txAcc.upsertAsync(tx, makeValue(1, r.doubleValue("balance") + DELTA))).join();
                }
        );

        assertEquals(BALANCE_1 + DELTA, accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));
    }

    @Test
    public void testLockOrdering() throws InterruptedException {
        accounts.recordView().upsert(null, makeValue(1, 50.));

        InternalTransaction tx1 = (InternalTransaction) igniteTransactions.begin();
        InternalTransaction tx2 = (InternalTransaction) igniteTransactions.begin();
        InternalTransaction tx3 = (InternalTransaction) igniteTransactions.begin();
        InternalTransaction tx4 = (InternalTransaction) igniteTransactions.begin();

        assertTrue(tx3.id().compareTo(tx4.id()) < 0);
        assertTrue(tx2.id().compareTo(tx3.id()) < 0);
        assertTrue(tx1.id().compareTo(tx2.id()) < 0);

        RecordView<Tuple> acc0 = accounts.recordView();
        RecordView<Tuple> acc2 = accounts.recordView();
        RecordView<Tuple> acc3 = accounts.recordView();
        RecordView<Tuple> acc4 = accounts.recordView();

        acc0.upsert(tx4, makeValue(1, 100.));

        CompletableFuture<Void> fut = acc3.upsertAsync(tx2, makeValue(1, 300.));

        Thread.sleep(100);

        assertFalse(fut.isDone());

        CompletableFuture<Void> fut2 = acc4.upsertAsync(tx2, makeValue(1, 400.));

        Thread.sleep(100);

        assertFalse(fut2.isDone());

        CompletableFuture<Void> fut3 = acc2.upsertAsync(tx3, makeValue(1, 200.));

        assertFalse(fut3.isDone());
    }

    /**
     * Tests a transaction closure.
     */
    @Test
    public void testTxClosure() throws TransactionException {
        RecordView<Tuple> view = accounts.recordView();

        view.upsert(null, makeValue(1, BALANCE_1));
        view.upsert(null, makeValue(2, BALANCE_2));

        igniteTransactions.runInTransaction(tx -> {
            CompletableFuture<Tuple> read1 = view.getAsync(tx, makeKey(1));
            CompletableFuture<Tuple> read2 = view.getAsync(tx, makeKey(2));

            // TODO asch IGNITE-15938 must ensure a commit happens after all pending tx async ops.
            view.upsertAsync(tx, makeValue(1, read1.join().doubleValue("balance") - DELTA)).join();
            view.upsertAsync(tx, makeValue(2, read2.join().doubleValue("balance") + DELTA)).join();
        });

        assertEquals(BALANCE_1 - DELTA, view.get(null, makeKey(1)).doubleValue("balance"));
        assertEquals(BALANCE_2 + DELTA, view.get(null, makeKey(2)).doubleValue("balance"));

        assertEquals(5, clientTxManager().finished());
        assertEquals(0, clientTxManager().pending());
    }

    /**
     * Tests a transaction closure over key-value view.
     */
    @Test
    public void testTxClosureKeyValueView() throws TransactionException {
        accounts.recordView().upsert(null, makeValue(1, BALANCE_1));
        accounts.recordView().upsert(null, makeValue(2, BALANCE_2));

        igniteTransactions.runInTransaction(tx -> {
            KeyValueView<Tuple, Tuple> view = accounts.keyValueView();

            CompletableFuture<Tuple> read1 = view.getAsync(tx, makeKey(1));
            CompletableFuture<Tuple> read2 = view.getAsync(tx, makeKey(2));

            view.putAsync(tx, makeKey(1), makeValue(read1.join().doubleValue("balance") - DELTA)).join();
            view.putAsync(tx, makeKey(2), makeValue(read2.join().doubleValue("balance") + DELTA)).join();
        });

        assertEquals(BALANCE_1 - DELTA, accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));
        assertEquals(BALANCE_2 + DELTA, accounts.recordView().get(null, makeKey(2)).doubleValue("balance"));

        assertEquals(5, clientTxManager().finished());
        assertEquals(0, clientTxManager().pending());
    }

    /**
     * Tests positive transfer scenario.
     */
    @Test
    public void testTxClosureAsync() {
        double balance1 = 200.;
        double balance2 = 300.;
        double delta = 50.;
        Tuple ret = transferAsync(balance1, balance2, delta).join();

        RecordView<Tuple> view = accounts.recordView();

        assertEquals(balance1 - delta, view.get(null, makeKey(1)).doubleValue("balance"));
        assertEquals(balance2 + delta, view.get(null, makeKey(2)).doubleValue("balance"));
        assertEquals(balance1, ret.doubleValue("balance1"));
        assertEquals(balance2, ret.doubleValue("balance2"));
    }

    /**
     * Tests negative transfer scenario.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-17861")
    public void testTxClosureAbortAsync() {
        double balance1 = 10.;
        double balance2 = 300.;
        double delta = 50.;
        assertThrows(CompletionException.class, () -> transferAsync(balance1, balance2, delta).join());

        RecordView<Tuple> view = accounts.recordView();

        assertEquals(balance1, view.get(null, makeKey(1)).doubleValue("balance"));
        assertEquals(balance2, view.get(null, makeKey(2)).doubleValue("balance"));
    }

    /**
     * Tests uncaught exception in the closure.
     */
    @Test
    public void testTxClosureUncaughtExceptionAsync() {
        double balance = 10.;
        double delta = 50.;

        RecordView<Tuple> view = accounts.recordView();
        view.upsert(null, makeValue(1, balance));

        CompletableFuture<Double> fut0 = igniteTransactions.runInTransactionAsync(tx -> {
            CompletableFuture<Double> fut = view.getAsync(tx, makeKey(1))
                    .thenCompose(val2 -> {
                        double prev = val2.doubleValue("balance");
                        return view.upsertAsync(tx, makeValue(1, delta + 20)).thenApply(ignored -> prev);
                    });

            fut.join();

            if (true) {
                throw new IllegalArgumentException();
            }

            return fut;
        });

        var err = assertThrows(CompletionException.class, fut0::join);

        try {
            assertInstanceOf(IllegalArgumentException.class, err.getCause());
        } catch (AssertionError e) {
            throw new AssertionError("Unexpected exception type", err);
        }

        assertEquals(balance, view.get(null, makeKey(1)).doubleValue("balance"));
    }

    /**
     * Tests uncaught exception in the chain.
     */
    @Test
    public void testTxClosureUncaughtExceptionInChainAsync() {
        RecordView<Tuple> view = accounts.recordView();

        CompletableFuture<Double> fut0 = igniteTransactions.runInTransactionAsync(tx -> {
            return view.getAsync(tx, makeKey(2))
                    .thenCompose(val2 -> {
                        double prev = val2.doubleValue("balance"); // val2 is null - NPE is thrown here
                        return view.upsertAsync(tx, makeValue(1, 100)).thenApply(ignored -> prev);
                    });
        });

        var err = assertThrows(CompletionException.class, fut0::join);

        try {
            assertInstanceOf(NullPointerException.class, err.getCause());
        } catch (AssertionError e) {
            throw new AssertionError("Unexpected exception type", err);
        }
    }

    @Test
    public void testBatchPutConcurrently() {
        Transaction tx1 = igniteTransactions.begin();
        Transaction tx2 = igniteTransactions.begin();

        log.info("Tx " + tx2);
        log.info("Tx2 " + tx1);

        ArrayList<Tuple> rows = new ArrayList<>();
        ArrayList<Tuple> rows2 = new ArrayList<>();

        for (int i = 0; i < 1; i++) {
            rows.add(makeValue(i, i * 100.));
            rows2.add(makeValue(i, 2 * i * 100.));
        }

        var table = accounts.recordView();
        var table2 = accounts.recordView();

        table2.upsertAll(tx1, rows2);

        Exception err = assertThrows(Exception.class, () -> table.upsertAll(tx2, rows));

        assertTrue(err.getMessage().contains("Failed to acquire a lock"), err.getMessage());

        tx1.commit();
    }

    @Test
    public void testBatchReadPutConcurrently() throws InterruptedException {
        InternalTransaction tx1 = (InternalTransaction) igniteTransactions.begin();
        InternalTransaction tx2 = (InternalTransaction) igniteTransactions.begin();

        log.info("Tx1 " + tx1);
        log.info("Tx2 " + tx2);

        var table = accounts.recordView();
        var table2 = accounts.recordView();

        ArrayList<Tuple> keys = new ArrayList<>();
        ArrayList<Tuple> keys2 = new ArrayList<>();

        for (int i = 0; i < 1; i++) {
            keys.add(makeKey(i));
            keys2.add(makeKey(i));
        }

        table2.getAll(tx1, keys);
        table2.getAll(tx2, keys2);

        ArrayList<Tuple> rows = new ArrayList<>();
        ArrayList<Tuple> rows2 = new ArrayList<>();

        for (int i = 0; i < 1; i++) {
            rows.add(makeValue(i, i * 100.));
            rows2.add(makeValue(i, 2 * i * 100.));
        }

        var futUpd2 = table2.upsertAllAsync(tx1, rows2);

        assertTrue(IgniteTestUtils.waitForCondition(() -> {
            boolean lockUpgraded = false;

            for (Iterator<Lock> it = txManager(accounts).lockManager().locks(tx1.id()); it.hasNext(); ) {
                Lock lock = it.next();

                lockUpgraded = txManager(accounts).lockManager().waiter(lock.lockKey(), tx1.id()).intendedLockMode() == LockMode.X;

                if (lockUpgraded) {
                    break;
                }
            }

            return lockUpgraded;
        }, 3000));

        assertFalse(futUpd2.isDone());

        assertThrowsWithCause(() -> table.upsertAll(tx2, rows), LockException.class);
    }

    /**
     * Tests an asynchronous transaction.
     */
    @Test
    public void testTxAsync() {
        accounts.recordView().upsert(null, makeValue(1, BALANCE_1));
        accounts.recordView().upsert(null, makeValue(2, BALANCE_2));

        igniteTransactions.beginAsync()
                .thenCompose(tx -> accounts.recordView().getAsync(tx, makeKey(1))
                        .thenCombine(accounts.recordView().getAsync(tx, makeKey(2)), (v1, v2) -> new Pair<>(v1, v2))
                        .thenCompose(pair -> allOf(
                                        accounts.recordView().upsertAsync(
                                                tx, makeValue(1, pair.getFirst().doubleValue("balance") - DELTA)),
                                        accounts.recordView().upsertAsync(
                                                tx, makeValue(2, pair.getSecond().doubleValue("balance") + DELTA))
                                )
                                        .thenApply(ignored -> tx)
                        )
                ).thenCompose(Transaction::commitAsync).join();

        assertEquals(BALANCE_1 - DELTA, accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));
        assertEquals(BALANCE_2 + DELTA, accounts.recordView().get(null, makeKey(2)).doubleValue("balance"));
    }

    /**
     * Tests an asynchronous transaction over key-value view.
     */
    @Test
    public void testTxAsyncKeyValueView() {
        accounts.recordView().upsert(null, makeValue(1, BALANCE_1));
        accounts.recordView().upsert(null, makeValue(2, BALANCE_2));

        igniteTransactions.beginAsync()
                .thenCompose(tx -> accounts.keyValueView().getAsync(tx, makeKey(1))
                        .thenCombine(accounts.recordView().getAsync(tx, makeKey(2)), (v1, v2) -> new Pair<>(v1, v2))
                        .thenCompose(pair -> allOf(
                                        accounts.keyValueView().putAsync(
                                                tx, makeKey(1), makeValue(pair.getFirst().doubleValue("balance") - DELTA)),
                                        accounts.keyValueView().putAsync(
                                                tx, makeKey(2), makeValue(pair.getSecond().doubleValue("balance") + DELTA))
                                )
                                        .thenApply(ignored -> tx)
                        )
                ).thenCompose(Transaction::commitAsync).join();

        assertEquals(BALANCE_1 - DELTA, accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));
        assertEquals(BALANCE_2 + DELTA, accounts.recordView().get(null, makeKey(2)).doubleValue("balance"));
    }

    @Test
    public void testSimpleConflict() throws Exception {
        accounts.recordView().upsert(null, makeValue(1, 100.));

        Transaction tx1 = igniteTransactions.begin();
        Transaction tx2 = igniteTransactions.begin();

        var table = accounts.recordView();
        var table2 = accounts.recordView();

        double val = table.get(tx2, makeKey(1)).doubleValue("balance");
        table2.get(tx1, makeKey(1)).doubleValue("balance");

        try {
            table.upsert(tx2, makeValue(1, val + 1));

            fail();
        } catch (Exception e) {
            // Expected.
        }

        table2.upsert(tx1, makeValue(1, val + 1));

        tx1.commit();

        tx2.commit();

        assertEquals(101., accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));
    }

    @Test
    public void testCommit() throws TransactionException {
        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();

        Tuple key = makeKey(1);

        var table = accounts.recordView();

        table.upsert(tx, makeValue(1, 100.));

        assertEquals(100., table.get(tx, key).doubleValue("balance"));

        table.upsert(tx, makeValue(1, 200.));

        assertEquals(200., table.get(tx, key).doubleValue("balance"));

        tx.commit();

        assertEquals(200., accounts.recordView().get(null, key).doubleValue("balance"));
    }

    @Test
    public void testAbort() throws TransactionException {
        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();

        Tuple key = makeKey(1);

        var table = accounts.recordView();

        table.upsert(tx, makeValue(1, 100.));

        assertEquals(100., table.get(tx, key).doubleValue("balance"));

        table.upsert(tx, makeValue(1, 200.));

        assertEquals(200., table.get(tx, key).doubleValue("balance"));

        tx.rollback();

        assertNull(accounts.recordView().get(null, key));
    }

    @Test
    public void testAbortNoUpdate() throws TransactionException {
        accounts.recordView().upsert(null, makeValue(1, 100.));

        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();

        tx.rollback();

        assertEquals(100., accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));
    }

    @Test
    public void testConcurrent() throws TransactionException {
        Transaction tx = igniteTransactions.begin();
        Transaction tx2 = igniteTransactions.begin();

        Tuple key = makeKey(1);
        Tuple val = makeValue(1, 100.);

        accounts.recordView().upsert(null, val);

        var table = accounts.recordView();
        var table2 = accounts.recordView();

        assertEquals(100., table.get(tx, key).doubleValue("balance"));
        assertEquals(100., table2.get(tx2, key).doubleValue("balance"));

        tx.commit();
        tx2.commit();
    }

    /**
     * Tests if a lost update is not happening on concurrent increment.
     */
    @Test
    public void testIncrement() throws TransactionException {
        Transaction tx1 = igniteTransactions.begin();
        Transaction tx2 = igniteTransactions.begin();

        Tuple key = makeKey(1);
        Tuple val = makeValue(1, 100.);

        accounts.recordView().upsert(null, val); // Creates implicit transaction.

        var table = accounts.recordView();
        var table2 = accounts.recordView();

        // Read in tx2
        double valTx = table.get(tx2, key).doubleValue("balance");

        // Read in tx1
        double valTx2 = table2.get(tx1, key).doubleValue("balance");

        // Write in tx2 (out of order)
        // TODO asch IGNITE-15937 fix exception model.
        Exception err = assertThrows(Exception.class, () -> table.upsert(tx2, makeValue(1, valTx + 1)));

        assertTrue(err.getMessage().contains("Failed to acquire a lock"), err.getMessage());

        // Write in tx1
        table2.upsert(tx1, makeValue(1, valTx2 + 1));

        tx1.commit();

        assertEquals(101., accounts.recordView().get(null, key).doubleValue("balance"));
    }

    @Test
    public void testAbortWithValue() throws TransactionException {
        accounts.recordView().upsert(null, makeValue(0, 100.));

        assertEquals(100., accounts.recordView().get(null, makeKey(0)).doubleValue("balance"));

        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();
        var table = accounts.recordView();
        table.upsert(tx, makeValue(0, 200.));
        assertEquals(200., table.get(tx, makeKey(0)).doubleValue("balance"));
        tx.rollback();

        assertEquals(100., accounts.recordView().get(null, makeKey(0)).doubleValue("balance"));
    }

    @Test
    public void testInsert() throws TransactionException {
        assertNull(accounts.recordView().get(null, makeKey(1)));

        Transaction tx = igniteTransactions.begin();

        var table = accounts.recordView();

        assertTrue(table.insert(tx, makeValue(1, 100.)));
        assertFalse(table.insert(tx, makeValue(1, 200.)));
        assertEquals(100., table.get(tx, makeKey(1)).doubleValue("balance"));

        tx.commit();

        assertEquals(100., accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));

        assertTrue(accounts.recordView().insert(null, makeValue(2, 200.)));
        assertEquals(200., accounts.recordView().get(null, makeKey(2)).doubleValue("balance"));

        Transaction tx2 = igniteTransactions.begin();

        table = accounts.recordView();

        assertTrue(table.insert(tx2, makeValue(3, 100.)));
        assertFalse(table.insert(tx2, makeValue(3, 200.)));
        assertEquals(100., table.get(tx2, makeKey(3)).doubleValue("balance"));

        tx2.rollback();

        assertEquals(100., accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));
        assertEquals(200., accounts.recordView().get(null, makeKey(2)).doubleValue("balance"));
        assertNull(accounts.recordView().get(null, makeKey(3)));
    }

    @Test
    public void testDelete() throws TransactionException {
        Tuple key = makeKey(1);

        assertFalse(accounts.recordView().delete(null, key));
        assertNull(accounts.recordView().get(null, key));

        accounts.recordView().upsert(null, makeValue(1, 100.));

        igniteTransactions.runInTransaction(tx -> {
            assertNotNull(accounts.recordView().get(tx, key));
            assertTrue(accounts.recordView().delete(tx, key));
            assertNull(accounts.recordView().get(tx, key));
        });

        assertNull(accounts.recordView().get(null, key));
        accounts.recordView().upsert(null, makeValue(1, 100.));
        assertNotNull(accounts.recordView().get(null, key));

        Tuple key2 = makeKey(2);

        accounts.recordView().upsert(null, makeValue(2, 100.));

        assertThrows(RuntimeException.class, () -> igniteTransactions.runInTransaction((Consumer<Transaction>) tx -> {
            assertNotNull(accounts.recordView().get(tx, key2));
            assertTrue(accounts.recordView().delete(tx, key2));
            assertNull(accounts.recordView().get(tx, key2));
            throw new RuntimeException(); // Triggers rollback.
        }));

        assertNotNull(accounts.recordView().get(null, key2));
        assertTrue(accounts.recordView().delete(null, key2));
        assertNull(accounts.recordView().get(null, key2));
    }

    @Test
    public void testGetAll() {
        List<Tuple> keys = List.of(makeKey(1), makeKey(2));

        Collection<Tuple> ret = accounts.recordView().getAll(null, keys);

        assertThat(ret, contains(null, null));

        accounts.recordView().upsert(null, makeValue(1, 100.));
        accounts.recordView().upsert(null, makeValue(2, 200.));

        ret = new ArrayList<>(accounts.recordView().getAll(null, keys));

        validateBalance(ret, 100., 200.);
    }

    /**
     * Tests if a transaction is rolled back if one of the batch keys can't be locked.
     */
    @Test
    public void testGetAllAbort() throws TransactionException {
        List<Tuple> keys = List.of(makeKey(1), makeKey(2));

        accounts.recordView().upsertAll(null, List.of(makeValue(1, 100.), makeValue(2, 200.)));

        Transaction tx = igniteTransactions.begin();

        RecordView<Tuple> txAcc = accounts.recordView();

        txAcc.upsert(tx, makeValue(1, 300.));
        validateBalance(txAcc.getAll(tx, keys), 300., 200.);

        tx.rollback();

        validateBalance(accounts.recordView().getAll(null, keys), 100., 200.);
    }

    /**
     * Tests if a transaction is rolled back if one of the batch keys can't be locked.
     */
    @Test
    public void testGetAllConflict() throws Exception {
        accounts.recordView().upsertAll(null, List.of(makeValue(1, 100.), makeValue(2, 200.)));

        InternalTransaction tx1 = (InternalTransaction) igniteTransactions.begin();
        InternalTransaction tx2 = (InternalTransaction) igniteTransactions.begin();

        RecordView<Tuple> txAcc = accounts.recordView();
        RecordView<Tuple> txAcc2 = accounts.recordView();

        txAcc2.upsert(tx1, makeValue(1, 300.));
        txAcc.upsert(tx2, makeValue(2, 400.));

        Exception err = assertThrows(Exception.class, () -> txAcc.getAll(tx2, List.of(makeKey(2), makeKey(1))));
        assertTrue(err.getMessage().contains("Failed to acquire a lock"), err.getMessage());

        validateBalance(txAcc2.getAll(tx1, List.of(makeKey(2), makeKey(1))), 200., 300.);
        validateBalance(txAcc2.getAll(tx1, List.of(makeKey(1), makeKey(2))), 300., 200.);

        assertTrue(IgniteTestUtils.waitForCondition(() -> TxState.ABORTED == tx2.state(), 5_000), tx2.state().toString());

        tx1.commit();

        validateBalance(accounts.recordView().getAll(null, List.of(makeKey(2), makeKey(1))), 200., 300.);
    }

    @Test
    public void testPutAll() throws TransactionException {
        igniteTransactions.runInTransaction(tx -> {
            accounts.recordView().upsertAll(tx, List.of(makeValue(1, 100.), makeValue(2, 200.)));
        });

        validateBalance(accounts.recordView().getAll(null, List.of(makeKey(2), makeKey(1))), 200., 100.);

        assertThrows(IgniteException.class, () -> igniteTransactions.runInTransaction(tx -> {
            accounts.recordView().upsertAll(tx, List.of(makeValue(3, 300.), makeValue(4, 400.)));
            if (true) {
                throw new IgniteException();
            }
        }));

        assertNull(accounts.recordView().get(null, makeKey(3)));
        assertNull(accounts.recordView().get(null, makeKey(4)));
    }

    @Test
    public void testInsertAll() throws TransactionException {
        accounts.recordView().upsertAll(null, List.of(makeValue(1, 100.), makeValue(2, 200.)));

        igniteTransactions.runInTransaction(
                tx -> {
                    Collection<Tuple> res = accounts.recordView().insertAll(
                            tx,
                            List.of(makeValue(1, 200.), makeValue(3, 300.))
                    );

                    assertEquals(1, res.size());
                });

        validateBalance(
                accounts.recordView().getAll(
                        null,
                        List.of(makeKey(1), makeKey(2), makeKey(3))
                ),
                100., 200., 300.
        );
    }

    @Test
    public void testDeleteAll() throws TransactionException {
        accounts.recordView().upsertAll(null, List.of(makeValue(1, 100.), makeValue(2, 200.)));

        igniteTransactions.runInTransaction(tx -> {
            Collection<Tuple> res = accounts.recordView().deleteAll(tx, List.of(makeKey(1), makeKey(2), makeKey(3)));

            assertEquals(1, res.size());
        });

        assertNull(accounts.recordView().get(null, makeKey(1)));
        assertNull(accounts.recordView().get(null, makeKey(2)));
    }

    @Test
    public void testReplace() throws TransactionException {
        accounts.recordView().upsert(null, makeValue(1, 100.));

        igniteTransactions.runInTransaction(tx -> {
            assertFalse(accounts.recordView().replace(tx, makeValue(2, 200.)));
            assertTrue(accounts.recordView().replace(tx, makeValue(1, 200.)));
        });

        validateBalance(accounts.recordView().getAll(null, List.of(makeKey(1))), 200.);
    }

    @Test
    public void testGetAndReplace() throws TransactionException {
        accounts.recordView().upsert(null, makeValue(1, 100.));

        igniteTransactions.runInTransaction(tx -> {
            assertNull(accounts.recordView().getAndReplace(tx, makeValue(2, 200.)));
            assertNotNull(accounts.recordView().getAndReplace(tx, makeValue(1, 200.)));
        });

        validateBalance(accounts.recordView().getAll(null, List.of(makeKey(1))), 200.);
    }

    @Test
    public void testDeleteExact() throws TransactionException {
        accounts.recordView().upsert(null, makeValue(1, 100.));

        igniteTransactions.runInTransaction(tx -> {
            assertFalse(accounts.recordView().deleteExact(tx, makeValue(1, 200.)));
            assertTrue(accounts.recordView().deleteExact(tx, makeValue(1, 100.)));
        });

        Tuple actual = accounts.recordView().get(null, makeKey(1));

        assertNull(actual);
    }

    @Test
    public void testDeleteAllExact() throws TransactionException {
        accounts.recordView().upsertAll(null, List.of(makeValue(1, 100.), makeValue(2, 200.)));

        igniteTransactions.runInTransaction(
                tx -> {
                    Collection<Tuple> res = accounts.recordView().deleteAllExact(
                            tx,
                            List.of(makeValue(1, 200.), makeValue(2, 200.), makeValue(3, 300.))
                    );

                    assertEquals(2, res.size());
                });

        assertNotNull(accounts.recordView().get(null, makeKey(1)));
        assertNull(accounts.recordView().get(null, makeKey(2)));
    }

    @Test
    public void testGetAndPut() throws TransactionException {
        accounts.recordView().upsert(null, makeValue(1, 100.));

        igniteTransactions.runInTransaction(tx -> {
            assertNotNull(accounts.recordView().getAndUpsert(tx, makeValue(1, 200.)));
            assertNull(accounts.recordView().getAndUpsert(tx, makeValue(2, 200.)));
        });

        validateBalance(accounts.recordView().getAll(null, List.of(makeKey(1), makeKey(2))), 200., 200.);
    }

    @Test
    public void testGetAndDelete() throws TransactionException {
        accounts.recordView().upsert(null, makeValue(1, 100.));

        igniteTransactions.runInTransaction(tx -> {
            assertEquals(100., accounts.recordView().getAndDelete(tx, makeKey(1)).doubleValue("balance"));
        });

        assertNull(accounts.recordView().get(null, makeKey(1)));
    }

    @Test
    public void testRollbackUpgradedLock() throws Exception {
        accounts.recordView().upsert(null, makeValue(1, 100.));

        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();
        InternalTransaction tx2 = (InternalTransaction) igniteTransactions.begin();

        var table = accounts.recordView();
        var table2 = accounts.recordView();

        double v0 = table.get(tx, makeKey(1)).doubleValue("balance");
        double v1 = table2.get(tx2, makeKey(1)).doubleValue("balance");

        assertEquals(v0, v1);

        // Try to upgrade a lock.
        table2.upsertAsync(tx2, makeValue(1, v0 + 10));
        Thread.sleep(300); // Give some time to update lock queue TODO asch IGNITE-15928

        tx2.rollback();
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15938") // TODO asch IGNITE-15938
    public void testUpgradedLockInvalidation() throws Exception {
        accounts.recordView().upsert(null, makeValue(1, 100.));

        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();
        InternalTransaction tx2 = (InternalTransaction) igniteTransactions.begin();

        var table = accounts.recordView();
        var table2 = accounts.recordView();

        double v0 = table.get(tx, makeKey(1)).doubleValue("balance");
        double v1 = table2.get(tx2, makeKey(1)).doubleValue("balance");

        assertEquals(v0, v1);

        // Try to upgrade a lock.
        table2.upsertAsync(tx2, makeValue(1, v0 + 10));
        Thread.sleep(300); // Give some time to update lock queue TODO asch IGNITE-15928

        table.upsert(tx, makeValue(1, v0 + 20));

        tx.commit();
        assertThrows(Exception.class, () -> tx2.commit());
    }

    @Test
    public void testReorder() throws Exception {
        accounts.recordView().upsert(null, makeValue(1, 100.));

        InternalTransaction tx1 = (InternalTransaction) igniteTransactions.begin();
        InternalTransaction tx2 = (InternalTransaction) igniteTransactions.begin();
        InternalTransaction tx3 = (InternalTransaction) igniteTransactions.begin();

        var table = accounts.recordView();
        var table2 = accounts.recordView();
        var table3 = accounts.recordView();

        double v0 = table.get(tx3, makeKey(1)).doubleValue("balance");

        table.upsertAsync(tx3, makeValue(1, v0 + 10));

        CompletableFuture<Tuple> fut = table2.getAsync(tx2, makeKey(1));
        assertFalse(fut.isDone());

        CompletableFuture<Tuple> fut2 = table3.getAsync(tx1, makeKey(1));
        assertFalse(fut2.isDone());
    }

    @Test
    public void testCrossTable() throws Exception {
        customers.recordView().upsert(null, makeValue(1, "test"));
        accounts.recordView().upsert(null, makeValue(1, 100.));

        assertEquals("test", customers.recordView().get(null, makeKey(1)).stringValue("name"));
        assertEquals(100., accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));

        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();

        var txCust = customers.recordView();
        var txAcc = accounts.recordView();

        txCust.upsert(tx, makeValue(1, "test2"));
        txAcc.upsert(tx, makeValue(1, 200.));

        Tuple txValCust = txCust.get(tx, makeKey(1));
        assertEquals("test2", txValCust.stringValue("name"));

        txValCust.set("accountNumber", 2L);
        txValCust.set("name", "test3");

        Tuple txValAcc = txAcc.get(tx, makeKey(1));
        assertEquals(200., txValAcc.doubleValue("balance"));

        txValAcc.set("accountNumber", 2L);
        txValAcc.set("balance", 300.);

        tx.commit();

        assertEquals("test2", customers.recordView().get(null, makeKey(1)).stringValue("name"));
        assertEquals(200., accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));

        assertTrue(IgniteTestUtils.waitForCondition(() -> lockManager(accounts).isEmpty(), 10_000));
        assertTrue(IgniteTestUtils.waitForCondition(() -> lockManager(customers).isEmpty(), 10_000));
    }

    @Test
    public void testTwoTables() throws Exception {
        customers.recordView().upsert(null, makeValue(1, "test"));
        accounts.recordView().upsert(null, makeValue(1, 100.));

        assertEquals("test", customers.recordView().get(null, makeKey(1)).stringValue("name"));
        assertEquals(100., accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));

        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();
        InternalTransaction tx2 = (InternalTransaction) igniteTransactions.begin();

        var txCust = customers.recordView();
        var txAcc = accounts.recordView();

        txCust.upsert(tx, makeValue(1, "test2"));
        txAcc.upsert(tx, makeValue(1, 200.));

        Tuple txValCust = txCust.get(tx, makeKey(1));
        assertEquals("test2", txValCust.stringValue("name"));

        txValCust.set("accountNumber", 2L);
        txValCust.set("name", "test3");

        Tuple txValAcc = txAcc.get(tx, makeKey(1));
        assertEquals(200., txValAcc.doubleValue("balance"));

        txValAcc.set("accountNumber", 2L);
        txValAcc.set("balance", 300.);

        tx.commit();
        tx2.commit();

        assertEquals("test2", customers.recordView().get(null, makeKey(1)).stringValue("name"));
        assertEquals(200., accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));

        assertTrue(IgniteTestUtils.waitForCondition(() -> lockManager(accounts).isEmpty(), 10_000));
    }

    @Test
    public void testCrossTableKeyValueView() throws Exception {
        customers.recordView().upsert(null, makeValue(1L, "test"));
        accounts.recordView().upsert(null, makeValue(1L, 100.));

        assertEquals("test", customers.recordView().get(null, makeKey(1)).stringValue("name"));
        assertEquals(100., accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));

        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();
        InternalTransaction tx2 = (InternalTransaction) igniteTransactions.begin();

        var txCust = customers.keyValueView();
        var txAcc = accounts.keyValueView();

        txCust.put(tx, makeKey(1), makeValue("test2"));
        txAcc.put(tx, makeKey(1), makeValue(200.));

        Tuple txValCust = txCust.get(tx, makeKey(1));
        assertEquals("test2", txValCust.stringValue("name"));

        txValCust.set("accountNumber", 2L);
        txValCust.set("name", "test3");

        Tuple txValAcc = txAcc.get(tx, makeKey(1));
        assertEquals(200., txValAcc.doubleValue("balance"));

        txValAcc.set("accountNumber", 2L);
        txValAcc.set("balance", 300.);

        tx.commit();
        tx2.commit();

        assertEquals("test2", customers.recordView().get(null, makeKey(1)).stringValue("name"));
        assertEquals(200., accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));

        assertTrue(IgniteTestUtils.waitForCondition(() -> lockManager(accounts).isEmpty(), 10_000));
    }

    @Test
    public void testCrossTableAsync() throws Exception {
        customers.recordView().upsert(null, makeValue(1, "test"));
        accounts.recordView().upsert(null, makeValue(1, 100.));

        igniteTransactions.beginAsync()
                .thenCompose(
                        tx -> accounts.recordView().upsertAsync(tx, makeValue(1, 200.))
                                .thenCombine(customers.recordView().upsertAsync(tx, makeValue(1, "test2")), (v1, v2) -> tx)
                )
                .thenCompose(Transaction::commitAsync).join();

        assertEquals("test2", customers.recordView().get(null, makeKey(1)).stringValue("name"));
        assertEquals(200., accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));

        assertTrue(IgniteTestUtils.waitForCondition(() -> lockManager(accounts).isEmpty(), 10_000));
    }

    @Test
    public void testCrossTableAsyncRollback() throws Exception {
        customers.recordView().upsert(null, makeValue(1, "test"));
        accounts.recordView().upsert(null, makeValue(1, 100.));

        igniteTransactions.beginAsync()
                .thenCompose(
                        tx -> accounts.recordView().upsertAsync(tx, makeValue(1, 200.))
                                .thenCombine(customers.recordView().upsertAsync(tx, makeValue(1, "test2")), (v1, v2) -> tx)
                )
                .thenCompose(Transaction::rollbackAsync).join();

        assertEquals("test", customers.recordView().get(null, makeKey(1)).stringValue("name"));
        assertEquals(100., accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));

        assertTrue(IgniteTestUtils.waitForCondition(() -> lockManager(accounts).isEmpty(), 10_000));
    }

    @Test
    public void testCrossTableAsyncKeyValueView() throws Exception {
        customers.recordView().upsert(null, makeValue(1, "test"));
        accounts.recordView().upsert(null, makeValue(1, 100.));

        igniteTransactions.beginAsync()
                .thenCompose(
                        tx -> accounts.keyValueView().putAsync(tx, makeKey(1), makeValue(200.))
                                .thenCombine(customers.keyValueView().putAsync(tx, makeKey(1), makeValue("test2")),
                                        (v1, v2) -> tx)
                )
                .thenCompose(Transaction::commitAsync).join();

        assertEquals("test2", customers.recordView().get(null, makeKey(1)).stringValue("name"));
        assertEquals(200., accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));

        assertTrue(IgniteTestUtils.waitForCondition(() -> lockManager(accounts).isEmpty(), 10_000));
    }

    @Test
    public void testCrossTableAsyncKeyValueViewRollback() throws Exception {
        customers.recordView().upsert(null, makeValue(1, "test"));
        accounts.recordView().upsert(null, makeValue(1, 100.));

        igniteTransactions.beginAsync()
                .thenCompose(
                        tx -> accounts.keyValueView().putAsync(tx, makeKey(1), makeValue(200.))
                                .thenCombine(customers.keyValueView().putAsync(tx, makeKey(1), makeValue("test2")),
                                        (v1, v2) -> tx)
                )
                .thenCompose(Transaction::rollbackAsync).join();

        assertEquals("test", customers.recordView().get(null, makeKey(1)).stringValue("name"));
        assertEquals(100., accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));

        assertTrue(IgniteTestUtils.waitForCondition(() -> lockManager(accounts).isEmpty(), 10_000));
    }

    @Test
    public void testBalance() throws InterruptedException {
        doTestSingleKeyMultithreaded(5_000, false);
    }

    @Test
    public void testLockedTooLong() {
        // TODO asch IGNITE-15936 if lock can't be acquired until timeout tx should be rolled back.
    }

    @Test
    public void testScan() throws Exception {
        doTestScan(null);
    }

    @Test
    public void testScanExplicit() throws Exception {
        igniteTransactions.runInTransaction(this::doTestScan);
    }

    /**
     * Do scan in test.
     *
     * @param tx The transaction.
     */
    private void doTestScan(@Nullable Transaction tx) {
        accounts.recordView().upsertAll(tx, List.of(makeValue(1, 100.), makeValue(2, 200.)));

        CompletableFuture<List<Tuple>> scanFut = scan(accounts.internalTable(), tx == null ? null : (InternalTransaction) tx);

        var rows = scanFut.join();

        Map<Long, Tuple> map = new HashMap<>();

        for (Tuple row : rows) {
            map.put(row.longValue("accountNumber"), row);
        }

        assertEquals(100., map.get(1L).doubleValue("balance"));
        assertEquals(200., map.get(2L).doubleValue("balance"));

        // Attempt to overwrite.
        accounts.recordView().upsertAll(tx, List.of(makeValue(1, 300.), makeValue(2, 400.)));
    }

    /**
     * Scans {@code 0} partition of a table in a specific transaction or implicit one.
     *
     * @param internalTable Internal table to scanning.
     * @param internalTx Internal transaction of {@code null}.
     * @return Future to scanning result.
     */
    private CompletableFuture<List<Tuple>> scan(InternalTable internalTable, @Nullable InternalTransaction internalTx) {
        Flow.Publisher<BinaryRow> pub = internalTx != null && internalTx.isReadOnly()
                ?
                internalTable.scan(
                        0,
                        internalTx.id(),
                        internalTx.readTimestamp(),
                        internalTable.tableRaftService().leaderAssignment(0),
                        internalTx.coordinatorId()
                )
                : internalTable.scan(0, internalTx);

        List<Tuple> rows = new ArrayList<>();

        var fut = new CompletableFuture<List<Tuple>>();

        pub.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(3);
            }

            @Override
            public void onNext(BinaryRow item) {
                SchemaRegistry registry = accounts.schemaView();
                Row row = registry.resolve(item, registry.lastKnownSchema());

                rows.add(TableRow.tuple(row));
            }

            @Override
            public void onError(Throwable throwable) {
                // No-op.
            }

            @Override
            public void onComplete() {
                fut.complete(rows);
            }
        });

        return fut;
    }

    @Test
    public void testComplexImplicit() {
        doTestComplex(accounts.recordView(), null);
    }

    @Test
    public void testComplexExplicit() throws TransactionException {
        doTestComplex(accounts.recordView(), igniteTransactions.begin());
    }

    @Test
    public void testComplexImplicitKeyValueView() {
        doTestComplexKeyValue(accounts.keyValueView(), null);
    }

    @Test
    public void testComplexExplicitKeyValueView() throws TransactionException {
        doTestComplexKeyValue(accounts.keyValueView(), igniteTransactions.begin());
    }

    /**
     * Checks operation over tuple record view. The scenario was moved from ITDistributedTableTest.
     *
     * @param view Record view.
     * @param tx Transaction or {@code null} for implicit one.
     */
    private void doTestComplex(RecordView<Tuple> view, @Nullable Transaction tx) {
        final int keysCnt = 10;

        long start = System.nanoTime();

        for (long i = 0; i < keysCnt; i++) {
            view.insert(tx, makeValue(i, i + 2.));
        }

        long dur = (long) ((System.nanoTime() - start) / 1000 / 1000.);

        log.info("Inserted={}, time={}ms  avg={} tps={}", keysCnt, dur, dur / keysCnt, 1000 / (dur / (float) keysCnt));

        for (long i = 0; i < keysCnt; i++) {
            Tuple entry = view.get(tx, makeKey(i));

            assertEquals(i + 2., entry.doubleValue("balance"));
        }

        for (int i = 0; i < keysCnt; i++) {
            view.upsert(tx, makeValue(i, i + 5.));

            Tuple entry = view.get(tx, makeKey(i));

            assertEquals(i + 5., entry.doubleValue("balance"));
        }

        HashSet<Tuple> keys = new HashSet<>();

        for (long i = 0; i < keysCnt; i++) {
            keys.add(makeKey(i));
        }

        Collection<Tuple> entries = view.getAll(tx, keys);

        assertEquals(keysCnt, entries.size());

        for (long i = 0; i < keysCnt; i++) {
            boolean res = view.replace(tx, makeValue(i, i + 5.), makeValue(i, i + 2.));

            assertTrue(res, "Failed to replace for idx=" + i);
        }

        for (long i = 0; i < keysCnt; i++) {
            boolean res = view.delete(tx, makeKey(i));

            assertTrue(res);

            Tuple entry = view.get(tx, makeKey(i));

            assertNull(entry);
        }

        ArrayList<Tuple> batch = new ArrayList<>(keysCnt);

        for (long i = 0; i < keysCnt; i++) {
            batch.add(makeValue(i, i + 2.));
        }

        view.upsertAll(tx, batch);

        for (long i = 0; i < keysCnt; i++) {
            Tuple entry = view.get(tx, makeKey(i));

            assertEquals(i + 2., entry.doubleValue("balance"));
        }

        view.deleteAll(tx, keys);

        for (Tuple key : keys) {
            Tuple entry = view.get(tx, key);

            assertNull(entry);
        }

        if (tx != null) {
            tx.commit();
        }
    }

    /**
     * Checks operation over tuple key value view. The scenario was moved from ITDistributedTableTest.
     *
     * @param view Table view.
     * @param tx Transaction or {@code null} for implicit one.
     */
    public void doTestComplexKeyValue(KeyValueView<Tuple, Tuple> view, @Nullable Transaction tx) {
        final int keysCnt = 10;

        for (long i = 0; i < keysCnt; i++) {
            view.put(tx, makeKey(i), makeValue(i + 2.));
        }

        for (long i = 0; i < keysCnt; i++) {
            Tuple entry = view.get(tx, makeKey(i));

            assertEquals(i + 2., entry.doubleValue("balance"));
        }

        for (int i = 0; i < keysCnt; i++) {
            view.put(tx, makeKey(i), makeValue(i + 5.));

            Tuple entry = view.get(tx, makeKey(i));

            assertEquals(i + 5., entry.doubleValue("balance"));
        }

        HashSet<Tuple> keys = new HashSet<>();

        for (long i = 0; i < keysCnt; i++) {
            keys.add(makeKey(i));
        }

        Map<Tuple, Tuple> entries = view.getAll(tx, keys);

        assertEquals(keysCnt, entries.size());

        for (long i = 0; i < keysCnt; i++) {
            boolean res = view.replace(tx, makeKey(i), makeValue(i + 5.), makeValue(i + 2.));

            assertTrue(res, "Failed to replace for idx=" + i);
        }

        for (long i = 0; i < keysCnt; i++) {
            boolean res = view.remove(tx, makeKey(i));

            assertTrue(res);

            Tuple entry = view.get(tx, makeKey(i));

            assertNull(entry);
        }

        Map<Tuple, Tuple> batch = new LinkedHashMap<>(keysCnt);

        for (long i = 0; i < keysCnt; i++) {
            batch.put(makeKey(i), makeValue(i + 2.));
        }

        view.putAll(tx, batch);

        for (long i = 0; i < keysCnt; i++) {
            Tuple entry = view.get(tx, makeKey(i));

            assertEquals(i + 2., entry.doubleValue("balance"));
        }

        view.removeAll(tx, keys);

        for (Tuple key : keys) {
            Tuple entry = view.get(tx, key);

            assertNull(entry);
        }

        if (tx != null) {
            tx.commit();
        }
    }

    /**
     * Performs a test.
     *
     * @param duration The duration.
     * @param verbose Verbose mode.
     * @throws InterruptedException If interrupted while waiting.
     */
    private void doTestSingleKeyMultithreaded(long duration, boolean verbose) throws InterruptedException {
        int threadsCnt = Runtime.getRuntime().availableProcessors() * 2;

        Thread[] threads = new Thread[threadsCnt];

        final int accountsCount = threads.length * 10;

        final double initial = 1000;
        final double total = accountsCount * initial;

        for (int i = 0; i < accountsCount; i++) {
            accounts.recordView().upsert(null, makeValue(i, 1000));
        }

        double total0 = 0;

        for (long i = 0; i < accountsCount; i++) {
            double balance = accounts.recordView().get(null, makeKey(i)).doubleValue("balance");

            total0 += balance;
        }

        assertEquals(total, total0, "Total amount invariant is not preserved");

        CyclicBarrier startBar = new CyclicBarrier(threads.length, () -> log.info("Before test"));

        LongAdder ops = new LongAdder();
        LongAdder fails = new LongAdder();

        AtomicBoolean stop = new AtomicBoolean();

        Random r = new Random();

        AtomicReference<Throwable> firstErr = new AtomicReference<>();

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        startBar.await();
                    } catch (Exception e) {
                        fail();
                    }

                    while (!stop.get() && firstErr.get() == null) {
                        InternalTransaction tx = clientTxManager().begin(timestampTracker);

                        var table = accounts.recordView();

                        try {
                            long acc1 = r.nextInt(accountsCount);

                            double amount = 100 + r.nextInt(500);

                            if (verbose) {
                                log.info("op=tryGet ts={} id={}", tx.id(), acc1);
                            }

                            double val0 = table.get(tx, makeKey(acc1)).doubleValue("balance");

                            long acc2 = acc1;

                            while (acc1 == acc2) {
                                acc2 = r.nextInt(accountsCount);
                            }

                            if (verbose) {
                                log.info("op=tryGet ts={} id={}", tx.id(), acc2);
                            }

                            double val1 = table.get(tx, makeKey(acc2)).doubleValue("balance");

                            if (verbose) {
                                log.info("op=tryPut ts={} id={}", tx.id(), acc1);
                            }

                            table.upsert(tx, makeValue(acc1, val0 - amount));

                            if (verbose) {
                                log.info("op=tryPut ts={} id={}", tx.id(), acc2);
                            }

                            table.upsert(tx, makeValue(acc2, val1 + amount));

                            tx.commit();

                            ops.increment();
                        } catch (Exception e) {
                            assertTrue(e.getMessage().contains("Failed to acquire a lock"), e.getMessage());

                            tx.rollback();

                            fails.increment();
                        }
                    }
                }
            });

            threads[i].setName("Worker-" + i);
            threads[i].setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    firstErr.compareAndExchange(null, e);
                }
            });
            threads[i].start();
        }

        Thread.sleep(duration);

        stop.set(true);

        for (Thread thread : threads) {
            thread.join(3_000);
        }

        if (firstErr.get() != null) {
            throw new IgniteException(firstErr.get());
        }

        log.info("After test ops={} fails={}", ops.sum(), fails.sum());

        total0 = 0;

        for (long i = 0; i < accountsCount; i++) {
            double balance = accounts.recordView().get(null, makeKey(i)).doubleValue("balance");

            total0 += balance;
        }

        assertEquals(total, total0, "Total amount invariant is not preserved");
    }

    /**
     * Makes a key.
     *
     * @param id The id.
     * @return The key tuple.
     */
    protected Tuple makeKey(long id) {
        return Tuple.create().set("accountNumber", id);
    }

    /**
     * Makes a tuple containing key and value.
     *
     * @param id The id.
     * @param balance The balance.
     * @return The value tuple.
     */
    protected Tuple makeValue(long id, double balance) {
        return Tuple.create().set("accountNumber", id).set("balance", balance);
    }

    /**
     * Makes a tuple containing key and value.
     *
     * @param id The id.
     * @param name The name.
     * @return The value tuple.
     */
    private Tuple makeValue(long id, String name) {
        return Tuple.create().set("accountNumber", id).set("name", name);
    }

    /**
     * Makes a value.
     *
     * @param balance The balance.
     * @return The value tuple.
     */
    private Tuple makeValue(double balance) {
        return Tuple.create().set("balance", balance);
    }

    /**
     * Makes a value.
     *
     * @param name The name.
     * @return The value tuple.
     */
    private Tuple makeValue(String name) {
        return Tuple.create().set("name", name);
    }

    /**
     * Get a lock manager on a partition leader.
     *
     * @param t The table.
     * @return Lock manager.
     */
    protected LockManager lockManager(TableViewInternal t) {
        return txManager(t).lockManager();
    }

    /**
     * Validates balances.
     *
     * @param rows Rows.
     * @param expected Expected values.
     */
    protected static void validateBalance(Collection<Tuple> rows, @Nullable Double... expected) {
        assertThat(
                rows.stream().map(tuple -> tuple == null ? null : tuple.doubleValue("balance")).collect(toList()),
                contains(expected)
        );
    }

    /**
     * Transfers money between accounts.
     *
     * @param balance1 First account initial balance.
     * @param balance2 Second account initial balance.
     * @param delta Delta.
     * @return The future holding tuple with previous balances.
     */
    private CompletableFuture<Tuple> transferAsync(double balance1, double balance2, double delta) {
        RecordView<Tuple> view = accounts.recordView();

        view.upsert(null, makeValue(1, balance1));
        view.upsert(null, makeValue(2, balance2));

        return igniteTransactions.runInTransactionAsync(tx -> {
            // Attempt to withdraw from first account.
            CompletableFuture<Double> fut1 = view.getAsync(tx, makeKey(1))
                    .thenCompose(val1 -> {
                        double prev = val1.doubleValue("balance");
                        double balance = prev - delta;

                        if (balance < 0) {
                            return tx.rollbackAsync().thenApply(ignored -> null);
                        }

                        return view.upsertAsync(tx, makeValue(1, balance)).thenApply(ignored -> prev);
                    });

            // Optimistically deposit to second account.
            CompletableFuture<Double> fut2 = view.getAsync(tx, makeKey(2))
                    .thenCompose(val2 -> {
                        double prev = val2.doubleValue("balance");
                        return view.upsertAsync(tx, makeValue(2, prev + delta)).thenApply(ignored -> prev);
                    });

            return fut1.thenCompose(val1 -> fut2.thenCompose(val2 ->
                    completedFuture(Tuple.create().set("balance1", val1).set("balance2", val2))));
        });
    }

    @Test
    public void testReadOnlyGet() {
        accounts.recordView().upsert(null, makeValue(1, 100.));

        Transaction readOnlyTx = igniteTransactions.begin(new TransactionOptions().readOnly(true));
        assertEquals(100., accounts.recordView().get(readOnlyTx, makeKey(1)).doubleValue("balance"));
    }

    @Test
    public void testReadOnlyScan() throws Exception {
        accounts.recordView().upsert(null, makeValue(1, 100.));
        accounts.recordView().upsert(null, makeValue(2, 500.));

        // Pending tx
        Transaction tx = igniteTransactions.begin();
        accounts.recordView().upsert(tx, makeValue(1, 300.));
        accounts.recordView().delete(tx, makeKey(2));

        InternalTransaction readOnlyTx = (InternalTransaction) igniteTransactions.begin(new TransactionOptions().readOnly(true));

        CompletableFuture<List<Tuple>> roBeforeCommitTxFut = scan(accounts.internalTable(), readOnlyTx);

        var roBeforeCommitTxRows = roBeforeCommitTxFut.get(10, TimeUnit.SECONDS);

        assertEquals(2, roBeforeCommitTxRows.size());

        for (Tuple row : roBeforeCommitTxRows) {
            if (row.longValue("accountNumber") == 1) {
                assertEquals(100., row.doubleValue("balance"));
            } else {
                assertEquals(2, row.longValue("accountNumber"));
                assertEquals(500., row.doubleValue("balance"));
            }
        }

        // Commit pending tx.
        tx.commit();

        // Same read-only transaction.
        roBeforeCommitTxFut = scan(accounts.internalTable(), readOnlyTx);

        roBeforeCommitTxRows = roBeforeCommitTxFut.get(10, TimeUnit.SECONDS);

        assertEquals(2, roBeforeCommitTxRows.size());

        for (Tuple row : roBeforeCommitTxRows) {
            if (row.longValue("accountNumber") == 1) {
                assertEquals(100., row.doubleValue("balance"));
            } else {
                assertEquals(2, row.longValue("accountNumber"));
                assertEquals(500., row.doubleValue("balance"));
            }
        }

        // New read-only transaction.
        InternalTransaction readOnlyTx2 = (InternalTransaction) igniteTransactions.begin(new TransactionOptions().readOnly(true));

        CompletableFuture<List<Tuple>> roAfterCommitTxFut = scan(accounts.internalTable(), readOnlyTx2);

        var roAfterCommitTxRows = roAfterCommitTxFut.get(10, TimeUnit.SECONDS);

        assertEquals(1, roAfterCommitTxRows.size());

        for (Tuple row : roAfterCommitTxRows) {
            assertEquals(1, row.longValue("accountNumber"));
            assertEquals(300., row.doubleValue("balance"));
        }
    }

    @Test
    public void testReadOnlyGetWriteIntentResolutionUpdate() {
        accounts.recordView().upsert(null, makeValue(1, 100.));

        // Pending tx
        Transaction tx = igniteTransactions.begin();
        accounts.recordView().upsert(tx, makeValue(1, 300.));

        // Update
        Transaction readOnlyTx = igniteTransactions.begin(new TransactionOptions().readOnly(true));
        assertEquals(100., accounts.recordView().get(readOnlyTx, makeKey(1)).doubleValue("balance"));

        // Commit pending tx.
        tx.commit();

        // Same read-only transaction.
        assertEquals(100., accounts.recordView().get(readOnlyTx, makeKey(1)).doubleValue("balance"));

        // New read-only transaction.
        Transaction readOnlyTx2 = igniteTransactions.begin(new TransactionOptions().readOnly(true));
        assertEquals(300., accounts.recordView().get(readOnlyTx2, makeKey(1)).doubleValue("balance"));
    }

    @Test
    public void testReadOnlyGetWriteIntentResolutionRemove() {
        accounts.recordView().upsert(null, makeValue(1, 100.));

        // Pending tx
        Transaction tx = igniteTransactions.begin();
        accounts.recordView().delete(tx, makeKey(1));

        // Remove.
        Transaction readOnlyTx = igniteTransactions.begin(new TransactionOptions().readOnly(true));
        assertEquals(100., accounts.recordView().get(readOnlyTx, makeKey(1)).doubleValue("balance"));

        // Commit pending tx.
        tx.commit();

        // Same read-only transaction.
        assertEquals(100., accounts.recordView().get(readOnlyTx, makeKey(1)).doubleValue("balance"));

        // New read-only transaction.
        Transaction readOnlyTx2 = igniteTransactions.begin(new TransactionOptions().readOnly(true));
        Tuple row = accounts.recordView().get(readOnlyTx2, makeKey(1));
        assertNull(row);
    }

    @Test
    public void testReadOnlyGetAll() {
        accounts.recordView().upsert(null, makeValue(1, 100.));
        accounts.recordView().upsert(null, makeValue(2, 200.));
        accounts.recordView().upsert(null, makeValue(3, 300.));

        Transaction readOnlyTx = igniteTransactions.begin(new TransactionOptions().readOnly(true));
        Collection<Tuple> retrievedKeys = accounts.recordView().getAll(readOnlyTx, List.of(makeKey(1), makeKey(2)));
        validateBalance(retrievedKeys, 100., 200.);
    }

    @Test
    public void testReadOnlyPendingWriteIntentSkippedCombined() {
        accounts.recordView().upsert(null, makeValue(1, 100.));
        accounts.recordView().upsert(null, makeValue(2, 200.));

        // Pending tx
        Transaction tx = igniteTransactions.begin();
        accounts.recordView().delete(tx, makeKey(1));
        accounts.recordView().upsert(tx, makeValue(2, 300.));

        Transaction readOnlyTx = igniteTransactions.begin(new TransactionOptions().readOnly(true));
        Collection<Tuple> retrievedKeys = accounts.recordView().getAll(readOnlyTx, List.of(makeKey(1), makeKey(2)));
        validateBalance(retrievedKeys, 100., 200.);

        // Commit pending tx.
        tx.commit();

        Collection<Tuple> retrievedKeys2 = accounts.recordView().getAll(readOnlyTx, List.of(makeKey(1), makeKey(2)));
        validateBalance(retrievedKeys2, 100., 200.);

        Transaction readOnlyTx2 = igniteTransactions.begin(new TransactionOptions().readOnly(true));
        Collection<Tuple> retrievedKeys3 = accounts.recordView().getAll(readOnlyTx2, List.of(makeKey(1), makeKey(2)));
        validateBalance(retrievedKeys3, null, 300.);
    }

    @Test
    public void testTransactionAlreadyCommitted() {
        testTransactionAlreadyFinished(true, true, (transaction, uuid) -> {
            transaction.commit();

            log.info("Committed transaction {}", uuid);
        });
    }

    @Test
    public void testTransactionAlreadyRolledback() {
        testTransactionAlreadyFinished(false, true, (transaction, uuid) -> {
            transaction.rollback();

            log.info("Rolled back transaction {}", uuid);
        });
    }

    @Test
    public void testImplicit() {
        accounts.recordView().upsert(null, makeValue(1, BALANCE_1));
        assertEquals(BALANCE_1, accounts.recordView().get(null, makeKey(1)).doubleValue("balance"));
    }

    @Test
    public void testWriteIntentResolutionFallbackToCommitPartitionPath() {
        accounts.recordView().upsert(null, makeValue(1, 100.));

        // Pending tx
        Transaction tx = igniteTransactions.begin();
        accounts.recordView().delete(tx, makeKey(1));

        // Imitate the restart of the client node, which is a tx coordinator, in order to make its volatile state unavailable.
        // Now coordinator path of the write intent resolution has no effect, and we should fallback to commit partition path.
        UUID txId = ((ReadWriteTransactionImpl) tx).id();

        for (TxManager txManager : txManagers()) {
            txManager.updateTxMeta(txId, old -> old == null ? null : new TxStateMeta(
                    old.txState(),
                    "restarted",
                    old.commitPartitionId(),
                    old.commitTimestamp()
            ));
        }

        // Read-only.
        Transaction readOnlyTx = igniteTransactions.begin(new TransactionOptions().readOnly(true));
        assertEquals(100., accounts.recordView().get(readOnlyTx, makeKey(1)).doubleValue("balance"));
    }

    @Test
    public void testSingleGet() {
        var accountRecordsView = accounts.recordView();

        accountRecordsView.upsert(null, makeValue(1, 100.));

        Transaction tx1 = igniteTransactions.begin();
        Transaction tx2 = igniteTransactions.begin();

        accountRecordsView.upsert(tx1, makeValue(1, 200.));

        assertThrows(TransactionException.class, () -> accountRecordsView.get(tx2, makeKey(1)));

        assertEquals(100., accountRecordsView.get(null, makeKey(1)).doubleValue("balance"));

        tx1.commit();

        assertEquals(200., accountRecordsView.get(null, makeKey(1)).doubleValue("balance"));
    }

    @Test
    public void testBatchSinglePartitionGet() throws Exception {
        var accountRecordsView = accounts.recordView();

        SchemaRegistry schemaRegistry = accounts.schemaView();
        var marshaller = new TupleMarshallerImpl(schemaRegistry.lastKnownSchema());

        int partId = accounts.internalTable().partition(marshaller.marshalKey(makeKey(0)));

        ArrayList<Integer> keys = new ArrayList<>(10);
        keys.add(0);

        for (int i = 1; i < 10_000 && keys.size() < 10; i++) {
            var p = accounts.internalTable().partition(marshaller.marshalKey(makeKey(i)));

            if (p == partId) {
                keys.add(i);
            }
        }

        log.info("A batch of keys for a single partition is found [partId={}, keys{}]", partId, keys);

        accountRecordsView.upsertAll(null, keys.stream().map(k -> makeValue(k, 100.)).collect(toList()));

        Transaction tx1 = igniteTransactions.begin();
        Transaction tx2 = igniteTransactions.begin();

        accountRecordsView.upsertAll(tx1, keys.stream().map(k -> makeValue(k, 200.)).collect(toList()));

        assertThrows(TransactionException.class,
                () -> accountRecordsView.getAll(tx2, keys.stream().map(k -> makeKey(k)).collect(toList())));

        for (Tuple tuple : accountRecordsView.getAll(null, keys.stream().map(k -> makeKey(k)).collect(toList()))) {
            assertEquals(100., tuple.doubleValue("balance"));
        }

        tx1.commit();

        for (Tuple tuple : accountRecordsView.getAll(null, keys.stream().map(k -> makeKey(k)).collect(toList()))) {
            assertEquals(200., tuple.doubleValue("balance"));
        }
    }

    @Test
    public void testYoungerTransactionWithHigherPriorityWaitsForOlderTransactionCommit() {
        IgniteTransactionsImpl igniteTransactionsImpl = (IgniteTransactionsImpl) igniteTransactions;

        KeyValueView<Long, String> keyValueView = customers.keyValueView(Long.class, String.class);

        // Init data.
        keyValueView.put(null, 1L, "init");

        // Start low priority transaction.
        Transaction oldLowTx = igniteTransactionsImpl.beginWithPriority(false, TxPriority.LOW);

        // Update data.
        keyValueView.put(oldLowTx, 1L, "low");

        // Start normal priority transaction.
        Transaction youngNormalTx = igniteTransactionsImpl.beginWithPriority(false, TxPriority.NORMAL);

        // Try to update the same key with normal priority.
        CompletableFuture<String> objectCompletableFuture = CompletableFuture.supplyAsync(
                () -> keyValueView.getAndPut(youngNormalTx, 1L, "normal")
        );

        // Commit low priority transaction.
        oldLowTx.commit();

        // Wait for normal priority transaction to update the key.
        assertThat(objectCompletableFuture, willBe("low"));

        // Commit normal priority transaction.
        youngNormalTx.commit();

        // Check that normal priority transaction has updated the key.
        assertEquals("normal", keyValueView.get(null, 1L));
    }

    @ParameterizedTest
    @EnumSource(TxPriority.class)
    public void testYoungerTransactionThrowsExceptionIfKeyLockedByOlderTransactionWithSamePriority(TxPriority priority) {
        IgniteTransactionsImpl igniteTransactionsImpl = (IgniteTransactionsImpl) igniteTransactions;

        KeyValueView<Long, String> keyValueView = customers.keyValueView(Long.class, String.class);

        // Init data.
        keyValueView.put(null, 1L, "init");

        // Start the first transaction.
        Transaction oldNormalTx = igniteTransactionsImpl.beginWithPriority(false, priority);

        // Update data with the first transaction.
        keyValueView.put(oldNormalTx, 1L, "low");

        // Start the second transaction with the same priority.
        Transaction youngNormalTx = igniteTransactionsImpl.beginWithPriority(false, priority);

        // Try to update the same key with the second normal priority transaction.
        assertThrows(TransactionException.class, () -> keyValueView.put(youngNormalTx, 1L, "normal"));
    }

    @Test
    public void testIgniteTransactionsAndReadTimestamp() {
        Transaction readWriteTx = igniteTransactions.begin();
        assertFalse(readWriteTx.isReadOnly());
        assertNull(((InternalTransaction) readWriteTx).readTimestamp());

        Transaction readOnlyTx = igniteTransactions.begin(new TransactionOptions().readOnly(true));
        assertTrue(readOnlyTx.isReadOnly());
        assertNotNull(((InternalTransaction) readOnlyTx).readTimestamp());

        readWriteTx.commit();

        Transaction readOnlyTx2 = igniteTransactions.begin(new TransactionOptions().readOnly(true));
        readOnlyTx2.rollback();
    }

    /**
     * Checks operations that act after a transaction is committed, are finished with exception.
     *
     * @param commit True when transaction is committed, false the transaction is rolled back.
     * @param checkLocks Whether to check locks after.
     * @param finisher Finishing closure.
     */
    protected void testTransactionAlreadyFinished(
            boolean commit,
            boolean checkLocks,
            BiConsumer<Transaction, UUID> finisher
    ) {
        Transaction tx = igniteTransactions.begin();

        var txId = ((ReadWriteTransactionImpl) tx).id();

        log.info("Started transaction {}", txId);

        var accountsRv = accounts.recordView();

        accountsRv.upsert(tx, makeValue(1, 100.));
        accountsRv.upsert(tx, makeValue(2, 200.));

        Collection<Tuple> res = accountsRv.getAll(tx, List.of(makeKey(1), makeKey(2)));

        validateBalance(res, 100., 200.);

        finisher.accept(tx, txId);

        assertThrowsWithCode(TransactionException.class, TX_ALREADY_FINISHED_ERR,
                () -> accountsRv.get(tx, makeKey(1)), "Transaction is already finished");

        assertThrowsWithCode(TransactionException.class, TX_ALREADY_FINISHED_ERR,
                () -> accountsRv.delete(tx, makeKey(1)), "Transaction is already finished");

        assertThrowsWithCode(TransactionException.class, TX_ALREADY_FINISHED_ERR,
                () -> accountsRv.get(tx, makeKey(2)), "Transaction is already finished");

        assertThrowsWithCode(TransactionException.class, TX_ALREADY_FINISHED_ERR,
                () -> accountsRv.upsert(tx, makeValue(2, 300.)), "Transaction is already finished");

        if (checkLocks) {
            assertTrue(CollectionUtils.nullOrEmpty(txManager(accounts).lockManager().locks(txId)));
        }

        if (commit) {
            res = accountsRv.getAll(null, List.of(makeKey(1), makeKey(2)));

            validateBalance(res, 100., 200.);
        } else {
            res = accountsRv.getAll(null, List.of(makeKey(1), makeKey(2)));

            assertThat(res, contains(null, null));
        }
    }
}
