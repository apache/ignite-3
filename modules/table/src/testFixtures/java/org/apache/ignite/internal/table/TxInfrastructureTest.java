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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.distributed.ItTxTestCluster;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.NodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.partition.replicator.raft.ZonePartitionRaftListener;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicatorConstants;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.distributed.raft.TablePartitionProcessor;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Setup infrastructure for tx related test scenarios.
 */
@ExtendWith({MockitoExtension.class, ConfigurationExtension.class, ExecutorServiceExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public abstract class TxInfrastructureTest extends IgniteAbstractTest {
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

    protected HybridTimestampTracker timestampTracker = HybridTimestampTracker.atomicTracker(null);

    protected IgniteTransactions igniteTransactions;

    // TODO fsync can be turned on again after https://issues.apache.org/jira/browse/IGNITE-20195
    @InjectConfiguration("mock: { fsync: false }")
    protected RaftConfiguration raftConfiguration;

    @InjectConfiguration
    protected SystemLocalConfiguration systemLocalConfiguration;

    @InjectConfiguration("mock.properties.txnLockRetryCount=\"0\"")
    protected SystemDistributedConfiguration systemDistributedConfiguration;

    @InjectConfiguration
    protected TransactionConfiguration txConfiguration;

    @InjectConfiguration
    protected ReplicationConfiguration replicationConfiguration;

    @InjectExecutorService
    protected ScheduledExecutorService commonExecutor;

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
    public TxInfrastructureTest(TestInfo testInfo) {
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
            protected HybridClock createClock(InternalClusterNode node) {
                return TxInfrastructureTest.this.createClock(node);
            }

            @Override
            protected long getSafeTimePropagationTimeout() {
                return TxInfrastructureTest.this.getSafeTimePropagationTimeout();
            }
        };
        txTestCluster.prepareCluster();

        this.igniteTransactions = txTestCluster.igniteTransactions();

        accounts = txTestCluster.startTable(ACC_TABLE_NAME, ACCOUNTS_SCHEMA);
        customers = txTestCluster.startTable(CUST_TABLE_NAME, CUSTOMERS_SCHEMA);

        log.info("Tables have been started");
    }

    protected HybridClock createClock(InternalClusterNode node) {
        return new HybridClockImpl();
    }

    protected long getSafeTimePropagationTimeout() {
        return ReplicatorConstants.DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;
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

        assertThat(network.startAsync(new ComponentContext()), willCompleteSuccessfully());

        return network;
    }

    /** {@inheritDoc} */
    protected TxManager clientTxManager() {
        return txTestCluster.clientTxManager();
    }

    /** {@inheritDoc} */
    protected TxManager txManager(TableViewInternal t) {
        String leaseHolder = primaryNode(t);

        assertNotNull(leaseHolder, "Table primary node should not be null");

        TxManager manager = txTestCluster.txManagers().get(leaseHolder);

        assertNotNull(manager);

        return manager;
    }

    protected @Nullable String primaryNode(TableViewInternal t) {
        CompletableFuture<ReplicaMeta> primaryReplicaFuture = txTestCluster.placementDriver().getPrimaryReplica(
                replicationGroupId(t, 0),
                txTestCluster.clocks().get(txTestCluster.localNodeName()).now());

        assertThat(primaryReplicaFuture, willCompleteSuccessfully());

        return primaryReplicaFuture.join().getLeaseholder();
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

            var groupId = replicationGroupId(table, partId);

            Peer serverPeer = server.localPeers(groupId).get(0);

            RaftGroupService grp = server.raftGroupService(new RaftNodeId(groupId, serverPeer));

            var fsm = (JraftServerImpl.DelegatingStateMachine) grp.getRaftNode().getOptions().getFsm();

            TablePartitionProcessor tableProcessor = fsm.getListeners().stream()
                    .filter(ZonePartitionRaftListener.class::isInstance)
                    .map(ZonePartitionRaftListener.class::cast)
                    .map(listener -> (TablePartitionProcessor) listener.tableProcessor(table.tableId()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("ZonePartitionRaftListener not found"));

            MvPartitionStorage storage = tableProcessor.getMvStorage();

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
        Mockito.doReturn(CompletableFuture.failedFuture(new Exception())).when(replicaService).invoke((InternalClusterNode) any(), any());
    }

    protected Collection<TxManager> txManagers() {
        return txTestCluster.txManagers().values();
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
    protected Tuple makeValue(long id, String name) {
        return Tuple.create().set("accountNumber", id).set("name", name);
    }

    /**
     * Makes a value.
     *
     * @param balance The balance.
     * @return The value tuple.
     */
    protected Tuple makeValue(double balance) {
        return Tuple.create().set("balance", balance);
    }

    /**
     * Makes a value.
     *
     * @param name The name.
     * @return The value tuple.
     */
    protected Tuple makeValue(String name) {
        return Tuple.create().set("name", name);
    }

    protected static ZonePartitionId replicationGroupId(TableViewInternal tableViewInternal, int partitionIndex) {
        return new ZonePartitionId(tableViewInternal.internalTable().zoneId(), partitionIndex);
    }
}
