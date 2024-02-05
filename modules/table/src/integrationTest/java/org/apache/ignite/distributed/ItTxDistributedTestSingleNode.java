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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
import org.apache.ignite.internal.schema.configuration.StorageUpdateConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.TxAbstractTest;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

/**
 * Distributed transaction test using a single partition table.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItTxDistributedTestSingleNode extends TxAbstractTest {
    protected static int ACC_TABLE_ID = 0;

    protected static int CUST_TABLE_ID = 1;

    protected static final String ACC_TABLE_NAME = "accounts";

    protected static final String CUST_TABLE_NAME = "customers";

    //TODO fsync can be turned on again after https://issues.apache.org/jira/browse/IGNITE-20195
    @InjectConfiguration("mock: { fsync: false }")
    protected RaftConfiguration raftConfiguration;

    @InjectConfiguration
    protected TransactionConfiguration txConfiguration;

    @InjectConfiguration
    protected StorageUpdateConfiguration storageUpdateConfiguration;

    /**
     * Returns a count of nodes.
     *
     * @return Nodes.
     */
    protected int nodes() {
        return 1;
    }

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

    protected final TestInfo testInfo;

    protected ItTxTestCluster txTestCluster;

    /**
     * The constructor.
     *
     * @param testInfo Test info.
     */
    public ItTxDistributedTestSingleNode(TestInfo testInfo) {
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
                timestampTracker
        );
        txTestCluster.prepareCluster();

        this.igniteTransactions = txTestCluster.igniteTransactions;

        accounts = txTestCluster.startTable(ACC_TABLE_NAME, ACC_TABLE_ID, ACCOUNTS_SCHEMA);
        customers = txTestCluster.startTable(CUST_TABLE_NAME, CUST_TABLE_ID, CUSTOMERS_SCHEMA);

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
    protected static ClusterService startNode(TestInfo testInfo, String name, int port,
            NodeFinder nodeFinder) {
        var network = ClusterServiceTestUtils.clusterService(testInfo, port, nodeFinder);

        network.start();

        return network;
    }

    /** {@inheritDoc} */
    @Override
    protected TxManager clientTxManager() {
        return txTestCluster.clientTxManager;
    }

    /** {@inheritDoc} */
    @Override
    protected TxManager txManager(TableViewInternal t) {
        CompletableFuture<ReplicaMeta> primaryReplicaFuture = txTestCluster.placementDriver.getPrimaryReplica(
                new TablePartitionId(t.tableId(), 0),
                txTestCluster.clocks.get(txTestCluster.localNodeName).now());

        assertThat(primaryReplicaFuture, willCompleteSuccessfully());

        TxManager manager = txTestCluster.txManagers.get(primaryReplicaFuture.join().getLeaseholder());

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
    @Override
    protected boolean assertPartitionsSame(TableViewInternal table, int partId) {
        long storageIdx = 0;

        for (Map.Entry<String, Loza> entry : txTestCluster.raftServers.entrySet()) {
            Loza svc = entry.getValue();

            var server = (JraftServerImpl) svc.server();

            var groupId = new TablePartitionId(table.tableId(), partId);

            Peer serverPeer = server.localPeers(groupId).get(0);

            org.apache.ignite.raft.jraft.RaftGroupService grp = server.raftGroupService(new RaftNodeId(groupId, serverPeer));

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

    @Override
    protected void injectFailureOnNextOperation(TableViewInternal accounts) {
        InternalTable internalTable = accounts.internalTable();
        ReplicaService replicaService = IgniteTestUtils.getFieldValue(internalTable, "replicaSvc");
        Mockito.doReturn(CompletableFuture.failedFuture(new Exception())).when(replicaService).invoke((String) any(), any());
        Mockito.doReturn(CompletableFuture.failedFuture(new Exception())).when(replicaService).invoke((ClusterNode) any(), any());
    }

    @Override
    protected Collection<TxManager> txManagers() {
        return txTestCluster.txManagers.values();
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
}
