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

package org.apache.ignite.internal.catalog.compaction;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap.Entry;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.compaction.CatalogCompactionRunner.TimeHolder;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl.DelegatingStateMachine;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.Test;

/**
 * Integration tests to verify catalog compaction.
 */
class ItCatalogCompactionTest extends ClusterPerClassIntegrationTest {
    private static final int CLUSTER_SIZE = 3;

    @Override
    protected int initialNodes() {
        return CLUSTER_SIZE;
    }

    @Override
    protected int[] cmgMetastoreNodes() {
        return IntStream.range(0, CLUSTER_SIZE).toArray();
    }

    @Test
    void testRaftGroupsUpdate() throws InterruptedException {
        IgniteImpl ignite = CLUSTER.aliveNode();
        CatalogManagerImpl catalogManager = ((CatalogManagerImpl) CLUSTER.aliveNode().catalogManager());

        sql(format("create zone if not exists test with partitions=16, replicas={}, storage_profiles='default'",
                initialNodes()));
        sql("alter zone test set default");

        // Latest active catalog contains all required tables.
        {
            sql("create table a(a int primary key)");

            Catalog minRequiredCatalog = catalogManager.catalog(catalogManager.latestCatalogVersion());
            assertNotNull(minRequiredCatalog);

            sql("create table b(a int primary key)");

            HybridTimestamp expectedTime = HybridTimestamp.hybridTimestamp(minRequiredCatalog.time());

            CompletableFuture<Void> fut = ignite.catalogCompactionRunner()
                    .propagateMinimalRequiredTimeToReplicas(expectedTime);

            assertThat(fut, willCompleteSuccessfully());

            ensureTimestampStoredAllReplicas(expectedTime, 2);
        }

        // Latest active catalog contains all required tables.
        long requiredTime = CLUSTER.aliveNode().clockService().nowLong();
        {
            sql("drop table a");;
            sql("drop table b");;

            HybridTimestamp expectedTime = HybridTimestamp.hybridTimestamp(requiredTime);

            CompletableFuture<Void> fut = ignite.catalogCompactionRunner()
                    .propagateMinimalRequiredTimeToReplicas(expectedTime);

            assertThat(fut, willCompleteSuccessfully());

            ensureTimestampStoredAllReplicas(expectedTime, 2);
        }

        // Update to lower timestamp should not succeed.
        {
            HybridTimestamp expectedTime = HybridTimestamp.hybridTimestamp(requiredTime - 1);

            CompletableFuture<Void> fut = ignite.catalogCompactionRunner()
                    .propagateMinimalRequiredTimeToReplicas(expectedTime);

            assertThat(fut, willCompleteSuccessfully());

            ensureTimestampStoredAllReplicas(HybridTimestamp.hybridTimestamp(requiredTime), 2);
        }
    }

    @Test
    void testGlobalMinimalTxBeginTime() {
        IgniteImpl node0 = CLUSTER.node(0);
        IgniteImpl node1 = CLUSTER.node(1);
        IgniteImpl node2 = CLUSTER.node(2);

        List<CatalogCompactionRunner> compactors = List.of(
                node0.catalogCompactionRunner(),
                node1.catalogCompactionRunner(),
                node2.catalogCompactionRunner()
        );

        Collection<ClusterNode> topologyNodes = node0.clusterNodes();

        InternalTransaction tx1 = (InternalTransaction) node0.transactions().begin();
        InternalTransaction tx2 = (InternalTransaction) node1.transactions().begin();
        InternalTransaction readonlyTx = (InternalTransaction) node1.transactions().begin(new TransactionOptions().readOnly(true));
        InternalTransaction tx3 = (InternalTransaction) node2.transactions().begin();

        compactors.forEach(compactor -> {
            TimeHolder timeHolder = await(compactor.determineGlobalMinimalRequiredTime(topologyNodes, 0L));
            assertThat(timeHolder.minActiveTxTime, equalTo(tx1.startTimestamp()));
        });

        tx1.rollback();

        compactors.forEach(compactor -> {
            TimeHolder timeHolder = await(compactor.determineGlobalMinimalRequiredTime(topologyNodes, 0L));
            assertThat(timeHolder.minActiveTxTime, equalTo(tx2.startTimestamp()));
        });

        tx2.commit();

        compactors.forEach(compactor -> {
            TimeHolder timeHolder = await(compactor.determineGlobalMinimalRequiredTime(topologyNodes, 0L));
            assertThat(timeHolder.minActiveTxTime, equalTo(tx3.startTimestamp()));
        });

        tx3.rollback();

        compactors.forEach(compactor -> {
            TimeHolder timeHolder = await(compactor.determineGlobalMinimalRequiredTime(topologyNodes, 0L));
            assertThat(timeHolder.minActiveTxTime, is(nullValue()));
        });

        readonlyTx.rollback();
    }

    private void ensureTimestampStoredAllReplicas(HybridTimestamp expectedTs, int expectedTablesCount) throws InterruptedException {
        Int2IntMap tablesWithPartitions = catalogManagerHelper().collectTablesWithPartitionsBetween(
                expectedTs.longValue(),
                CLUSTER.aliveNode().clockService().nowLong()
        );

        Loza loza = CLUSTER.aliveNode().raftManager();
        JraftServerImpl server = (JraftServerImpl) loza.server();

        assertThat(tablesWithPartitions.keySet(), hasSize(expectedTablesCount));

        for (Entry e : tablesWithPartitions.int2IntEntrySet()) {
            for (int p = 0; p < e.getIntValue(); p++) {
                TablePartitionId groupId = new TablePartitionId(e.getIntKey(), p);
                List<Peer> peers = server.localPeers(groupId);

                assertThat(peers, is(not(empty())));

                Peer serverPeer = server.localPeers(groupId).get(0);
                RaftGroupService grp = server.raftGroupService(new RaftNodeId(groupId, serverPeer));
                DelegatingStateMachine fsm = (DelegatingStateMachine) grp.getRaftNode().getOptions().getFsm();
                PartitionListener listener = (PartitionListener) fsm.getListener();

                IgniteTestUtils.waitForCondition(() -> Long.valueOf(expectedTs.longValue()).equals(listener.minimalActiveTxTime()), 5_000);
                assertThat(grp.getGroupId(), listener.minimalActiveTxTime(), equalTo(expectedTs.longValue()));
            }
        }
    }

    private CatalogManagerCompactionHelper catalogManagerHelper() {
        return new CatalogManagerCompactionHelper((CatalogManagerImpl) CLUSTER.aliveNode().catalogManager());
    }
}
