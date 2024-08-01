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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap.Entry;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
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
                    .propagateTimeToReplicas(expectedTime.longValue());

            assertThat(fut, willCompleteSuccessfully());

            ensureTimestampStoredInAllReplicas(expectedTime, 2);
        }

        // Latest active catalog does not contain all required tables.
        // Replicas of dropped tables must also be updated.
        long requiredTime = CLUSTER.aliveNode().clockService().nowLong();

        {
            sql("drop table a");
            sql("drop table b");

            HybridTimestamp expectedTime = HybridTimestamp.hybridTimestamp(requiredTime);

            CompletableFuture<Void> fut = ignite.catalogCompactionRunner()
                    .propagateTimeToReplicas(expectedTime.longValue());

            assertThat(fut, willCompleteSuccessfully());

            ensureTimestampStoredInAllReplicas(expectedTime, 2);
        }

        // Update to lower timestamp should not succeed.
        {
            HybridTimestamp expectedTime = HybridTimestamp.hybridTimestamp(requiredTime - 1);

            CompletableFuture<Void> fut = ignite.catalogCompactionRunner()
                    .propagateTimeToReplicas(expectedTime.longValue());

            assertThat(fut, willCompleteSuccessfully());

            ensureTimestampStoredInAllReplicas(HybridTimestamp.hybridTimestamp(requiredTime), 2);
        }
    }

    @Test
    void testGlobalMinimumTxBeginTime() {
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
            TimeHolder timeHolder = await(compactor.determineGlobalMinimumRequiredTime(topologyNodes, 0L));
            assertThat(timeHolder.minActiveTxBeginTime, is(tx1.startTimestamp().longValue()));
        });

        tx1.rollback();

        compactors.forEach(compactor -> {
            TimeHolder timeHolder = await(compactor.determineGlobalMinimumRequiredTime(topologyNodes, 0L));
            assertThat(timeHolder.minActiveTxBeginTime, is(tx2.startTimestamp().longValue()));
        });

        tx2.commit();

        compactors.forEach(compactor -> {
            TimeHolder timeHolder = await(compactor.determineGlobalMinimumRequiredTime(topologyNodes, 0L));
            assertThat(timeHolder.minActiveTxBeginTime, is(tx3.startTimestamp().longValue()));
        });

        tx3.rollback();

        // Since there are no active RW transactions in the cluster, the minimum time will be min(now()) across all nodes.
        compactors.forEach(compactor -> {
            long minTime = Stream.of(node0, node1, node2).map(node -> node.clockService().nowLong()).min(Long::compareTo).orElseThrow();

            TimeHolder timeHolder = await(compactor.determineGlobalMinimumRequiredTime(topologyNodes, 0L));

            long maxTime = Stream.of(node0, node1, node2).map(node -> node.clockService().nowLong()).min(Long::compareTo).orElseThrow();

            // Read-only transactions are not counted,
            assertThat(timeHolder.minActiveTxBeginTime, greaterThan(readonlyTx.startTimestamp().longValue()));

            assertThat(timeHolder.minActiveTxBeginTime, greaterThanOrEqualTo(minTime));
            assertThat(timeHolder.minActiveTxBeginTime, lessThanOrEqualTo(maxTime));
        });

        readonlyTx.rollback();
    }

    private static void ensureTimestampStoredInAllReplicas(HybridTimestamp expTime, int expTablesCount) throws InterruptedException {
        Int2IntMap tablesWithPartitions = catalogManagerFacade().collectTablesWithPartitionsBetween(
                expTime.longValue(),
                CLUSTER.aliveNode().clockService().nowLong()
        );

        Loza loza = CLUSTER.aliveNode().raftManager();
        JraftServerImpl server = (JraftServerImpl) loza.server();

        assertThat(tablesWithPartitions.keySet(), hasSize(expTablesCount));

        for (Entry e : tablesWithPartitions.int2IntEntrySet()) {
            for (int p = 0; p < e.getIntValue(); p++) {
                TablePartitionId groupId = new TablePartitionId(e.getIntKey(), p);
                List<Peer> peers = server.localPeers(groupId);

                assertThat(peers, is(not(empty())));

                Peer serverPeer = server.localPeers(groupId).get(0);
                RaftGroupService grp = server.raftGroupService(new RaftNodeId(groupId, serverPeer));
                DelegatingStateMachine fsm = (DelegatingStateMachine) grp.getRaftNode().getOptions().getFsm();
                PartitionListener listener = (PartitionListener) fsm.getListener();

                // When a future completes from `Invoke`, it is guaranteed that the leader will be updated,
                // the remaining replicas can be updated later.
                IgniteTestUtils.waitForCondition(
                        () -> Long.valueOf(expTime.longValue()).equals(listener.minimumActiveTxStartTime()),
                        5_000
                );

                assertThat(grp.getGroupId(), listener.minimumActiveTxStartTime(), equalTo(expTime.longValue()));
            }
        }
    }

    private static CatalogManagerCompactionFacade catalogManagerFacade() {
        return new CatalogManagerCompactionFacade((CatalogManagerImpl) CLUSTER.aliveNode().catalogManager());
    }
}
