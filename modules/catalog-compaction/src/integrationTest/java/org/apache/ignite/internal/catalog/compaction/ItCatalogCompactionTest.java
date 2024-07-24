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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
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
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

/**
 * TODO Describe it.
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

        sql(format("create zone if not exists test with partitions=16, replicas={}, storage_profiles='default'",
                initialNodes()));
        sql("alter zone test set default");

        sql("create table a(a int primary key)");

        CatalogManagerImpl catalogManager = ((CatalogManagerImpl) CLUSTER.aliveNode().catalogManager());

        Catalog catalog = catalogManager.catalog(catalogManager.latestCatalogVersion());
        assertNotNull(catalog);

        sql("create table b(a int primary key)");
        sql("drop table b");
        sql("drop table a");

        {
            HybridTimestamp expectedTime = HybridTimestamp.hybridTimestamp(catalog.time());

            CompletableFuture<Void> fut = ignite.catalogCompactionRunner()
                    .updateAllReplicasWithMinimalTime(expectedTime);

            assertThat(fut, willCompleteSuccessfully());

            verifyMinimalActiveTxTime(catalog, expectedTime);
        }

//        {
//            HybridTimestamp expectedTime = HybridTimestamp.MAX_VALUE;
//
//            CompletableFuture<Void> fut = ignite.catalogCompactionRunner()
//                    .updateAllReplicasWithMinimalTime(expectedTime);
//
//            assertThat(fut, willCompleteSuccessfully());
//
//            verifyMinimalActiveTxTime(catalog, 2, expectedTime);
//        }
    }

    @Test
    void testGlobalMinimalTxBeginTime() throws Exception {
        IgniteImpl node0 = CLUSTER.node(0);
        IgniteImpl node1 = CLUSTER.node(1);
        IgniteImpl node2 = CLUSTER.node(2);

        InternalTransaction tx0min = (InternalTransaction) node0.transactions().begin();
        InternalTransaction tx0max = (InternalTransaction) node0.transactions().begin();
        Transaction tx1 = node1.transactions().begin();
        Transaction tx2 = node2.transactions().begin();

        {
            HybridTimestamp time = node2.catalogCompactionRunner().determineGlobalMinimumTxStartTime(node0.clusterNodes()).get();
            assertThat(time, equalTo(tx0min.startTimestamp()));
        }

        tx0min.rollback();

        {
            HybridTimestamp time = node2.catalogCompactionRunner().determineGlobalMinimumTxStartTime(node0.clusterNodes()).get();
            assertThat(time, equalTo(tx0max.startTimestamp()));
        }
    }

    private static void verifyMinimalActiveTxTime(
            Catalog catalog,
            HybridTimestamp expectedTs
    ) throws InterruptedException {
        Loza loza = CLUSTER.aliveNode().raftManager();
        JraftServerImpl server = (JraftServerImpl) loza.server();
        Collection<CatalogTableDescriptor> tables = catalog.tables();

        for (CatalogTableDescriptor table : tables) {
            CatalogZoneDescriptor zone = catalog.zone(table.zoneId());

            assertNotNull(zone);

            for (int p = 0; p < zone.partitions(); p++) {
                TablePartitionId groupId = new TablePartitionId(table.id(), p);
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
}
