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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl.DelegatingStateMachine;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.raft.jraft.RaftGroupService;
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

            verifyMinimalActiveTxTime(expectedTime, 2);
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

            verifyMinimalActiveTxTime(expectedTime, 2);
        }

        // Update to lower timestamp should not succeed.
        {
            HybridTimestamp expectedTime = HybridTimestamp.hybridTimestamp(requiredTime - 1);

            CompletableFuture<Void> fut = ignite.catalogCompactionRunner()
                    .propagateMinimalRequiredTimeToReplicas(expectedTime);

            assertThat(fut, willCompleteSuccessfully());

            verifyMinimalActiveTxTime(HybridTimestamp.hybridTimestamp(requiredTime), 2);
        }
    }

    private void verifyMinimalActiveTxTime(HybridTimestamp expectedTs, int expectedTablesCount) throws InterruptedException {
        Map<Integer, Integer> tablesWithPartitions = catalogManagerHelper().findAllTablesSince(expectedTs.longValue());
        Loza loza = CLUSTER.aliveNode().raftManager();
        JraftServerImpl server = (JraftServerImpl) loza.server();

        assertThat(tablesWithPartitions.keySet(), hasSize(expectedTablesCount));

        for (Map.Entry<Integer, Integer> e : tablesWithPartitions.entrySet()) {
            for (int p = 0; p < e.getValue(); p++) {
                TablePartitionId groupId = new TablePartitionId(e.getKey(), p);

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

    private CatalogManagerHelper catalogManagerHelper() {
        return new CatalogManagerHelper((CatalogManagerImpl) CLUSTER.aliveNode().catalogManager(), CLUSTER.aliveNode().clockService());
    }
}
