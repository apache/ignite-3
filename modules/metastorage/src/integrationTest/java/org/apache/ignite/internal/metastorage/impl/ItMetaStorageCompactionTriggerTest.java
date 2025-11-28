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

package org.apache.ignite.internal.metastorage.impl;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.metastorage.TestMetasStorageUtils.FOO_KEY;
import static org.apache.ignite.internal.metastorage.TestMetasStorageUtils.VALUE;
import static org.apache.ignite.internal.metastorage.TestMetasStorageUtils.allNodesContainSingleRevisionForKeyLocally;
import static org.apache.ignite.internal.metastorage.TestMetasStorageUtils.collectCompactionRevisionForAllNodesLocally;
import static org.apache.ignite.internal.metastorage.TestMetasStorageUtils.collectRevisionForKeyForAllNodesLocally;
import static org.apache.ignite.internal.metastorage.TestMetasStorageUtils.createClusterConfigWithCompactionProperties;
import static org.apache.ignite.internal.metastorage.TestMetasStorageUtils.latestKeyRevision;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.distributionzones.rebalance.DistributionZoneRebalanceEngineV2;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Integration test for {@link MetaStorageCompactionTrigger}. */
@WithSystemProperty(key = DistributionZoneRebalanceEngineV2.SKIP_REBALANCE_TRIGGERS_RECOVERY, value = "true")
public class ItMetaStorageCompactionTriggerTest extends ClusterPerClassIntegrationTest {
    @Override
    protected int initialNodes() {
        return 2;
    }

    @Override
    protected int[] cmgMetastoreNodes() {
        assertEquals(2, initialNodes());

        return new int[] {0, 1};
    }

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration(createClusterConfigWithCompactionProperties(10, 10));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMetastorageCompactionOccursOnAllNodes(boolean changeMetastorageLeader) throws Exception {
        MetaStorageManager metaStorageManager = aliveNode().metaStorageManager();

        var updateFooKeyLatch = new CountDownLatch(2);
        watchExact(metaStorageManager, FOO_KEY, updateFooKeyLatch);

        assertThat(metaStorageManager.put(FOO_KEY, VALUE), willCompleteSuccessfully());

        if (changeMetastorageLeader) {
            transferMetastorageLeadershipToAnotherNode();
        }

        assertThat(metaStorageManager.put(FOO_KEY, VALUE), willCompleteSuccessfully());

        assertTrue(updateFooKeyLatch.await(1, SECONDS));

        long latestFooEntryRevision = latestKeyRevision(metaStorageManager, FOO_KEY);

        assertTrue(
                waitForCondition(() -> allNodesContainSingleRevisionForKeyLocally(CLUSTER, FOO_KEY, latestFooEntryRevision), 10, 1_000),
                () -> String.format(
                        "Failed to compact revisions for key: ["
                                + "latestKeyRevision=%s, "
                                + "keyRevisionsByNodeName=%s, "
                                + "compactionRevisionByNodeName=%s"
                                + "]",
                        latestFooEntryRevision,
                        collectRevisionForKeyForAllNodesLocally(CLUSTER, FOO_KEY),
                        collectCompactionRevisionForAllNodesLocally(CLUSTER)
                )
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCompactionRevisionMonotonouslyGrowsWithoutDuplicates(boolean changeMetastorageLeader) {
        MetaStorageManagerImpl metaStorageManager = (MetaStorageManagerImpl) aliveNode().metaStorageManager();

        long currentCompactionRevisionLocally = metaStorageManager.getCompactionRevisionLocally();

        log.info("Current compaction revision before start: " + currentCompactionRevisionLocally);

        var stopCollectCompactionRevisionFuture = new CompletableFuture<Void>();
        var compactionRevisions = new ConcurrentLinkedQueue<Long>();

        metaStorageManager.storage().registerCompactionRevisionUpdateListener(compactionRevision -> {
            compactionRevisions.add(compactionRevision);

            if (compactionRevision >= currentCompactionRevisionLocally + 100) {
                stopCollectCompactionRevisionFuture.complete(null);
            }
        });

        CompletableFuture<Void> updateMetastorageForCompactionFuture = runAsync(() -> {
            while (!stopCollectCompactionRevisionFuture.isDone()) {
                assertThat(metaStorageManager.put(FOO_KEY, VALUE), willCompleteSuccessfully());
            }
        });

        CompletableFuture<Void> changeMetastorageLeaderFuture = nullCompletedFuture();

        if (changeMetastorageLeader) {
            changeMetastorageLeaderFuture = runAsync(() -> {
                while (!stopCollectCompactionRevisionFuture.isDone()) {
                    transferMetastorageLeadershipToAnotherNode();

                    Thread.sleep(50);
                }
            });
        }

        assertThat(stopCollectCompactionRevisionFuture, willCompleteSuccessfully());
        assertThat(updateMetastorageForCompactionFuture, willCompleteSuccessfully());
        assertThat(changeMetastorageLeaderFuture, willCompleteSuccessfully());

        // Let's check that there are no duplicates.
        List<Long> copyCompactionRevisions = List.copyOf(compactionRevisions);
        assertThat(copyCompactionRevisions, hasSize(greaterThan(0)));
        assertThat(Set.copyOf(copyCompactionRevisions), hasSize(copyCompactionRevisions.size()));
    }

    private static IgniteImpl aliveNode() {
        return unwrapIgniteImpl(CLUSTER.aliveNode());
    }

    private static void watchExact(MetaStorageManager metaStorageManager, ByteArray key, CountDownLatch latch) {
        metaStorageManager.registerExactWatch(key, event -> {
            latch.countDown();

            return nullCompletedFuture();
        });
    }

    private void transferMetastorageLeadershipToAnotherNode() throws Exception {
        RaftGroupService raftGroupService = CLUSTER.leaderServiceFor(MetastorageGroupId.INSTANCE);

        String leaderConsistentId = raftGroupService.getRaftNode().getLeaderId().getConsistentId();

        for (int i = 0; i < initialNodes(); i++) {
            if (!CLUSTER.node(i).name().equals(leaderConsistentId)) {
                CLUSTER.transferLeadershipTo(i, MetastorageGroupId.INSTANCE);

                return;
            }
        }

        fail("Failed to change metastorage leader: " + leaderConsistentId);
    }
}
