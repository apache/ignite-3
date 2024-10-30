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
import static org.apache.ignite.internal.metastorage.TestMetasStorageUtils.allNodesContainsSingleRevisionForKeyLocally;
import static org.apache.ignite.internal.metastorage.TestMetasStorageUtils.createClusterConfigWithCompactionProperties;
import static org.apache.ignite.internal.metastorage.TestMetasStorageUtils.latestKeyRevision;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Integration test for {@link MetaStorageCompactionTrigger}. */
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
        builder.clusterConfiguration(createClusterConfigWithCompactionProperties("10", "10"));
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
                waitForCondition(() -> allNodesContainsSingleRevisionForKeyLocally(CLUSTER, FOO_KEY, latestFooEntryRevision), 10, 1_000)
        );
    }

    private static IgniteImpl aliveNode() {
        return unwrapIgniteImpl(CLUSTER.aliveNode());
    }

    private static void watchExact(MetaStorageManager metaStorageManager, ByteArray key, CountDownLatch latch) {
        metaStorageManager.registerExactWatch(key, new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent event) {
                latch.countDown();

                return nullCompletedFuture();
            }

            @Override
            public void onError(Throwable e) {
            }
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
