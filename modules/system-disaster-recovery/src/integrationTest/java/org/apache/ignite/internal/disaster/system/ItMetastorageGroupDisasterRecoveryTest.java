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

package org.apache.ignite.internal.disaster.system;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.app.IgniteServerImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.metastorage.server.time.ClusterTime;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.junit.jupiter.api.Test;

class ItMetastorageGroupDisasterRecoveryTest extends ItSystemGroupDisasterRecoveryTest {
    @Test
    void repairWhenMgWas1Node() throws Exception {
        // Node with index 2 will host neither of voting sets.
        startAndInitCluster(3, new int[]{0}, new int[]{1});
        waitTillClusterStateIsSavedToVaultOnConductor(0);

        // This makes the MG majority go away.
        cluster.stopNode(1);

        IgniteImpl node0BeforeRestart = igniteImpl(0);

        assertThatMgHasNoMajority(node0BeforeRestart);

        initiateMgRepairVia(node0BeforeRestart, 1, 0);

        IgniteImpl restartedNode0 = waitTillNodeRestartsInternally(0);
        waitTillMgHasMajority(restartedNode0);

        IgniteImpl restartedNode2 = waitTillNodeRestartsInternally(2);
        waitTillMgHasMajority(restartedNode2);

        assertResetClusterMessageIsNotPresentAt(restartedNode0);
        assertResetClusterMessageIsNotPresentAt(restartedNode2);
    }

    private static void assertThatMgHasNoMajority(IgniteImpl ignite) {
        assertThat(ignite.metaStorageManager().get(new ByteArray("abc")), willTimeoutIn(1, SECONDS));
    }

    private static void waitTillMgHasMajority(IgniteImpl ignite) {
        assertThat(ignite.metaStorageManager().get(new ByteArray("abc")), willCompleteSuccessfully());
    }

    private void initiateMgRepairVia(IgniteImpl conductor, int mgReplicationFactor, int... newCmgIndexes) {
        // TODO: IGNITE-22897 - initiate repair via CLI.

        CompletableFuture<Void> initiationFuture = conductor.systemDisasterRecoveryManager()
                .resetClusterRepairingMetastorage(List.of(nodeNames(newCmgIndexes)), mgReplicationFactor);
        assertThat(initiationFuture, willCompleteSuccessfully());
    }

    @Test
    void afterRepairingWithReplicationFactor1LeaderPerformsSecondaryDuties() throws Exception {
        startAndInitCluster(2, new int[]{0}, new int[]{1});
        waitTillClusterStateIsSavedToVaultOnConductor(0);

        // This makes the MG majority go away.
        cluster.stopNode(1);

        IgniteImpl igniteImpl0BeforeRestart = igniteImpl(0);

        initiateMgRepairVia(igniteImpl0BeforeRestart, 1, 0);

        IgniteImpl restartedIgniteImpl0 = waitTillNodeRestartsInternally(0);
        waitTillMgHasMajority(restartedIgniteImpl0);

        // This makes sure the new leader manages Metastorage learners.
        assertDoesNotThrow(() -> cluster.startNode(2));

        assertIdleSafeTimeGetsPropagated(restartedIgniteImpl0);
    }

    @Test
    void repairWhenMgWas3Nodes() throws Exception {
        // Node with index 5 will host neither of voting sets.
        startAndInitCluster(6, new int[]{0, 1, 2}, new int[]{2, 3, 4});
        waitTillClusterStateIsSavedToVaultOnConductor(2);

        // Stop the majority of MG.
        IntStream.of(3, 4).parallel().forEach(cluster::stopNode);

        IgniteImpl igniteImpl2BeforeRestart = igniteImpl(2);

        assertThatMgHasNoMajority(igniteImpl2BeforeRestart);

        initiateMgRepairVia(igniteImpl2BeforeRestart, 3, 0, 1, 2);

        IgniteImpl restartedIgniteImpl2 = waitTillNodeRestartsInternally(2);
        waitTillMgHasMajority(restartedIgniteImpl2);

        waitTillNodeRestartsInternally(0);
        waitTillNodeRestartsInternally(1);
        waitTillNodeRestartsInternally(5);

        for (int n : new int[]{0, 1, 2, 5}) {
            assertIdleSafeTimeGetsPropagated(unwrapIgniteImpl(cluster.server(n).api()));
        }
    }

    @Test
    void repairedClusterCanJoinBlankNodes() throws Exception {
        startAndInitCluster(2, new int[]{0}, new int[]{1});
        waitTillClusterStateIsSavedToVaultOnConductor(0);

        cluster.stopNode(1);

        initiateMgRepairVia(igniteImpl(0), 1, 0);

        // Doing this wait to make sure that blank node will be able to connect at least someone. If we don't do this, the new node
        // will still be able to connect, but this will happen on Scalecube's initial sync retry, and we don't want to wait for it
        // in our test.
        waitTillNodeRestartsInternally(0);

        cluster.startNode(2);

        assertIdleSafeTimeGetsPropagated(igniteImpl(2));
    }

    private static void assertIdleSafeTimeGetsPropagated(IgniteImpl ignite) throws InterruptedException {
        ClusterTime clusterTime = ignite.metaStorageManager().clusterTime();
        HybridTimestamp started = clusterTime.currentSafeTime();

        assertTrue(
                waitForCondition(() -> clusterTime.currentSafeTime().longValue() > started.longValue(), SECONDS.toMillis(10)),
                () -> "Did not see " + ignite.name() + " to receive Metastorage SafeTime updates; current SafeTime is "
                        + clusterTime.currentSafeTime()
        );
    }

    @Test
    void repairIsPossibleWhenAllNodesWaitForMgMajorityOnJoin() throws Exception {
        // Node with index 2 will host neither of voting sets.
        startAndInitCluster(3, new int[]{0}, new int[]{1});
        waitTillClusterStateIsSavedToVaultOnConductor(0);

        cluster.stopNode(1);

        IntStream.of(0, 2).parallel().forEach(this::restartPartially);

        initiateMgRepairVia(((IgniteServerImpl) cluster.server(0)).igniteImpl(), 1, 0);

        IgniteImpl restartedIgniteImpl0 = waitTillNodeRestartsInternally(0);
        waitTillMgHasMajority(restartedIgniteImpl0);
    }

    @Test
    void repairsBothCmgAndMg() throws Exception {
        // Node with index 2 will host neither of voting sets.
        startAndInitCluster(3, new int[]{1}, new int[]{1});
        waitTillClusterStateIsSavedToVaultOnConductor(0);

        // This makes both CMG and MG majorities go away.
        cluster.stopNode(1);

        IgniteImpl igniteImpl0BeforeRestart = igniteImpl(0);

        initiateMgRepairVia(igniteImpl0BeforeRestart, 1, 0);

        IgniteImpl restartedIgniteImpl0 = waitTillNodeRestartsInternally(0);
        waitTillMgHasMajority(restartedIgniteImpl0);

        IgniteImpl restartedIgniteImpl2 = waitTillNodeRestartsInternally(2);
        waitTillMgHasMajority(restartedIgniteImpl2);
    }

    @Test
    void oldMgLeaderDoesNotHijackLeadership() throws Exception {
        startAndInitCluster(2, new int[]{0}, new int[]{1});
        waitTillClusterStateIsSavedToVaultOnConductor(0);

        // This makes the MG majority go away.
        cluster.stopNode(1);

        IgniteImpl igniteImpl0BeforeRestart = igniteImpl(0);

        assertThatMgHasNoMajority(igniteImpl0BeforeRestart);

        initiateMgRepairVia(igniteImpl0BeforeRestart, 1, 0);

        IgniteImpl restartedIgniteImpl0 = waitTillNodeRestartsInternally(0);
        waitTillMgHasMajority(restartedIgniteImpl0);

        // Make sure the new leader will not tell the old leader about new configuration (to give the old leader timing for a hijack).
        ((MetaStorageManagerImpl) restartedIgniteImpl0.metaStorageManager()).disableLearnersAddition();

        initiateMigration(1, 0);
        CompletableFuture<Void> ignite1RestartFuture = waitForRestartOrShutdownFuture(1);

        // It should not be able to start: it should abstain from becoming a leader and node 1 (the new leader) does not add it as
        // a learner.
        assertThat(ignite1RestartFuture, willTimeoutIn(5, SECONDS));

        // Make sure the new leader is still a leader.
        RaftGroupService mgClient0 = metastorageGroupClient(restartedIgniteImpl0);
        assertThat(leaderName(mgClient0), is(cluster.nodeName(0)));
    }

    private static RaftGroupService metastorageGroupClient(IgniteImpl ignite)
            throws NodeStoppingException, ExecutionException, InterruptedException, TimeoutException {
        PeersAndLearners config = PeersAndLearners.fromConsistentIds(Set.of(ignite.name()));
        CompletableFuture<RaftGroupService> future = ignite.raftManager().startRaftGroupService(MetastorageGroupId.INSTANCE, config);
        return future.get(10, SECONDS);
    }

    private static String leaderName(RaftGroupService mgClient0) {
        CompletableFuture<LeaderWithTerm> future = mgClient0.refreshAndGetLeaderWithTerm();
        assertThat(future, willSucceedIn(10, SECONDS));

        Peer leader = future.join().leader();
        assertThat(leader, is(notNullValue()));

        return leader.consistentId();
    }
}
