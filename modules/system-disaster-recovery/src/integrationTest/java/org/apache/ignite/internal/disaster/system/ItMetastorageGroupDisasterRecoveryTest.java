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

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.disaster.system.SystemDisasterRecoveryClient.initiateClusterReset;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.impl.MetastorageDivergedException;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.metastorage.server.time.ClusterTime;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.util.IgniteUtils;
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

        initiateMgRepairVia(0, 1, 0);

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

    private void initiateMgRepairVia(int conductorIndex, int mgReplicationFactor, int... newCmgIndexes) {
        initiateClusterReset("localhost", cluster.httpPort(conductorIndex), mgReplicationFactor, nodeNames(newCmgIndexes));
    }

    @Test
    void afterRepairingWithReplicationFactor1LeaderPerformsSecondaryDuties() throws Exception {
        startAndInitCluster(2, new int[]{0}, new int[]{1});
        waitTillClusterStateIsSavedToVaultOnConductor(0);

        // This makes the MG majority go away.
        cluster.stopNode(1);

        initiateMgRepairVia(0, 1, 0);

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

        initiateMgRepairVia(2, 3, 0, 1, 2);

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

        initiateMgRepairVia(0, 1, 0);

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

        await("Expected Metastorage SafeTime to advance")
                .untilAsserted(() -> {
                    assertThat(clusterTime.currentSafeTime().longValue(), greaterThan(started.longValue()));
                });
    }

    @Test
    void repairIsPossibleWhenAllNodesWaitForMgMajorityOnJoin() throws Exception {
        // Node with index 2 will host neither of voting sets.
        startAndInitCluster(3, new int[]{0}, new int[]{1});
        waitTillClusterStateIsSavedToVaultOnConductor(0);

        cluster.stopNode(1);

        IntStream.of(0, 2).parallel().forEach(this::restartPartially);

        initiateMgRepairVia(0, 1, 0);

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

        initiateMgRepairVia(0, 1, 0);

        IgniteImpl restartedIgniteImpl0 = waitTillNodeRestartsInternally(0);
        waitTillMgHasMajority(restartedIgniteImpl0);

        IgniteImpl restartedIgniteImpl2 = waitTillNodeRestartsInternally(2);
        waitTillMgHasMajority(restartedIgniteImpl2);
    }

    @Test
    void oldMgLeaderDoesNotHijackLeadership() throws Exception {
        startAndInitCluster(2, new int[]{0}, new int[]{1});
        waitTillClusterStateIsSavedToVaultOnConductor(0);

        // Copy Metastorage state from old leader (1) to future leader (0) to make sure that 1 is not ahead of 0 and there will be
        // no Metastorage divergence when we make 0 new leader and migrate 1 to cluster again.
        // This stops both nodes.
        makeSure2NodesHaveSameMetastorageState(1, 0);

        cluster.startEmbeddedNode(0);
        initiateMgRepairVia(0, 1, 0);

        IgniteImpl restartedIgniteImpl0 = waitTillNodeRestartsInternally(0);
        waitTillMgHasMajority(restartedIgniteImpl0);

        // Make sure the new leader will not tell the old leader about new configuration (to give the old leader timing for a hijack).
        ((MetaStorageManagerImpl) restartedIgniteImpl0.metaStorageManager()).disableLearnersAddition();

        initiateMigration(1, 0);
        CompletableFuture<Void> ignite1RestartFuture = waitForRestartFuture(1);

        // It should not be able to start: it should abstain from becoming a leader and node 1 (the new leader) does not add it as
        // a learner.
        assertThat(ignite1RestartFuture, willTimeoutIn(5, SECONDS));

        // Make sure the new leader is still a leader.
        RaftGroupService mgClient0 = metastorageGroupClient(restartedIgniteImpl0);
        assertThat(leaderName(mgClient0), is(cluster.nodeName(0)));
    }

    private static RaftGroupService metastorageGroupClient(IgniteImpl ignite)
            throws NodeStoppingException {
        PeersAndLearners config = PeersAndLearners.fromConsistentIds(Set.of(ignite.name()));
        return ignite.raftManager().startRaftGroupService(MetastorageGroupId.INSTANCE, config, true);
    }

    private static String leaderName(RaftGroupService mgClient0) {
        CompletableFuture<LeaderWithTerm> future = mgClient0.refreshAndGetLeaderWithTerm();
        assertThat(future, willSucceedIn(10, SECONDS));

        Peer leader = future.join().leader();
        assertThat(leader, is(notNullValue()));

        return leader.consistentId();
    }

    @Test
    void migratesNodesThatSawNoReparationToNewClusterIfMetastorageDidNotDiverge() throws Exception {
        startAndInitCluster(2, new int[]{0}, new int[]{1});
        waitTillClusterStateIsSavedToVaultOnConductor(0);

        // Copy Metastorage state from old leader (1) to future leader (0) to make sure that 1 is not ahead of 0 and there will be
        // no Metastorage divergence when we make 0 new leader and migrate 1 to cluster again.
        // This stops both nodes.
        makeSure2NodesHaveSameMetastorageState(1, 0);

        // Repair MG with just node 0 in CMG.
        cluster.startEmbeddedNode(0);
        initiateMgRepairVia(0, 1, 0);
        IgniteImpl restartedIgniteImpl0 = waitTillNodeRestartsInternally(0);
        waitTillMgHasMajority(restartedIgniteImpl0);

        // Starting the node that did not see the repair.
        migrate(1, 0);

        LogicalTopologySnapshot topologySnapshot = restartedIgniteImpl0.logicalTopologyService().logicalTopologyOnLeader().get(10, SECONDS);
        assertTopologyContainsNode(1, topologySnapshot);
    }

    private void makeSure2NodesHaveSameMetastorageState(int leaderIndex, int followerIndex) throws IOException {
        ComponentWorkingDir followerMsWorkDir = igniteImpl(followerIndex).metastorageWorkDir();
        ComponentWorkingDir leaderMsWorkDir = igniteImpl(leaderIndex).metastorageWorkDir();

        IntStream.of(followerIndex, leaderIndex).parallel().forEach(cluster::stopNode);

        copyMetastorageState(leaderMsWorkDir, followerMsWorkDir);
    }

    private static void copyMetastorageState(ComponentWorkingDir source, ComponentWorkingDir dest) throws IOException {
        replaceDir(source.dbPath(), dest.dbPath());
        replaceDir(source.raftLogPath(), dest.raftLogPath());

        String pathToSnapshots = "metastorage_group-0/snapshot";
        replaceDir(source.metaPath().resolve(pathToSnapshots), dest.metaPath().resolve(pathToSnapshots));
    }

    private static void replaceDir(Path sourceDir, Path destDir) throws IOException {
        assertTrue(Files.isDirectory(sourceDir));
        assertTrue(Files.isDirectory(destDir));

        assertTrue(IgniteUtils.deleteIfExists(destDir));
        Files.createDirectory(destDir);
        copyDir(sourceDir, destDir);
    }

    private static void copyDir(Path src, Path dest) throws IOException {
        try (Stream<Path> stream = Files.walk(src)) {
            stream.forEach(source -> copyFile(source, dest.resolve(src.relativize(source))));
        }
    }

    private static void copyFile(Path source, Path dest) {
        try {
            Files.copy(source, dest, REPLACE_EXISTING);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    void detectsMetastorageDivergence() throws Exception {
        startAndInitCluster(2, new int[]{0}, new int[]{1});
        waitTillClusterStateIsSavedToVaultOnConductor(0);

        // Stopping node 0 to make sure it does not see the subsequent Metastorage write accepted by the leader (1).
        // Later, we'll stop node 1, repair MG on 0, try to migrate 1 to the cluster, and, as 1 has a Metastorage put which 0 does not
        // have, Metastorage divergence will have to be detected.
        cluster.stopNode(0);

        igniteImpl(1).metaStorageManager().put(new ByteArray("test-key"), new byte[]{42});

        // This makes the MG majority go away.
        cluster.stopNode(1);

        cluster.startEmbeddedNode(0);
        initiateMgRepairVia(0, 1, 0);
        IgniteImpl restartedIgniteImpl0 = waitTillNodeRestartsInternally(0);
        waitTillMgHasMajority(restartedIgniteImpl0);

        // Starting the node that did not see the repair.
        initiateMigration(1, 0);

        assertThat(waitForRestartFuture(1), willThrowWithCauseOrSuppressed(MetastorageDivergedException.class, "Metastorage has diverged"));

        // Attempt to migrate should fail.
        assertThrowsWithCause(() -> cluster.server(1).api(), MetastorageDivergedException.class, "Metastorage has diverged");

        // Subsequent restart should also fail.
        assertThrowsWithCause(() -> cluster.restartNode(1), MetastorageDivergedException.class, "Metastorage has diverged");
    }

    @Test
    void repairIsProhibitedWhenMgMajorityIsOnline() throws Exception {
        startAndInitCluster(3, new int[]{0}, new int[]{0, 1, 2});
        waitTillClusterStateIsSavedToVaultOnConductor(0);

        // After this, MG majority will still be online.
        cluster.stopNode(2);

        IgniteImpl node0BeforeRestart = igniteImpl(0);

        waitTillMgHasMajority(node0BeforeRestart);

        Exception ex = assertThrows(Exception.class, () -> initiateMgRepairVia(0, 2, 0));
        assertThat(ex.getMessage(), containsString("Majority repair is rejected because majority of Metastorage nodes are online"));
    }
}
