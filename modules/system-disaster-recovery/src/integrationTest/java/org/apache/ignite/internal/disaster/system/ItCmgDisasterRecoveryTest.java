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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.app.IgniteServerImpl;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

class ItCmgDisasterRecoveryTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 0;
    }

    @Test
    void repairWhenCmgWas1Node() throws Exception {
        cluster.startAndInit(3, paramsBuilder -> {
            paramsBuilder.cmgNodeNames(nodeNames(0));
            paramsBuilder.metaStorageNodeNames(nodeNames(1));
            // Node with index 2 will host neither of voting sets.
        });
        waitTillClusterStateIsSavedToVaultOnConductor(1);

        // This makes the CMG majority go away.
        cluster.stopNode(0);

        IgniteImpl igniteImpl1BeforeRestart = igniteImpl(1);

        assertThatCmgHasNoMajority(igniteImpl1BeforeRestart);

        initiateCmgRepairVia(igniteImpl1BeforeRestart, 1);

        IgniteImpl restartedIgniteImpl1 = waitTillNodeRestartsInternally(1);
        waitTillCmgHasMajority(restartedIgniteImpl1);

        assertResetClusterMessageIsNotPresentAt(restartedIgniteImpl1);
        assertResetClusterMessageIsNotPresentAt(waitTillNodeRestartsInternally(2));
    }

    private void waitTillClusterStateIsSavedToVaultOnConductor(int nodeIndex) throws InterruptedException {
        assertTrue(waitForCondition(
                () -> new SystemDisasterRecoveryStorage(igniteImpl(nodeIndex).vault()).readClusterState() != null,
                SECONDS.toMillis(10)
        ));
    }

    private static void assertThatCmgHasNoMajority(IgniteImpl igniteImpl1BeforeRestart) {
        assertThat(igniteImpl1BeforeRestart.logicalTopologyService().logicalTopologyOnLeader(), willTimeoutIn(1, SECONDS));
    }

    private static void waitTillCmgHasMajority(IgniteImpl restartedIgniteImpl1) {
        assertThat(restartedIgniteImpl1.logicalTopologyService().logicalTopologyOnLeader(), willCompleteSuccessfully());
    }

    private void initiateCmgRepairVia(IgniteImpl conductor, int... newCmgIndexes) {
        // TODO: IGNITE-22812 - initiate repair via CLI.

        CompletableFuture<Void> initiationFuture = conductor.systemDisasterRecoveryManager()
                .resetCluster(List.of(nodeNames(newCmgIndexes)));
        assertThat(initiationFuture, willCompleteSuccessfully());
    }

    private String[] nodeNames(int... nodeIndexes) {
        return IntStream.of(nodeIndexes)
                .mapToObj(cluster::nodeName)
                .toArray(String[]::new);
    }

    private IgniteImpl waitTillNodeRestartsInternally(int nodeIndex) throws InterruptedException {
        // restartOrShutdownFuture() becomes non-null when restart or shutdown is initiated; we know it's restart.

        assertTrue(waitForCondition(() -> restartOrShutdownFuture(nodeIndex) != null, SECONDS.toMillis(20)));
        assertThat(restartOrShutdownFuture(nodeIndex), willCompleteSuccessfully());

        return unwrapIgniteImpl(cluster.server(nodeIndex).api());
    }

    @Nullable
    private CompletableFuture<Void> restartOrShutdownFuture(int nodeIndex) {
        return ((IgniteServerImpl) cluster.server(nodeIndex)).restartOrShutdownFuture();
    }

    private static void assertResetClusterMessageIsNotPresentAt(IgniteImpl ignite) {
        assertThat(new SystemDisasterRecoveryStorage(ignite.vault()).readResetClusterMessage(), is(nullValue()));
    }

    @Test
    void repairWhenCmgWas3Nodes() throws Exception {
        cluster.startAndInit(6, paramsBuilder -> {
            paramsBuilder.cmgNodeNames(nodeNames(0, 1, 2));
            paramsBuilder.metaStorageNodeNames(nodeNames(2, 3, 4));
            // Node with index 5 will host neither of voting sets.
        });
        waitTillClusterStateIsSavedToVaultOnConductor(2);

        // Stop the majority of CMG.
        IntStream.of(0, 1).parallel().forEach(cluster::stopNode);

        IgniteImpl igniteImpl2BeforeRestart = igniteImpl(2);

        assertThatCmgHasNoMajority(igniteImpl2BeforeRestart);

        initiateCmgRepairVia(igniteImpl2BeforeRestart, 2, 3, 4);

        IgniteImpl restartedIgniteImpl2 = waitTillNodeRestartsInternally(2);
        waitTillCmgHasMajority(restartedIgniteImpl2);

        // TODO: IGNITE-23096 - remove after the hang is fixed.
        waitTillNodesRestartInProcess(3, 4, 5);
    }

    private void waitTillNodesRestartInProcess(int... nodeIndexes) throws InterruptedException {
        for (int i : nodeIndexes) {
            waitTillNodeRestartsInternally(i);
        }
    }

    @Test
    void repairedClusterCanJoinBlankNodes() throws Exception {
        cluster.startAndInit(2, paramsBuilder -> {
            paramsBuilder.cmgNodeNames(nodeNames(0));
            paramsBuilder.metaStorageNodeNames(nodeNames(1));
        });
        waitTillClusterStateIsSavedToVaultOnConductor(1);

        cluster.stopNode(0);

        initiateCmgRepairVia(igniteImpl(1), 1);

        // Doing this wait to make sure that blank node will be able to connect at least someone. If we don't do this, the new node
        // will still be able to connect, but this will happen on Scalecube's initial sync retry, and we don't want to wait for it
        // in our test.
        waitTillNodeRestartsInternally(1);

        IgniteImpl node2 = unwrapIgniteImpl(cluster.startNode(2));

        assertTrue(waitForCondition(
                () -> node2.logicalTopologyService().localLogicalTopology().nodes().stream()
                        .anyMatch(n -> node2.name().equals(n.name())),
                SECONDS.toMillis(10)
        ));
    }

    @Test
    void repairIsPossibleWhenAllNodesWaitForCmgMajorityOnJoin() throws Exception {
        cluster.startAndInit(3, paramsBuilder -> {
            paramsBuilder.cmgNodeNames(nodeNames(0));
            paramsBuilder.metaStorageNodeNames(nodeNames(1));
            // Node with index 2 will host neither of voting sets.
        });
        waitTillClusterStateIsSavedToVaultOnConductor(1);

        cluster.stopNode(0);

        IntStream.of(1, 2).parallel().forEach(this::restartPartially);

        initiateCmgRepairVia(((IgniteServerImpl) cluster.server(1)).igniteImpl(), 1);

        IgniteImpl restartedIgniteImpl1 = waitTillNodeRestartsInternally(1);
        waitTillCmgHasMajority(restartedIgniteImpl1);

        // TODO: IGNITE-23096 - remove after the hang is fixed.
        waitTillNodeRestartsInternally(2);
    }

    private void restartPartially(int index) {
        cluster.stopNode(index);
        cluster.startEmbeddedNode(index);
    }

    @Test
    void nodesThatSawNoReparationHaveSeparatePhysicalTopology() throws Exception {
        cluster.startAndInit(2, paramsBuilder -> {
            paramsBuilder.cmgNodeNames(nodeNames(0));
            paramsBuilder.metaStorageNodeNames(nodeNames(1));
        });
        waitTillClusterStateIsSavedToVaultOnConductor(1);

        // This makes the CMG majority go away.
        cluster.stopNode(0);

        initiateCmgRepairVia(igniteImpl(1), 1);

        IgniteImpl restartedIgniteImpl1 = waitTillNodeRestartsInternally(1);
        waitTillCmgHasMajority(restartedIgniteImpl1);

        // Starting the node that did not see the repair.
        cluster.startEmbeddedNode(0);

        assertFalse(
                waitForCondition(() -> restartedIgniteImpl1.clusterNodes().size() > 1, SECONDS.toMillis(3)),
                "Nodes from different clusters were able to establish a connection"
        );
    }
}
