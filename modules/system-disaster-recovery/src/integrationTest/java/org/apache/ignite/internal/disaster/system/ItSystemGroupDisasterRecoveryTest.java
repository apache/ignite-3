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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.app.IgniteServerImpl;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.jetbrains.annotations.Nullable;

/**
 * Base for tests of CMG and Metastorage group disaster recovery.
 */
abstract class ItSystemGroupDisasterRecoveryTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 0;
    }

    final void startAndInitCluster(int nodeCount, int[] cmgNodeIndexes, int[] metastorageNodeIndexes) {
        // Pre-allocate this to make sure that for each pair of nodes, if they start almost at the same time, at least one is able to make
        // an initial sync to another one.
        cluster.overrideSeedsCount(10);

        cluster.startAndInit(nodeCount, paramsBuilder -> {
            paramsBuilder.cmgNodeNames(nodeNames(cmgNodeIndexes));
            paramsBuilder.metaStorageNodeNames(nodeNames(metastorageNodeIndexes));
        });
    }

    final void waitTillClusterStateIsSavedToVaultOnConductor(int nodeIndex) throws InterruptedException {
        assertTrue(waitForCondition(
                () -> new SystemDisasterRecoveryStorage(igniteImpl(nodeIndex).vault()).readClusterState() != null,
                SECONDS.toMillis(10)
        ));
    }

    final String[] nodeNames(int... nodeIndexes) {
        return IntStream.of(nodeIndexes)
                .mapToObj(cluster::nodeName)
                .toArray(String[]::new);
    }

    final IgniteImpl waitTillNodeRestartsInternally(int nodeIndex) throws InterruptedException {
        assertThat(waitForRestartFuture(nodeIndex), willCompleteSuccessfully());

        return unwrapIgniteImpl(cluster.server(nodeIndex).api());
    }

    final CompletableFuture<Void> waitForRestartFuture(int nodeIndex) throws InterruptedException {
        // restartFuture() becomes non-null when restart is initiated.

        assertTrue(
                waitForCondition(() -> restartFuture(nodeIndex) != null, SECONDS.toMillis(20)),
                "Node did not attempt to be restarted in time"
        );

        CompletableFuture<Void> future = restartFuture(nodeIndex);
        assertNotNull(future);

        return future;
    }

    @Nullable
    private CompletableFuture<Void> restartFuture(int nodeIndex) {
        return ((IgniteServerImpl) cluster.server(nodeIndex)).restartFuture();
    }

    static ClusterState clusterState(IgniteImpl ignite) throws InterruptedException, ExecutionException, TimeoutException {
        return ignite.clusterManagementGroupManager().clusterState().get(10, SECONDS);
    }

    static void assertResetClusterMessageIsNotPresentAt(IgniteImpl ignite) {
        assertThat(new SystemDisasterRecoveryStorage(ignite.vault()).readResetClusterMessage(), is(nullValue()));
    }

    final void waitTillNodesRestartInternally(int... nodeIndexes) throws InterruptedException {
        for (int i : nodeIndexes) {
            waitTillNodeRestartsInternally(i);
        }
    }

    final void restartPartially(int index) {
        cluster.stopNode(index);
        cluster.startEmbeddedNode(index);
    }

    final void migrate(int oldClusterNodeIndex, int newClusterNodeIndex) throws Exception {
        initiateMigration(oldClusterNodeIndex, newClusterNodeIndex);

        waitTillNodeRestartsInternally(oldClusterNodeIndex);
    }

    final void initiateMigration(int oldClusterNodeIndex, int newClusterNodeIndex) {
        // Starting the node that did not see the repair.
        cluster.startEmbeddedNode(oldClusterNodeIndex);

        initiateMigrationToNewCluster(oldClusterNodeIndex, newClusterNodeIndex);
    }

    final void initiateMigrationToNewCluster(int nodeMissingRepairIndex, int repairedNodeIndex) {
        SystemDisasterRecoveryClient.initiateMigration(
                "localhost",
                cluster.httpPort(nodeMissingRepairIndex),
                "localhost",
                cluster.httpPort(repairedNodeIndex)
        );
    }

    final void assertTopologyContainsNode(int nodeIndex, LogicalTopologySnapshot topologySnapshot) {
        assertTrue(topologySnapshot.hasNode(cluster.nodeName(nodeIndex)));
    }
}
