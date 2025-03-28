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

package org.apache.ignite.internal.cluster.management;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorageManager;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.junit.jupiter.api.Test;

class ItClusterVersionUpgradeTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void versionUpgradeWorks() throws Exception {
        IgniteProductVersion nextVersion = incrementVersion(IgniteProductVersion.CURRENT_VERSION);

        rewriteProductVersionInCmg(nextVersion.toString());

        assertDoesNotThrow(() -> cluster.restartNode(0));
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    private static IgniteProductVersion incrementVersion(IgniteProductVersion version) {
        return new IgniteProductVersion(
                version.major(),
                version.minor(),
                (byte) (version.maintenance() + 1),
                version.patch(),
                version.preRelease()
        );
    }

    private void rewriteProductVersionInCmg(String versionToSet) throws InterruptedException, ExecutionException {
        // To make sure that after restart the node will see new version in its CMG, we update the storage and then make a snapshot.
        // When starting, the node will restore the CMG storage from snapshot, applying the rewritten version.

        writeProductVersionToCmgStorage(versionToSet);
        doSnapshotOfCmgStateMachine();
    }

    private void writeProductVersionToCmgStorage(String versionToSet) {
        IgniteImpl ignite = igniteImpl(0);
        var clusterStateStorageManager = new ClusterStateStorageManager(ignite.clusterStateStorage());

        ClusterState clusterState = clusterStateStorageManager.getClusterState();
        assertThat(clusterState, is(notNullValue()));

        ClusterState newClusterState = copyChangingVersion(clusterState, versionToSet);

        clusterStateStorageManager.putClusterState(newClusterState);
    }

    private static ClusterState copyChangingVersion(ClusterState clusterState, String versionToSet) {
        return new CmgMessagesFactory().clusterState()
                .cmgNodes(clusterState.cmgNodes())
                .metaStorageNodes(clusterState.metaStorageNodes())
                .version(versionToSet)
                .clusterTag(clusterState.clusterTag())
                .initialClusterConfiguration(clusterState.initialClusterConfiguration())
                .formerClusterIds(clusterState.formerClusterIds())
                .build();
    }

    private void doSnapshotOfCmgStateMachine() throws InterruptedException, ExecutionException {
        RaftGroupService cmgRaftGroupService = cluster.leaderServiceFor(CmgGroupId.INSTANCE);

        CompletableFuture<Status> fut = new CompletableFuture<>();
        cmgRaftGroupService.getRaftNode().snapshot(fut::complete, true);

        assertThat(fut, willCompleteSuccessfully());
        assertEquals(RaftError.SUCCESS, fut.get().getRaftError());
    }
}
