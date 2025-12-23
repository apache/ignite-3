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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.CliRequests.SnapshotRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/** Tests for metastorage snapshot installation. */
public class ItMetastorageInstallSnapshotTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Override
    protected int[] cmgMetastoreNodes() {
        return new int[]{0};
    }

    @Test
    void testInstallSnapshotWithoutNewCommands(TestInfo testInfo) {
        truncateRaftLog();

        cluster.shutdown();

        ClusterConfiguration.Builder clusterConfiguration = ClusterConfiguration.builder(testInfo, workDir)
                .defaultNodeBootstrapConfigTemplate(getNodeBootstrapConfigTemplate());

        customizeConfiguration(clusterConfiguration);

        cluster = new Cluster(clusterConfiguration.build());

        cluster.startAndInit(testInfo, 2, new int[]{0}, this::customizeInitParameters);
    }

    private void truncateRaftLog() {
        IgniteImpl igniteImpl = unwrapIgniteImpl(node(0));

        SnapshotRequest request = new RaftMessagesFactory().snapshotRequest()
                .groupId(MetastorageGroupId.INSTANCE.toString())
                .peerId(igniteImpl.name())
                .forced(true)
                .build();

        CompletableFuture<Object> snapshotFut = igniteImpl.raftManager().messagingService()
                .invoke(igniteImpl.name(), request, 10_000L)
                .thenApply(ignored -> null);

        assertThat(snapshotFut, willCompleteSuccessfully());
    }
}
