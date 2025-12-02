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

public class ItMetastorageSnapshotTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
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
