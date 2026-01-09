package org.apache.ignite.raft;

import static org.apache.ignite.internal.raft.server.RaftGroupOptions.defaults;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.raft.server.JraftAbstractTest;
import org.apache.ignite.raft.server.counter.CounterListener;
import org.junit.jupiter.api.Test;

public class ItRaftMetricTest extends JraftAbstractTest {
    private static final TablePartitionId TEST_GROUP = new TablePartitionId(0, 0);

    @Test
    void testLeaderCount() throws Exception {
        JraftServerImpl jraftServer = startServer(0, raftServer -> {
            String localNodeName = raftServer.clusterService().topologyService().localMember().name();

            Peer serverPeer = initialMembersConf.peer(localNodeName);

            RaftGroupOptions groupOptions = groupOptions(raftServer);

            groupOptions.setLogStorageFactory(logStorageFactories.get(0));
            groupOptions.serverDataPath(serverWorkingDirs.get(0).metaPath());

            raftServer.startRaftNode(new RaftNodeId(TEST_GROUP, serverPeer), initialMembersConf, new CounterListener(), groupOptions);
        }, opts -> {});

        RaftGroupService raftClient1 = startClient(TEST_GROUP);
        CompletableFuture<LeaderWithTerm> leader = raftClient1.refreshAndGetLeaderWithTerm();

        assertThat(leader, willCompleteSuccessfully());

        assertThat(jraftServer.options().getRaftMetrics().leaderMetric(), is(1));
    }

    private static RaftGroupOptions groupOptions(RaftServer raftServer) {
        return defaults().commandsMarshaller(new ThreadLocalOptimizedMarshaller(raftServer.clusterService().serializationRegistry()));
    }
}
