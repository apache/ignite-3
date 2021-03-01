package org.apache.ignite.raft.client.service;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.raft.client.MockUtils.TestInput1;
import org.apache.ignite.raft.client.MockUtils.TestInput2;
import org.apache.ignite.raft.client.MockUtils.TestOutput1;
import org.apache.ignite.raft.client.MockUtils.TestOutput2;
import org.apache.ignite.raft.client.rpc.RaftGroupRpcClient;
import org.apache.ignite.raft.client.rpc.impl.RaftGroupRpcClientImpl;
import org.apache.ignite.raft.client.service.impl.RaftGroupClientRequestServiceImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static java.util.Collections.singleton;
import static org.apache.ignite.raft.client.MockUtils.LEADER;
import static org.apache.ignite.raft.client.MockUtils.mockLeaderRequest;
import static org.apache.ignite.raft.client.MockUtils.mockUserInput1;
import static org.apache.ignite.raft.client.MockUtils.mockUserInput2;
import static org.apache.ignite.raft.client.message.RaftClientMessageFactoryImpl.INSTANCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(MockitoExtension.class)
public class RaftGroupClientRequestServiceTest {
    @Mock
    private NetworkCluster cluster;

    @Test
    public void testUserRequest() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);
        mockUserInput1(cluster);
        mockUserInput2(cluster);

        RaftGroupRpcClient client = new RaftGroupRpcClientImpl(cluster, INSTANCE, 5_000, singleton(LEADER.getNode()));

        RaftGroupClientRequestService service = new RaftGroupClientRequestServiceImpl(client, groupId);

        assertNull(client.state(groupId).leader());

        CompletableFuture<TestOutput1> fut1 = service.submit(new TestInput1());

        TestOutput1 output1 = fut1.get();

        assertNotNull(output1);

        CompletableFuture<TestOutput2> fut2 = service.submit(new TestInput2());

        TestOutput2 output2 = fut2.get();

        assertNotNull(output2);

        assertEquals(LEADER, client.state(groupId).leader());
    }
}
