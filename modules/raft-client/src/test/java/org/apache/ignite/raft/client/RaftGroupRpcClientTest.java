package org.apache.ignite.raft.client;

import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.State;
import org.apache.ignite.raft.client.rpc.impl.RaftGroupRpcClientImpl;
import org.apache.ignite.raft.client.message.RaftClientCommonMessageBuilderFactory;
import org.apache.ignite.raft.client.rpc.RaftGroupRpcClient;
import org.apache.ignite.raft.rpc.InvokeCallback;
import org.apache.ignite.raft.rpc.Message;
import org.apache.ignite.raft.rpc.NodeImpl;
import org.apache.ignite.raft.rpc.RaftGroupMessage;
import org.apache.ignite.raft.rpc.RpcClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;

@ExtendWith(MockitoExtension.class)
public class RaftGroupRpcClientTest {
    @Mock
    private RpcClient rpcClient;

    private static PeerId leader = new PeerId(new NodeImpl("test"));

    @Test
    public void testCustomMessage() throws Exception {
        String groupId = "test";

        mockClient();

        // TODO FIXME asch where to get initial configuration for the group ?
        RaftGroupRpcClient client = new RaftGroupRpcClientImpl(rpcClient, 5_000, Collections.singleton(leader.getNode()));

        JunkRequest req = new JunkRequest(groupId);

        Future<Message> fut = client.sendCustom(req);

        State state = client.state(groupId, false);

        // Expecting raft group state to be transparently loaded on first request.
        assertEquals(leader, state.getLeader());

        assertTrue(fut.get() instanceof JunkResponse);
    }

    private static class JunkRequest implements RaftGroupMessage {
        private final String groupId;

        JunkRequest(String groupId) {
            this.groupId = groupId;
        }

        @Override public String getGroupId() {
            return groupId;
        }
    }

    private static class JunkResponse implements Message {}

    private void mockClient() {
        // Mock junk request.
        Mockito.doAnswer(new Answer() {
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                InvokeCallback callback = invocation.getArgument(2);
                Executor executor = invocation.getArgument(3);

                executor.execute(() -> callback.complete(new JunkResponse(), null));

                return null;
            }
        }).when(rpcClient).invokeAsync(eq(leader.getNode()), any(JunkRequest.class), any(), any(), anyLong());

        // Mock get leader request.
        Mockito.doAnswer(new Answer() {
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                InvokeCallback callback = invocation.getArgument(2);
                Executor executor = invocation.getArgument(3);

                executor.execute(() -> callback.complete(RaftClientCommonMessageBuilderFactory.DEFAULT.createGetLeaderResponse().setLeaderId(leader).build(), null));

                return null;
            }
        }).when(rpcClient).invokeAsync(eq(leader.getNode()), any(RaftClientCommonMessages.GetLeaderRequest.class), any(), any(), anyLong());
    }
}
