package org.apache.ignite.raft.client;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.State;
import org.apache.ignite.raft.client.RaftClientCommonMessages.GetLeaderRequest;
import org.apache.ignite.raft.client.rpc.impl.RaftGroupRpcClientImpl;
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

import static org.apache.ignite.raft.client.message.ClientMessageBuilderFactory.DEFAULT_MESSAGE_BUILDER_FACTORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;

@ExtendWith(MockitoExtension.class)
public class RaftGroupRpcClientTest {
    @Mock
    private RpcClient rpcClient;

    private static PeerId leader = new PeerId(new NodeImpl("test"));

    @Test
    public void testRefreshLeader() throws Exception {
        String groupId = "test";

        mockLeaderRequest(false);

        RaftGroupRpcClient client = new RaftGroupRpcClientImpl(rpcClient, DEFAULT_MESSAGE_BUILDER_FACTORY,
            5_000, Collections.singleton(leader.getNode()));

        PeerId leaderId = client.refreshLeader(groupId).get();

        assertEquals(leader, client.state(groupId).leader());
        assertEquals(leader, leaderId);
    }

    @Test
    public void testRefreshLeaderMultithreaded() throws Exception {
        String groupId = "test";

        mockLeaderRequest(false);

        RaftGroupRpcClient client = new RaftGroupRpcClientImpl(rpcClient, DEFAULT_MESSAGE_BUILDER_FACTORY,
            5_000, Collections.singleton(leader.getNode()));

        int cnt = 20;

        Thread[] runners = new Thread[cnt];

        CountDownLatch l = new CountDownLatch(cnt);

        for (int i = 0; i < runners.length; i++) {
            runners[i] = new Thread(new Runnable() {
                @Override public void run() {
                    l.countDown();
                    try {
                        l.await();
                    }
                    catch (InterruptedException e) {
                        // Ignored.
                    }

                    try {
                        PeerId leaderId = client.refreshLeader(groupId).get();

                        assertEquals(leader, client.state(groupId).leader());
                        assertEquals(leader, leaderId);
                    }
                    catch (Exception e) {
                        fail(e);
                    }
                }
            });
            runners[i].setName("Executor-" + i);
            runners[i].start();
        }

        for (int i = 0; i < runners.length; i++)
            runners[i].join();
    }

    @Test
    public void testRefreshLeaderTimeout() throws Exception {
        String groupId = "test";

        mockLeaderRequest(true);

        RaftGroupRpcClient client = new RaftGroupRpcClientImpl(rpcClient, DEFAULT_MESSAGE_BUILDER_FACTORY,
            5_000, Collections.singleton(leader.getNode()));

        try {
            client.refreshLeader(groupId).get();

            fail();
        }
        catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    @Test
    public void testCustomMessage() throws Exception {
        String groupId = "test";

        mockLeaderRequest(false);
        mockCustomRequest();

        RaftGroupRpcClient client = new RaftGroupRpcClientImpl(rpcClient, DEFAULT_MESSAGE_BUILDER_FACTORY,
            5_000, Collections.singleton(leader.getNode()));

        JunkRequest req = new JunkRequest(groupId);

        Message resp = client.sendCustom(req).get();

        State state = client.state(groupId);

        // Expecting raft group state to be transparently loaded on first request.
        assertEquals(leader, state.leader());

        assertTrue(resp instanceof JunkResponse);
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

    private void mockCustomRequest() {
        Mockito.doAnswer(new Answer() {
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                InvokeCallback callback = invocation.getArgument(2);
                Executor executor = invocation.getArgument(3);

                executor.execute(() -> callback.complete(new JunkResponse(), null));

                return null;
            }
        }).when(rpcClient).invokeAsync(eq(leader.getNode()), any(JunkRequest.class), any(), any(), anyLong());
    }

    private void mockLeaderRequest(boolean timeout) {
        Mockito.doAnswer(new Answer() {
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                InvokeCallback callback = invocation.getArgument(2);
                Executor executor = invocation.getArgument(3);

                executor.execute(() -> {
                    if (timeout)
                        callback.complete(null, new TimeoutException());
                    else
                        callback.complete(DEFAULT_MESSAGE_BUILDER_FACTORY.createGetLeaderResponse().setLeaderId(leader).build(), null);
                });

                return null;
            }
        }).when(rpcClient).invokeAsync(eq(leader.getNode()), any(GetLeaderRequest.class), any(), any(), anyLong());
    }
}
