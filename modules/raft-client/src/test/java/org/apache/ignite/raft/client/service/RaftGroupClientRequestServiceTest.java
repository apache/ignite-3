package org.apache.ignite.raft.client.service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.client.RaftClientMessages;
import org.apache.ignite.raft.client.RaftClientMessages.UserRequest;
import org.apache.ignite.raft.client.rpc.RaftGroupRpcClient;
import org.apache.ignite.raft.client.rpc.impl.RaftGroupRpcClientImpl;
import org.apache.ignite.raft.client.service.impl.RaftGroupClientRequestServiceImpl;
import org.apache.ignite.raft.rpc.InvokeCallback;
import org.apache.ignite.raft.rpc.NodeImpl;
import org.apache.ignite.raft.rpc.RpcClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import static java.util.Collections.singleton;
import static org.apache.ignite.raft.client.message.RaftClientMessageBuilderFactory.INSTANCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;

@ExtendWith(MockitoExtension.class)
public class RaftGroupClientRequestServiceTest {
    @Mock
    private RpcClient rpcClient;

    private static PeerId leader = new PeerId(new NodeImpl("test"));

    @Test
    public void testUserRequest() throws Exception {
        String groupId = "test";

        mockLeaderRequest();
        mockUserRequest1();
        mockUserRequest2();

        RaftGroupRpcClient client = new RaftGroupRpcClientImpl(rpcClient, INSTANCE, 5_000, singleton(leader.getNode()));

        RaftGroupClientRequestService service = new RaftGroupClientRequestServiceImpl(client, groupId);

        assertNull(client.state(groupId).leader());

        CompletableFuture<TestOutput1> fut1 = service.submit(new TestInput1());

        TestOutput1 output1 = fut1.get();

        assertNotNull(output1);

        CompletableFuture<TestOutput2> fut2 = service.submit(new TestInput2());

        TestOutput2 output2 = fut2.get();

        assertNotNull(output2);

        assertEquals(leader, client.state(groupId).leader());
    }

    private static class TestInput1 {
    }

    private static class TestOutput1 {
    }

    private static class TestInput2 {
    }

    private static class TestOutput2 {
    }

    private void mockLeaderRequest() {
        Mockito.doAnswer(new Answer() {
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                InvokeCallback callback = invocation.getArgument(2);
                Executor executor = invocation.getArgument(3);

                executor.execute(() -> {
                    callback.complete(INSTANCE.createGetLeaderResponse().setLeaderId(leader).build(), null);
                });

                return null;
            }
        }).when(rpcClient).invokeAsync(eq(leader.getNode()), any(RaftClientMessages.GetLeaderRequest.class),
            any(), any(), anyLong());
    }

    private void mockUserRequest1() {
        Mockito.doAnswer(new Answer() {
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                InvokeCallback callback = invocation.getArgument(2);
                Executor executor = invocation.getArgument(3);

                executor.execute(() -> callback.complete(INSTANCE.createUserResponse().
                    setResponse(new TestOutput1()).build(), null));

                return null;
            }
        }).when(rpcClient).invokeAsync(eq(leader.getNode()), argThat(new ArgumentMatcher<UserRequest>() {
            @Override public boolean matches(UserRequest argument) {
                return argument.request() instanceof TestInput1;
            }
        }), any(), any(), anyLong());
    }

    private void mockUserRequest2() {
        Mockito.doAnswer(new Answer() {
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                InvokeCallback callback = invocation.getArgument(2);
                Executor executor = invocation.getArgument(3);

                executor.execute(() -> callback.complete(INSTANCE.createUserResponse().
                    setResponse(new TestOutput2()).build(), null));

                return null;
            }
        }).when(rpcClient).invokeAsync(eq(leader.getNode()), argThat(new ArgumentMatcher<UserRequest>() {
            @Override public boolean matches(UserRequest argument) {
                return argument.request() instanceof TestInput2;
            }
        }), any(), any(), anyLong());
    }
}
