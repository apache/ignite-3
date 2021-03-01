package org.apache.ignite.raft.client;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.raft.PeerId;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.apache.ignite.raft.client.message.RaftClientMessageFactoryImpl.INSTANCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;

public class MockUtils {
    public static PeerId LEADER = new PeerId(new NetworkMember("test"));

    public static class TestInput1 {
    }

    public static class TestOutput1 {
    }

    public static class TestInput2 {
    }

    public static class TestOutput2 {
    }

    public static void mockUserInput1(NetworkCluster cluster) {
        Mockito.doAnswer(new Answer() {
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                RaftClientMessages.UserResponse resp = INSTANCE.createUserResponse().setResponse(new TestOutput1()).build();

                return CompletableFuture.completedFuture(resp);
            }
        }).when(cluster).sendWithResponse(eq(LEADER.getNode()), argThat(new ArgumentMatcher<RaftClientMessages.UserRequest>() {
            @Override public boolean matches(RaftClientMessages.UserRequest arg) {
                return arg.request() instanceof TestInput1;
            }
        }), anyLong());
    }

    public static void mockUserInput2(NetworkCluster cluster) {
        Mockito.doAnswer(new Answer() {
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                RaftClientMessages.UserResponse resp = INSTANCE.createUserResponse().setResponse(new TestOutput2()).build();

                return CompletableFuture.completedFuture(resp);
            }
        }).when(cluster).sendWithResponse(eq(LEADER.getNode()), argThat(new ArgumentMatcher<RaftClientMessages.UserRequest>() {
            @Override public boolean matches(RaftClientMessages.UserRequest arg) {
                return arg.request() instanceof TestInput2;
            }
        }), anyLong());
    }

    public static void mockLeaderRequest(NetworkCluster cluster, boolean timeout) {
        Mockito.doAnswer(new Answer() {
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                RaftClientMessages.GetLeaderResponse resp = INSTANCE.createGetLeaderResponse().setLeaderId(LEADER).build();

                return timeout ? CompletableFuture.failedFuture(new TimeoutException()) : CompletableFuture.completedFuture(resp);
            }
        }).when(cluster).sendWithResponse(eq(LEADER.getNode()), any(RaftClientMessages.GetLeaderRequest.class), anyLong());
    }
}
