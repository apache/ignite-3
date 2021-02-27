package org.apache.ignite.raft.client.rpc.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.RaftException;
import org.apache.ignite.raft.State;
import org.apache.ignite.raft.client.RaftClientCommonMessages;
import org.apache.ignite.raft.client.RaftClientCommonMessages.GetLeaderResponse;
import org.apache.ignite.raft.client.RaftClientCommonMessages.StatusResponse;
import org.apache.ignite.raft.client.message.ClientMessageBuilderFactory;
import org.apache.ignite.raft.client.rpc.RaftGroupRpcClient;
import org.apache.ignite.raft.rpc.InvokeCallback;
import org.apache.ignite.raft.rpc.Message;
import org.apache.ignite.raft.rpc.Node;
import org.apache.ignite.raft.rpc.RaftGroupMessage;
import org.apache.ignite.raft.rpc.RpcClient;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class RaftGroupRpcClientImpl implements RaftGroupRpcClient {
    private final ExecutorService executor;
    private final int defaultTimeout;
    private final RpcClient rpcClient;

    /** Where to ask for initial configuration. */
    private final Set<Node> initialCfgNodes;

    /** */
    private final ClientMessageBuilderFactory factory;

    private Map<String, StateImpl> states = new ConcurrentHashMap<>();

    /**
     * Accepts dependencies in constructor.
     * @param rpcClient
     * @param defaultTimeout
     * @param initialCfgNode Initial configuration nodes.
     */
    public RaftGroupRpcClientImpl(RpcClient rpcClient, ClientMessageBuilderFactory factory, int defaultTimeout, Set<Node> initialCfgNodes) {
        this.defaultTimeout = defaultTimeout;
        this.rpcClient = rpcClient;
        this.factory = factory;
        this.initialCfgNodes = new HashSet<>(initialCfgNodes);
        executor = Executors.newWorkStealingPool();
    }

    @Override public State state(String groupId) {
        return getState(groupId);
    }

    private StateImpl getState(String groupId) {
        StateImpl newState = new StateImpl();

        StateImpl state = states.putIfAbsent(groupId, newState);

        if (state == null)
            state = newState;

        return state;
    }

    @Override public CompletableFuture<PeerId> refreshLeader(String groupId) {
        StateImpl state = getState(groupId);

        return refreshLeader(initialCfgNodes.iterator().next(), groupId).
            thenApply(resp -> {
                PeerId leaderId = resp.getLeaderId();

                state.leader = leaderId;

                return leaderId;
            });
    }

    @Override public CompletableFuture<State> refreshMembers(String groupId) {
        return null;
    }

    @Override public CompletableFuture<RaftClientCommonMessages.AddPeerResponse> addPeer(RaftClientCommonMessages.AddPeerRequest request) {
        return null;
    }

    @Override public CompletableFuture<RaftClientCommonMessages.RemovePeerResponse> removePeer(RaftClientCommonMessages.RemovePeerRequest request) {
        return null;
    }

    @Override public CompletableFuture<StatusResponse> resetPeers(PeerId peerId, RaftClientCommonMessages.ResetPeerRequest request) {
        return null;
    }

    @Override public CompletableFuture<StatusResponse> snapshot(PeerId peerId, RaftClientCommonMessages.SnapshotRequest request) {
        return null;
    }

    @Override public CompletableFuture<RaftClientCommonMessages.ChangePeersResponse> changePeers(RaftClientCommonMessages.ChangePeersRequest request) {
        return null;
    }

    @Override public CompletableFuture<RaftClientCommonMessages.LearnersOpResponse> addLearners(RaftClientCommonMessages.AddLearnersRequest request) {
        return null;
    }

    @Override public CompletableFuture<RaftClientCommonMessages.LearnersOpResponse> removeLearners(RaftClientCommonMessages.RemoveLearnersRequest request) {
        return null;
    }

    @Override public CompletableFuture<RaftClientCommonMessages.LearnersOpResponse> resetLearners(RaftClientCommonMessages.ResetLearnersRequest request) {
        return null;
    }

    @Override public CompletableFuture<StatusResponse> transferLeader(RaftClientCommonMessages.TransferLeaderRequest request) {
        return null;
    }

    private CompletableFuture<GetLeaderResponse> refreshLeader(Node node, String groupId) {
        StateImpl state = getState(groupId);

        while(true) {
            CompletableFuture<GetLeaderResponse> fut = state.updateFutRef.get();

            if (fut != null)
                return fut;

            if (state.updateFutRef.compareAndSet(null, (fut = new CompletableFuture<>()))) {
                RaftClientCommonMessages.GetLeaderRequest req = factory.createGetLeaderRequest().setGroupId(groupId).build();

                CompletableFuture<GetLeaderResponse> finalFut = fut;

                rpcClient.invokeAsync(node, req, new InvokeCallback<GetLeaderResponse>() {
                    @Override public void complete(GetLeaderResponse response, Throwable err) {
                        if (err != null)
                            finalFut.completeExceptionally(err);
                        else
                            finalFut.complete(response);

                        state.updateFutRef.set(null);
                    }
                }, executor, defaultTimeout);

                return fut;
            }
        }
    }

    @Override public <R extends Message> CompletableFuture<R> sendCustom(RaftGroupMessage request) {
        State state = state(request.getGroupId());

        CompletableFuture<R> fut = new CompletableFuture<>();

        fut.orTimeout(defaultTimeout, TimeUnit.MILLISECONDS);

        CompletableFuture<PeerId> fut0 = state.leader() == null ?
            refreshLeader(request.getGroupId()) : completedFuture(state.leader());

        fut0.whenComplete(new BiConsumer<PeerId, Throwable>() {
            @Override public void accept(PeerId peerId, Throwable error) {
                if (error == null) {
                    rpcClient.invokeAsync(peerId.getNode(), request, new InvokeCallback<R>() {
                        @Override public void complete(R response, Throwable err) {
                            if (err != null)
                                fut.completeExceptionally(err);
                            else {
                                if (response instanceof StatusResponse) {
                                    StatusResponse resp = (StatusResponse) response;

                                    // Translate error response to exception with the code.
                                    if (resp.getStatusCode() != 0)
                                        fut.completeExceptionally(new RaftException(resp.getStatusCode(), resp.getStatusMsg()));
                                    else
                                        fut.complete(response);
                                } else
                                    fut.complete(response);
                            }
                        }
                    }, executor, defaultTimeout);
                }
                else {
                    fut.completeExceptionally(error);
                }
            }
        });

        return fut;
    }

    @Override public ClientMessageBuilderFactory factory() {
        return this.factory;
    }

    private static class StateImpl implements State {
        private volatile PeerId leader;

        private volatile List<PeerId> peers;

        private volatile List<PeerId> learners;

        private AtomicReference<CompletableFuture<GetLeaderResponse>> updateFutRef = new AtomicReference<>();

        @Override public @Nullable PeerId leader() {
            return leader;
        }

        @Override public @Nullable List<PeerId> peers() {
            return peers;
        }

        @Override public @Nullable List<PeerId> learners() {
            return learners;
        }
    }
}
