package org.apache.ignite.raft.client.rpc.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.RaftException;
import org.apache.ignite.raft.State;
import org.apache.ignite.raft.client.RaftClientCommonMessages;
import org.apache.ignite.raft.client.RaftClientCommonMessages.StatusResponse;
import org.apache.ignite.raft.client.rpc.RaftGroupRpcClient;
import org.apache.ignite.raft.client.message.RaftClientCommonMessageBuilderFactory;
import org.apache.ignite.raft.rpc.InvokeCallback;
import org.apache.ignite.raft.rpc.Message;
import org.apache.ignite.raft.rpc.Node;
import org.apache.ignite.raft.rpc.RaftGroupMessage;
import org.apache.ignite.raft.rpc.RpcClient;

public class RaftGroupRpcClientImpl implements RaftGroupRpcClient {
    private final ExecutorService executor;
    private final int defaultTimeout;
    private final RpcClient rpcClient;

    /** Where to ask for initial configuration. */
    private final Set<Node> initialCfgNodes;

    private Map<String, State> states = new ConcurrentHashMap<>();

    /**
     * Accepts dependencies in constructor.
     * @param rpcClient
     * @param defaultTimeout
     * @param initialCfgNode Initial configuration nodes.
     */
    public RaftGroupRpcClientImpl(RpcClient rpcClient, int defaultTimeout, Set<Node> initialCfgNodes) {
        this.defaultTimeout = defaultTimeout;
        this.rpcClient = rpcClient;
        this.initialCfgNodes = new HashSet<>(initialCfgNodes);
        executor = Executors.newWorkStealingPool();
    }

    @Override public State state(String groupId) {
        State newState = new State();

        State state = states.putIfAbsent(groupId, newState);

        if (state == null)
            state = newState;

        return state;
    }

    @Override public CompletableFuture<PeerId> refreshLeader(String groupId) {
        State state = state(groupId);

        if (state.getLeader() == null) {
            synchronized (state) {
                PeerId leader = state.getLeader();

                if (leader == null) {
                    return refreshLeader(initialCfgNodes.iterator().next(), groupId).
                        thenApply(new Function<RaftClientCommonMessages.GetLeaderResponse, PeerId>() {
                            @Override public PeerId apply(RaftClientCommonMessages.GetLeaderResponse getLeaderResponse) {
                                PeerId leaderId = getLeaderResponse.getLeaderId();

                                state.setLeader(leaderId);

                                return leaderId;
                            }
                        });
                }
            }
        }

        return CompletableFuture.completedFuture(state.getLeader());
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

    private CompletableFuture<RaftClientCommonMessages.GetLeaderResponse> refreshLeader(Node node, String groupId) {
        RaftClientCommonMessages.GetLeaderRequest req = RaftClientCommonMessageBuilderFactory.DEFAULT.createGetLeaderRequest().setGroupId(groupId).build();

        CompletableFuture<RaftClientCommonMessages.GetLeaderResponse> fut = new CompletableFuture<>();

        rpcClient.invokeAsync(node, req, new InvokeCallback<RaftClientCommonMessages.GetLeaderResponse>() {
            @Override public void complete(RaftClientCommonMessages.GetLeaderResponse response, Throwable err) {
                if (err != null)
                    fut.completeExceptionally(err);
                else
                    fut.complete(response);
            }
        }, executor, defaultTimeout);

        return fut;
    }

    @Override public <R extends Message> CompletableFuture<R> sendCustom(RaftGroupMessage request) {
        State state = state(request.getGroupId());

        CompletableFuture<R> fut = new CompletableFuture<>();

        fut.orTimeout(defaultTimeout, TimeUnit.MILLISECONDS);

        CompletableFuture<PeerId> fut0 = refreshLeader(request.getGroupId());

        // TODO implement clean chaining.
        fut0.whenComplete(new BiConsumer<PeerId, Throwable>() {
            @Override public void accept(PeerId peerId, Throwable error) {
                if (error == null) {
                    rpcClient.invokeAsync(state.getLeader().getNode(), request, new InvokeCallback<R>() {
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
}
