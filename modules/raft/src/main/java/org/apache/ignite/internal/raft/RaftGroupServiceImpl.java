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

package org.apache.ignite.internal.raft;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.ThreadLocalRandom.current;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.AddLearnersRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.AddPeerRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.AddPeerResponse;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAsyncRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAsyncResponse;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersResponse;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.GetLeaderRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.GetLeaderResponse;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.GetPeersRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.GetPeersResponse;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.LearnersOpResponse;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.RemoveLearnersRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.RemovePeerRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.RemovePeerResponse;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.ResetLearnersRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.SnapshotRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.TransferLeaderRequest;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.LeaderWithTerm;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.ActionResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ErrorResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.SMErrorResponse;
import org.apache.ignite.raft.jraft.rpc.impl.RaftException;
import org.apache.ignite.raft.jraft.rpc.impl.SMCompactedThrowable;
import org.apache.ignite.raft.jraft.rpc.impl.SMFullThrowable;
import org.apache.ignite.raft.jraft.rpc.impl.SMThrowable;
import org.jetbrains.annotations.Nullable;

/**
 * The implementation of {@link RaftGroupService}.
 */
public class RaftGroupServiceImpl implements RaftGroupService {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RaftGroupServiceImpl.class);

    private volatile long timeout;

    /** Timeout for network calls. */
    private final long rpcTimeout;

    private final String groupId;

    private final ReplicationGroupId realGroupId;

    private final RaftMessagesFactory factory;

    @Nullable
    private volatile Peer leader;

    private volatile List<Peer> peers;

    private volatile List<Peer> learners;

    private final ClusterService cluster;

    private final long retryDelay;

    /** Executor for scheduling retries of {@link RaftGroupServiceImpl#sendWithRetry} invocations. */
    private final ScheduledExecutorService executor;

    /**
     * Constructor.
     *
     * @param groupId Group id.
     * @param cluster A cluster.
     * @param factory A message factory.
     * @param timeout Request timeout.
     * @param peers Initial peers list.
     * @param learners Initial learners list.
     * @param leader Group leader.
     * @param retryDelay Retry delay.
     * @param executor Executor for retrying requests.
     */
    private RaftGroupServiceImpl(
            ReplicationGroupId groupId,
            ClusterService cluster,
            RaftMessagesFactory factory,
            int timeout,
            int rpcTimeout,
            List<Peer> peers,
            List<Peer> learners,
            @Nullable Peer leader,
            long retryDelay,
            ScheduledExecutorService executor
    ) {
        this.cluster = cluster;
        this.peers = List.copyOf(peers);
        this.learners = List.copyOf(learners);
        this.factory = factory;
        this.timeout = timeout;
        this.rpcTimeout = rpcTimeout;
        this.groupId = groupId.toString();
        this.realGroupId = groupId;
        this.retryDelay = retryDelay;
        this.leader = leader;
        this.executor = executor;
    }

    /**
     * Starts raft group service.
     *
     * @param groupId Raft group id.
     * @param cluster Cluster service.
     * @param factory Message factory.
     * @param timeout Timeout.
     * @param rpcTimeout Network call timeout.
     * @param peers Initial peers list.
     * @param learners Initial learners list.
     * @param getLeader {@code True} to get the group's leader upon service creation.
     * @param retryDelay Retry delay.
     * @param executor Executor for retrying requests.
     * @return Future representing pending completion of the operation.
     */
    public static CompletableFuture<RaftGroupService> start(
            ReplicationGroupId groupId,
            ClusterService cluster,
            RaftMessagesFactory factory,
            int timeout,
            int rpcTimeout,
            List<Peer> peers,
            List<Peer> learners,
            boolean getLeader,
            long retryDelay,
            ScheduledExecutorService executor
    ) {
        var service = new RaftGroupServiceImpl(groupId, cluster, factory, timeout, rpcTimeout, peers, learners, null, retryDelay, executor);

        if (!getLeader) {
            return CompletableFuture.completedFuture(service);
        }

        return service.refreshLeader().handle((unused, throwable) -> {
            if (throwable != null) {
                if (throwable.getCause() instanceof TimeoutException) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Failed to refresh a leader [groupId={}]", groupId);
                    }
                } else {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("Failed to refresh a leader [groupId={}]", throwable, groupId);
                    }
                }
            }

            return service;
        });
    }

    /**
     * Starts raft group service.
     *
     * @param groupId Raft group id.
     * @param cluster Cluster service.
     * @param factory Message factory.
     * @param timeout Timeout.
     * @param peers Initial peers list.
     * @param getLeader {@code True} to get the group's leader upon service creation.
     * @param retryDelay Retry delay.
     * @param executor Executor for retrying requests.
     * @return Future representing pending completion of the operation.
     */
    public static CompletableFuture<RaftGroupService> start(
            ReplicationGroupId groupId,
            ClusterService cluster,
            RaftMessagesFactory factory,
            int timeout,
            List<Peer> peers,
            boolean getLeader,
            long retryDelay,
            ScheduledExecutorService executor
    ) {
        return start(groupId, cluster, factory, timeout, timeout, peers, List.of(), getLeader, retryDelay, executor);
    }

    @Override
    public ReplicationGroupId groupId() {
        return realGroupId;
    }

    @Override
    public long timeout() {
        return timeout;
    }

    @Override
    public void timeout(long newTimeout) {
        this.timeout = newTimeout;
    }

    @Override
    public Peer leader() {
        return leader;
    }

    @Override
    public List<Peer> peers() {
        return peers;
    }

    @Override
    public List<Peer> learners() {
        return learners;
    }

    @Override
    public CompletableFuture<Void> refreshLeader() {
        GetLeaderRequest req = factory.getLeaderRequest().groupId(groupId).build();

        return this.<GetLeaderResponse>sendWithRetry(randomNode(), req)
                .thenAccept(resp -> this.leader = parsePeer(resp.leaderId()));
    }

    @Override
    public CompletableFuture<LeaderWithTerm> refreshAndGetLeaderWithTerm() {
        GetLeaderRequest req = factory.getLeaderRequest().groupId(groupId).build();

        return this.<GetLeaderResponse>sendWithRetry(randomNode(), req)
                .thenApply(resp -> {
                    Peer respLeader = parsePeer(resp.leaderId());

                    this.leader = respLeader;

                    return new LeaderWithTerm(respLeader, resp.currentTerm());
                });
    }

    @Override
    public CompletableFuture<Void> refreshMembers(boolean onlyAlive) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader().thenCompose(res -> refreshMembers(onlyAlive));
        }

        GetPeersRequest req = factory.getPeersRequest().onlyAlive(onlyAlive).groupId(groupId).build();

        return this.<GetPeersResponse>sendWithRetry(leader, req)
                .thenAccept(resp -> {
                    this.peers = parsePeerList(resp.peersList());
                    this.learners = parsePeerList(resp.learnersList());
                });
    }

    @Override
    public CompletableFuture<Void> addPeer(Peer peer) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader().thenCompose(res -> addPeer(peer));
        }

        AddPeerRequest req = factory.addPeerRequest().groupId(groupId).peerId(peerId(peer)).build();

        return this.<AddPeerResponse>sendWithRetry(leader, req)
                .thenAccept(resp -> this.peers = parsePeerList(resp.newPeersList()));
    }

    @Override
    public CompletableFuture<Void> removePeer(Peer peer) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader().thenCompose(res -> removePeer(peer));
        }

        RemovePeerRequest req = factory.removePeerRequest().groupId(groupId).peerId(peerId(peer)).build();

        return this.<RemovePeerResponse>sendWithRetry(leader, req)
                .thenAccept(resp -> this.peers = parsePeerList(resp.newPeersList()));
    }

    @Override
    public CompletableFuture<Void> changePeers(Collection<Peer> peers) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader().thenCompose(res -> changePeers(peers));
        }

        ChangePeersRequest req = factory.changePeersRequest().groupId(groupId).newPeersList(peerIds(peers)).build();

        return this.<ChangePeersResponse>sendWithRetry(leader, req)
                .thenAccept(resp -> this.peers = parsePeerList(resp.newPeersList()));
    }

    @Override
    public CompletableFuture<Void> changePeersAsync(Collection<Peer> peers, Collection<Peer> learners, long term) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader().thenCompose(res -> changePeersAsync(peers, learners, term));
        }

        ChangePeersAsyncRequest req = factory.changePeersAsyncRequest()
                .groupId(groupId)
                .term(term)
                .newPeersList(peerIds(peers))
                .newLearnersList(peerIds(learners))
                .build();

        LOG.info("Sending changePeersAsync request for group={} to peers={} and learners={} with leader term={}",
                groupId, peers, learners, term);

        return this.<ChangePeersAsyncResponse>sendWithRetry(leader, req)
                .thenAccept(resp -> {
                    // We expect that all raft related errors will be handled by sendWithRetry, means that
                    // such responses will initiate a retrying of the original request.
                    assert !(resp instanceof RpcRequests.ErrorResponse);
                });
    }

    @Override
    public CompletableFuture<Void> addLearners(List<Peer> learners) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader().thenCompose(res -> addLearners(learners));
        }

        AddLearnersRequest req = factory.addLearnersRequest().groupId(groupId).learnersList(peerIds(learners)).build();

        return this.<LearnersOpResponse>sendWithRetry(leader, req)
                .thenAccept(resp -> this.learners = parsePeerList(resp.newLearnersList()));
    }

    @Override
    public CompletableFuture<Void> removeLearners(List<Peer> learners) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader().thenCompose(res -> removeLearners(learners));
        }

        RemoveLearnersRequest req = factory.removeLearnersRequest().groupId(groupId).learnersList(peerIds(learners)).build();

        return this.<LearnersOpResponse>sendWithRetry(leader, req)
                .thenAccept(resp -> this.learners = parsePeerList(resp.newLearnersList()));
    }

    @Override
    public CompletableFuture<Void> resetLearners(List<Peer> learners) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader().thenCompose(res -> resetLearners(learners));
        }

        ResetLearnersRequest req = factory.resetLearnersRequest().groupId(groupId).learnersList(peerIds(learners)).build();

        return this.<LearnersOpResponse>sendWithRetry(leader, req)
                .thenAccept(resp -> this.learners = parsePeerList(resp.newLearnersList()));
    }

    @Override
    public CompletableFuture<Void> snapshot(Peer peer) {
        SnapshotRequest req = factory.snapshotRequest().groupId(groupId).build();

        // Disable the timeout for a snapshot request.
        return resolvePeer(peer)
                .thenCompose(node -> cluster.messagingService().invoke(node, req, Integer.MAX_VALUE))
                .thenAccept(resp -> {
                    if (resp != null) {
                        RpcRequests.ErrorResponse resp0 = (RpcRequests.ErrorResponse) resp;

                        if (resp0.errorCode() != RaftError.SUCCESS.getNumber()) {
                            var ex = new RaftException(RaftError.forNumber(resp0.errorCode()), resp0.errorMsg());

                            throw new CompletionException(ex);
                        }
                    }
                });
    }

    @Override
    public CompletableFuture<Void> transferLeadership(Peer newLeader) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader().thenCompose(res -> transferLeadership(newLeader));
        }

        TransferLeaderRequest req = factory.transferLeaderRequest()
                .groupId(groupId)
                .leaderId(peerId(leader))
                .peerId(peerId(newLeader))
                .build();

        return sendWithRetry(leader, req)
                .thenRun(() -> this.leader = newLeader);
    }

    @Override
    public <R> CompletableFuture<R> run(Command cmd) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader().thenCompose(res -> run(cmd));
        }

        ActionRequest req = factory.actionRequest().command(cmd).groupId(groupId).readOnlySafe(true).build();

        return this.<ActionResponse>sendWithRetry(leader, req)
                .thenApply(resp -> (R) resp.result());
    }

    @Override
    public void shutdown() {
        // No-op.
    }

    @Override
    public ClusterService clusterService() {
        return cluster;
    }

    private <R extends NetworkMessage> CompletableFuture<R> sendWithRetry(Peer peer, NetworkMessage req) {
        var future = new CompletableFuture<R>();

        sendWithRetry(peer, req, currentTimeMillis() + timeout, future);

        return future;
    }

    /**
     * Retries a request until success or timeout.
     *
     * @param peer Target peer.
     * @param req The request.
     * @param stopTime Stop time.
     * @param fut The future.
     * @param <R> Response type.
     */
    private <R extends NetworkMessage> void sendWithRetry(Peer peer, NetworkMessage req, long stopTime, CompletableFuture<R> fut) {
        if (currentTimeMillis() >= stopTime) {
            fut.completeExceptionally(new TimeoutException());

            return;
        }

        //TODO: IGNITE-15389 org.apache.ignite.internal.metastorage.client.CursorImpl has potential deadlock inside
        resolvePeer(peer)
                .thenCompose(node -> cluster.messagingService().invoke(node, req, rpcTimeout))
                .whenCompleteAsync((resp, err) -> {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("sendWithRetry resp={} from={} to={} err={}",
                                S.toString(resp),
                                cluster.topologyService().localMember().address(),
                                peer.consistentId(),
                                err == null ? null : err.getMessage());
                    }

                    if (err != null) {
                        handleThrowable(err, peer, req, stopTime, fut);
                    } else if (resp instanceof ErrorResponse) {
                        handleErrorResponse((ErrorResponse) resp, peer, req, stopTime, fut);
                    } else if (resp instanceof SMErrorResponse) {
                        handleSmErrorResponse((SMErrorResponse) resp, fut);
                    } else {
                        leader = peer; // The OK response was received from a leader.

                        fut.complete((R) resp);
                    }
                });
    }

    private void handleThrowable(
            Throwable err, Peer peer, NetworkMessage req, long stopTime, CompletableFuture<? extends NetworkMessage> fut
    ) {
        if (recoverable(err)) {
            LOG.warn(
                    "Recoverable error during the request type={} occurred (will be retried on the randomly selected node): ",
                    err, req.getClass().getSimpleName()
            );

            scheduleRetry(() -> sendWithRetry(randomNode(peer), req, stopTime, fut));
        } else {
            fut.completeExceptionally(err);
        }
    }

    private void handleErrorResponse(
            ErrorResponse resp, Peer peer, NetworkMessage req, long stopTime, CompletableFuture<? extends NetworkMessage> fut
    ) {
        RaftError error = RaftError.forNumber(resp.errorCode());

        switch (error) {
            case SUCCESS:
                leader = peer; // The OK response was received from a leader.

                fut.complete(null); // Void response.

                break;

            case EBUSY:
            case EAGAIN:
                scheduleRetry(() -> sendWithRetry(peer, req, stopTime, fut));

                break;

            case ENOENT:
                scheduleRetry(() -> {
                    // If changing peers or requesting a leader and something is not found
                    // probably target peer is doing rebalancing, try another peer.
                    if (req instanceof GetLeaderRequest || req instanceof ChangePeersAsyncRequest) {
                        sendWithRetry(randomNode(peer), req, stopTime, fut);
                    } else {
                        sendWithRetry(peer, req, stopTime, fut);
                    }
                });

                break;

            case EPERM:
                // TODO: IGNITE-15706
            case UNKNOWN:
            case EINTERNAL:
                if (resp.leaderId() == null) {
                    scheduleRetry(() -> sendWithRetry(randomNode(peer), req, stopTime, fut));
                } else {
                    leader = parsePeer(resp.leaderId()); // Update a leader.

                    scheduleRetry(() -> sendWithRetry(leader, req, stopTime, fut));
                }

                break;

            default:
                fut.completeExceptionally(new RaftException(error, resp.errorMsg()));

                break;
        }
    }

    private static void handleSmErrorResponse(SMErrorResponse resp, CompletableFuture<? extends NetworkMessage> fut) {
        SMThrowable th = resp.error();

        if (th instanceof SMCompactedThrowable) {
            SMCompactedThrowable compactedThrowable = (SMCompactedThrowable) th;

            try {
                Throwable restoredTh = (Throwable) Class.forName(compactedThrowable.throwableClassName())
                        .getConstructor(String.class)
                        .newInstance(compactedThrowable.throwableMessage());

                fut.completeExceptionally(restoredTh);
            } catch (Exception e) {
                LOG.warn("Cannot restore throwable from user's state machine. "
                        + "Check if throwable " + compactedThrowable.throwableClassName()
                        + " is present in the classpath.");

                fut.completeExceptionally(new IgniteException(compactedThrowable.throwableMessage()));
            }
        } else if (th instanceof SMFullThrowable) {
            fut.completeExceptionally(((SMFullThrowable) th).throwable());
        }
    }

    private void scheduleRetry(Runnable runnable) {
        executor.schedule(runnable, retryDelay, TimeUnit.MILLISECONDS);
    }

    /**
     * Checks if an error is recoverable, for example, {@link java.net.ConnectException}.
     *
     * @param t The throwable.
     * @return {@code True} if this is a recoverable exception.
     */
    private static boolean recoverable(Throwable t) {
        if (t instanceof ExecutionException || t instanceof CompletionException) {
            t = t.getCause();
        }

        return t instanceof TimeoutException || t instanceof IOException;
    }

    private Peer randomNode() {
        return randomNode(null);
    }

    /**
     * Returns a random peer. Tries 5 times finding a peer different from the excluded peer. If excluded peer is null, just returns a random
     * peer.
     *
     * @param excludedPeer Excluded peer.
     * @return Random peer.
     */
    private Peer randomNode(@Nullable Peer excludedPeer) {
        List<Peer> peers0 = peers;

        assert peers0 != null && !peers0.isEmpty();

        int lastPeerIndex = excludedPeer == null ? -1 : peers0.indexOf(excludedPeer);

        ThreadLocalRandom random = current();

        int newIdx = 0;

        for (int retries = 0; retries < 5; retries++) {
            newIdx = random.nextInt(peers0.size());

            if (newIdx != lastPeerIndex) {
                break;
            }
        }

        return peers0.get(newIdx);
    }

    /**
     * Parse {@link Peer} from string representation of {@link PeerId}.
     *
     * @param peerId String representation of {@link PeerId}
     * @return Peer
     */
    // TODO: Remove after IGNITE-15506
    private static @Nullable Peer parsePeer(@Nullable String peerId) {
        PeerId id = PeerId.parsePeer(peerId);

        return id == null ? null : new Peer(id.getConsistentId());
    }

    /**
     * Parse list of {@link PeerId} from list with string representations.
     *
     * @param peers List of {@link PeerId} string representations.
     * @return List of {@link PeerId}
     */
    private static @Nullable List<Peer> parsePeerList(@Nullable Collection<String> peers) {
        if (peers == null) {
            return null;
        }

        List<Peer> res = new ArrayList<>(peers.size());

        for (String peer : peers) {
            res.add(parsePeer(peer));
        }

        return res;
    }

    private static String peerId(Peer peer) {
        return PeerId.fromPeer(peer).toString();
    }

    private static List<String> peerIds(Collection<Peer> peers) {
        return peers.stream().map(RaftGroupServiceImpl::peerId).collect(toList());
    }

    private CompletableFuture<ClusterNode> resolvePeer(Peer peer) {
        ClusterNode node = cluster.topologyService().getByConsistentId(peer.consistentId());

        if (node == null) {
            return CompletableFuture.failedFuture(new ConnectException("Peer " + peer.consistentId() + " is unavailable"));
        }

        return CompletableFuture.completedFuture(node);
    }
}
