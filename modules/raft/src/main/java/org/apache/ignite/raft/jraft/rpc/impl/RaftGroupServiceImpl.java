/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.raft.jraft.rpc.impl;

import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.ThreadLocalRandom.current;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.AddLearnersRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.AddPeerRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.AddPeerResponse;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.ActionResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAsyncRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAsyncResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.jetbrains.annotations.NotNull;

/**
 * The implementation of {@link RaftGroupService}
 */
public class RaftGroupServiceImpl implements RaftGroupService {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RaftGroupServiceImpl.class);

    /** */
    private volatile long timeout;

    /** Timeout for network calls. */
    private final long rpcTimeout;

    /** */
    private final String groupId;

    /** */
    private final RaftMessagesFactory factory;

    /** */
    private volatile Peer leader;

    /** */
    private volatile List<Peer> peers;

    /** */
    private volatile List<Peer> learners;

    /** */
    private final ClusterService cluster;

    /** */
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
     * @param peers Initial group configuration.
     * @param leader Group leader.
     * @param retryDelay Retry delay.
     * @param executor Executor for retrying requests.
     */
    private RaftGroupServiceImpl(
        String groupId,
        ClusterService cluster,
        RaftMessagesFactory factory,
        int timeout,
        int rpcTimeout,
        List<Peer> peers,
        Peer leader,
        long retryDelay,
        ScheduledExecutorService executor
    ) {
        this.cluster = requireNonNull(cluster);
        this.peers = requireNonNull(peers);
        this.learners = Collections.emptyList();
        this.factory = factory;
        this.timeout = timeout;
        this.rpcTimeout = rpcTimeout;
        this.groupId = groupId;
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
     * @param peers List of all peers.
     * @param getLeader {@code True} to get the group's leader upon service creation.
     * @param retryDelay Retry delay.
     * @param executor Executor for retrying requests.
     * @return Future representing pending completion of the operation.
     */
    public static CompletableFuture<RaftGroupService> start(
        String groupId,
        ClusterService cluster,
        RaftMessagesFactory factory,
        int timeout,
        int rpcTimeout,
        List<Peer> peers,
        boolean getLeader,
        long retryDelay,
        ScheduledExecutorService executor
    ) {
        var service = new RaftGroupServiceImpl(groupId, cluster, factory, timeout, rpcTimeout, peers, null, retryDelay, executor);

        if (!getLeader)
            return CompletableFuture.completedFuture(service);

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
     * @param peers List of all peers.
     * @param getLeader {@code True} to get the group's leader upon service creation.
     * @param retryDelay Retry delay.
     * @param executor Executor for retrying requests.
     * @return Future representing pending completion of the operation.
     */
    public static CompletableFuture<RaftGroupService> start(
        String groupId,
        ClusterService cluster,
        RaftMessagesFactory factory,
        int timeout,
        List<Peer> peers,
        boolean getLeader,
        long retryDelay,
        ScheduledExecutorService executor
    ) {
        return start(groupId, cluster, factory, timeout, timeout, peers, getLeader, retryDelay, executor);
    }

    /** {@inheritDoc} */
    @Override public @NotNull String groupId() {
        return groupId;
    }

    /** {@inheritDoc} */
    @Override public long timeout() {
        return timeout;
    }

    /** {@inheritDoc} */
    @Override public void timeout(long newTimeout) {
        this.timeout = newTimeout;
    }

    /** {@inheritDoc} */
    @Override public Peer leader() {
        return leader;
    }

    /** {@inheritDoc} */
    @Override public List<Peer> peers() {
        return peers;
    }

    /** {@inheritDoc} */
    @Override public List<Peer> learners() {
        return learners;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> refreshLeader() {
        GetLeaderRequest req = factory.getLeaderRequest().groupId(groupId).build();

        CompletableFuture<GetLeaderResponse> fut = new CompletableFuture<>();

        sendWithRetry(randomNode(), req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            leader = parsePeer(resp.leaderId());

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<IgniteBiTuple<Peer, Long>> refreshAndGetLeaderWithTerm() {
        GetLeaderRequest req = factory.getLeaderRequest().groupId(groupId).build();

        CompletableFuture<GetLeaderResponse> fut = new CompletableFuture<>();

        sendWithRetry(randomNode(), req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            Peer respLeader = parsePeer(resp.leaderId());

            leader = respLeader;

            return new IgniteBiTuple<>(respLeader, resp.currentTerm());
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> refreshMembers(boolean onlyAlive) {
        GetPeersRequest req = factory.getPeersRequest().onlyAlive(onlyAlive).groupId(groupId).build();

        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> refreshMembers(onlyAlive));

        CompletableFuture<GetPeersResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            peers = parsePeerList(resp.peersList());
            learners = parsePeerList(resp.learnersList());

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> addPeer(Peer peer) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> addPeer(peer));

        AddPeerRequest req = factory.addPeerRequest().groupId(groupId).peerId(PeerId.fromPeer(peer).toString()).build();

        CompletableFuture<AddPeerResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            this.peers = parsePeerList(resp.newPeersList());

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> removePeer(Peer peer) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> removePeer(peer));

        RemovePeerRequest req = factory.removePeerRequest().groupId(groupId).peerId(PeerId.fromPeer(peer).toString()).build();

        CompletableFuture<RemovePeerResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            this.peers = parsePeerList(resp.newPeersList());

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> changePeers(List<Peer> peers) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> changePeers(peers));

        List<String> peersToChange = peers.stream().map(p -> PeerId.fromPeer(p).toString())
            .collect(Collectors.toList());

        ChangePeersRequest req = factory.changePeersRequest().groupId(groupId)
            .newPeersList(peersToChange).build();

        CompletableFuture<ChangePeersResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            this.peers = parsePeerList(resp.newPeersList());

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> changePeersAsync(List<Peer> peers, long term) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader().thenCompose(res -> changePeersAsync(peers, term));
        }

        List<String> peersToChange = peers.stream().map(p -> PeerId.fromPeer(p).toString())
                .collect(Collectors.toList());

        ChangePeersAsyncRequest req = factory.changePeersAsyncRequest().groupId(groupId)
                .term(term)
                .newPeersList(peersToChange).build();

        CompletableFuture<ChangePeersAsyncResponse> fut = new CompletableFuture<>();

        LOG.info("Sending changePeersAsync request for group={} to peers={} with leader term={}",
                groupId, peers, term);

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.handle((resp, err) -> {
            // We expect that all raft related errors will be handled by sendWithRetry, means that
            // such responses will initiate a retrying of the original request.
            assert !(resp instanceof RpcRequests.ErrorResponse);

            if (err != null) {
                return CompletableFuture.<Void>failedFuture(err);
            }

            return CompletableFuture.<Void>completedFuture(null);
        }).thenCompose(Function.identity());
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> addLearners(List<Peer> learners) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> addLearners(learners));

        List<String> lrns = learners.stream().map(p -> PeerId.fromPeer(p).toString()).collect(Collectors.toList());
        AddLearnersRequest req = factory.addLearnersRequest().groupId(groupId).learnersList(lrns).build();

        CompletableFuture<LearnersOpResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            this.learners = parsePeerList(resp.newLearnersList());

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> removeLearners(List<Peer> learners) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> removeLearners(learners));

        List<String> lrns = learners.stream().map(p -> PeerId.fromPeer(p).toString()).collect(Collectors.toList());
        RemoveLearnersRequest req = factory.removeLearnersRequest().groupId(groupId).learnersList(lrns).build();

        CompletableFuture<LearnersOpResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            this.learners = parsePeerList(resp.newLearnersList());

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> resetLearners(List<Peer> learners) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> resetLearners(learners));

        List<String> lrns = learners.stream().map(p -> PeerId.fromPeer(p).toString()).collect(Collectors.toList());
        ResetLearnersRequest req = factory.resetLearnersRequest().groupId(groupId).learnersList(lrns).build();

        CompletableFuture<LearnersOpResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            this.learners = parsePeerList(resp.newLearnersList());

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> snapshot(Peer peer) {
        SnapshotRequest req = factory.snapshotRequest().groupId(groupId).build();

        // Disable the timeout for a snapshot request.
        CompletableFuture<NetworkMessage> fut = cluster.messagingService().invoke(peer.address(), req, Integer.MAX_VALUE);

        return fut.thenCompose(resp -> {
            if (resp != null) {
                RpcRequests.ErrorResponse resp0 = (RpcRequests.ErrorResponse) resp;

                if (resp0.errorCode() != RaftError.SUCCESS.getNumber())
                    return CompletableFuture.failedFuture(new RaftException(RaftError.forNumber(resp0.errorCode()), resp0.errorMsg()));
            }

            return CompletableFuture.completedFuture(null);
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> transferLeadership(Peer newLeader) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> transferLeadership(newLeader));

        TransferLeaderRequest req = factory.transferLeaderRequest()
                .groupId(groupId)
                .leaderId(PeerId.fromPeer(leader).toString())
                .peerId(PeerId.fromPeer(newLeader).toString())
                .build();

        CompletableFuture<NetworkMessage> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenRun(() -> {
            this.leader = newLeader;
        });
    }

    /** {@inheritDoc} */
    @Override public <R> CompletableFuture<R> run(Command cmd) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> run(cmd));

        ActionRequest req = factory.actionRequest().command(cmd).groupId(groupId).readOnlySafe(true).build();

        CompletableFuture<ActionResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> (R) resp.result());
    }

    /**
     * {@inheritDoc}
     */
    @Override public <R> CompletableFuture<R> run(Peer peer, ReadCommand cmd) {
        ActionRequest req = factory.actionRequest().command(cmd).groupId(groupId).readOnlySafe(false).build();

        return cluster.messagingService().invoke(peer.address(), req, rpcTimeout)
                .thenApply(resp -> (R) ((ActionResponse) resp).result());
    }

    /** {@inheritDoc} */
    @Override public void shutdown() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public ClusterService clusterService() {
        return cluster;
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
    private <R> void sendWithRetry(Peer peer, Object req, long stopTime, CompletableFuture<R> fut) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("sendWithRetry peers={} req={} from={} to={}",
                    peers,
                    S.toString(req),
                    cluster.topologyService().localMember().address(),
                    peer.address());
        }

        if (currentTimeMillis() >= stopTime) {
            fut.completeExceptionally(new TimeoutException());

            return;
        }

        CompletableFuture<?> fut0 = cluster.messagingService().invoke(peer.address(), (NetworkMessage) req, rpcTimeout);

        //TODO: IGNITE-15389 org.apache.ignite.internal.metastorage.client.CursorImpl has potential deadlock inside
        fut0.whenCompleteAsync(new BiConsumer<Object, Throwable>() {
            @Override public void accept(Object resp, Throwable err) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("sendWithRetry resp={} from={} to={} err={}",
                            S.toString(resp),
                            cluster.topologyService().localMember().address(),
                            peer.address(),
                            err == null ? null : err.getMessage());
                }

                if (err != null) {
                    if (recoverable(err)) {
                        executor.schedule(() -> {
                            LOG.warn("Recoverable error during the request type={} occurred (will be retried on the randomly selected node): ",
                                    err, req.getClass().getSimpleName());

                            sendWithRetry(randomNode(), req, stopTime, fut);

                            return null;
                        }, retryDelay, TimeUnit.MILLISECONDS);
                    }
                    else {
                        fut.completeExceptionally(err);
                    }
                }
                else if (resp instanceof RpcRequests.ErrorResponse) {
                    RpcRequests.ErrorResponse resp0 = (RpcRequests.ErrorResponse) resp;

                    if (resp0.errorCode() == RaftError.SUCCESS.getNumber()) { // Handle OK response.
                        leader = peer; // The OK response was received from a leader.

                        fut.complete(null); // Void response.
                    }
                    else if (resp0.errorCode() == RaftError.EBUSY.getNumber() ||
                        resp0.errorCode() == (RaftError.EAGAIN.getNumber()) ||
                        resp0.errorCode() == (RaftError.ENOENT.getNumber())) { // Possibly a node has not been started.
                        executor.schedule(() -> {
                            sendWithRetry(peer, req, stopTime, fut);

                            return null;
                        }, retryDelay, TimeUnit.MILLISECONDS);
                    }
                    else if (resp0.errorCode() == RaftError.EPERM.getNumber() ||
                        // TODO: IGNITE-15706
                        resp0.errorCode() == RaftError.UNKNOWN.getNumber() ||
                        resp0.errorCode() == RaftError.EINTERNAL.getNumber()) {
                        if (resp0.leaderId() == null) {
                            executor.schedule(() -> {
                                sendWithRetry(randomNode(), req, stopTime, fut);

                                return null;
                            }, retryDelay, TimeUnit.MILLISECONDS);
                        }
                        else {
                            leader = parsePeer(resp0.leaderId()); // Update a leader.

                            executor.schedule(() -> {
                                sendWithRetry(leader, req, stopTime, fut);

                                return null;
                            }, retryDelay, TimeUnit.MILLISECONDS);
                        }
                    }
                    else {
                        fut.completeExceptionally(
                            new RaftException(RaftError.forNumber(resp0.errorCode()), resp0.errorMsg()));
                    }
                }
                else if (resp instanceof RpcRequests.SMErrorResponse) {
                    SMThrowable th = ((RpcRequests.SMErrorResponse)resp).error();
                    if (th instanceof SMCompactedThrowable) {
                        SMCompactedThrowable compactedThrowable = (SMCompactedThrowable)th;

                        try {
                            Throwable restoredTh = (Throwable)Class.forName(compactedThrowable.throwableClassName())
                                .getConstructor(String.class)
                                .newInstance(compactedThrowable.throwableMessage());

                            fut.completeExceptionally(restoredTh);
                        }
                        catch (Exception e) {
                            LOG.warn("Cannot restore throwable from user's state machine. " +
                                "Check if throwable " + compactedThrowable.throwableClassName() +
                                " is presented in the classpath.");

                            fut.completeExceptionally(new IgniteException(compactedThrowable.throwableMessage()));
                        }
                    }
                    else if (th instanceof SMFullThrowable)
                        fut.completeExceptionally(((SMFullThrowable)th).throwable());
                }
                else {
                    leader = peer; // The OK response was received from a leader.

                    fut.complete((R) resp);
                }
            }
        });
    }

    /**
     * Checks if an error is recoverable, for example, {@link java.net.ConnectException}.
     * @param t The throwable.
     * @return {@code True} if this is a recoverable exception.
     */
    private static boolean recoverable(Throwable t) {
        if (t instanceof ExecutionException || t instanceof CompletionException) {
            t = t.getCause();
        }

        return t instanceof TimeoutException || t instanceof IOException;
    }

    /**
     * @return Random node.
     */
    private Peer randomNode() {
        List<Peer> peers0 = peers;

        assert peers0 != null && !peers0.isEmpty();

        return peers0.get(current().nextInt(peers0.size()));
    }

    /**
     * Parse {@link Peer} from string representation of {@link PeerId}.
     *
     * @param peerId String representation of {@link PeerId}
     * @return Peer
     */
    // TODO: Remove after IGNITE-15506
    private static Peer parsePeer(String peerId) {
        return peerFromPeerId(PeerId.parsePeer(peerId));
    }

    /**
     * Creates new {@link Peer} from {@link PeerId}.
     *
     * @param peer PeerId
     * @return {@link Peer}
     */
    private static Peer peerFromPeerId(PeerId peer) {
        if (peer == null)
            return null;
        else
            return new Peer(NetworkAddress.from(peer.getEndpoint().getIp() + ":" + peer.getEndpoint().getPort()));
    }

    /**
     * Parse list of {@link PeerId} from list with string representations.
     *
     * @param peers List of {@link PeerId} string representations.
     * @return List of {@link PeerId}
     */
    private List<Peer> parsePeerList(Collection<String> peers) {
        if (peers == null)
            return null;

        List<Peer> res = new ArrayList<>(peers.size());

        for (String peer: peers)
            res.add(parsePeer(peer));

        return res;
    }
}
