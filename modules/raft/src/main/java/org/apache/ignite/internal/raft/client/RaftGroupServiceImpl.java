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

package org.apache.ignite.internal.raft.client;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.tostring.IgniteToStringBuilder.includeSensitive;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.AddLearnersRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.AddPeerRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.AddPeerResponse;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAndLearnersAsyncRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAndLearnersAsyncResponse;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAndLearnersRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.GetLeaderRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.GetLeaderResponse;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.GetPeersRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.GetPeersResponse;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.LearnersOpResponse;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.RemoveLearnersRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.RemovePeerRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.RemovePeerResponse;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.ResetLearnersRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.TransferLeaderRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.network.TopologyEventHandler;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.ExceptionFactory;
import org.apache.ignite.internal.raft.GroupOverloadedException;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeerUnavailableException;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.StoppingExceptionFactories;
import org.apache.ignite.internal.raft.ThrottlingContextHolder;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.rebalance.RaftStaleUpdateException;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.ActionResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAndLearnersResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ErrorResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ReadIndexResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.SMErrorResponse;
import org.apache.ignite.raft.jraft.rpc.impl.RaftException;
import org.apache.ignite.raft.jraft.rpc.impl.SMCompactedThrowable;
import org.apache.ignite.raft.jraft.rpc.impl.SMFullThrowable;
import org.apache.ignite.raft.jraft.rpc.impl.SMThrowable;
import org.jetbrains.annotations.Nullable;

/**
 * The implementation of {@link RaftGroupService}.
 */
// TODO: IGNITE-20738 Methods updateConfiguration/refreshMembers/*Peer/*Learner are not thread-safe
//  and can produce meaningless (peers, learners) pairs as a result.
public class RaftGroupServiceImpl implements RaftGroupService {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RaftGroupServiceImpl.class);

    private final String groupId;

    private final ReplicationGroupId realGroupId;

    private final RaftMessagesFactory factory;

    private final RaftConfiguration configuration;

    @Nullable
    private volatile Peer leader;

    private volatile List<Peer> peers;

    private volatile List<Peer> learners;

    private final ClusterService cluster;

    /** Executor for scheduling retries of {@link RaftGroupServiceImpl#sendWithRetry} invocations. */
    private final ScheduledExecutorService executor;

    private final Marshaller commandsMarshaller;

    private final ExceptionFactory stoppingExceptionFactory;

    /** This flag is used only for logging. */
    private final AtomicBoolean peersAreUnavailable = new AtomicBoolean();

    /** Busy lock. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private static final Supplier<String> NO_DESCRIPTION = () -> null;

    private final ThrottlingContextHolder throttlingContextHolder;

    private final TopologyEventHandler topologyEventHandler;

    // TODO: https://issues.apache.org/jira/browse/IGNITE-26085 Remove, tmp hack
    private volatile boolean stopping;

    /**
     * Constructor.
     *
     * @param groupId Group id.
     * @param cluster A cluster.
     * @param factory A message factory.
     * @param configuration Raft configuration.
     * @param membersConfiguration Raft members configuration.
     * @param leader Group leader.
     * @param executor Executor for retrying requests.
     * @param commandsMarshaller Marshaller that should be used to serialize/deserialize commands.
     */
    private RaftGroupServiceImpl(
            ReplicationGroupId groupId,
            ClusterService cluster,
            RaftMessagesFactory factory,
            RaftConfiguration configuration,
            PeersAndLearners membersConfiguration,
            @Nullable Peer leader,
            ScheduledExecutorService executor,
            Marshaller commandsMarshaller,
            ExceptionFactory stoppingExceptionFactory,
            ThrottlingContextHolder throttlingContextHolder
    ) {
        this.cluster = cluster;
        this.configuration = configuration;
        this.peers = List.copyOf(membersConfiguration.peers());
        this.learners = List.copyOf(membersConfiguration.learners());
        this.factory = factory;
        this.groupId = groupId.toString();
        this.realGroupId = groupId;
        this.leader = leader;
        this.executor = executor;
        this.commandsMarshaller = commandsMarshaller;
        this.stoppingExceptionFactory = stoppingExceptionFactory;
        this.throttlingContextHolder = throttlingContextHolder;
        this.topologyEventHandler = topologyEventHandler();
        this.stopping = false;
    }

    /**
     * Starts raft group service.
     *
     * @param groupId Raft group id.
     * @param cluster Cluster service.
     * @param factory Message factory.
     * @param configuration Raft configuration.
     * @param membersConfiguration Raft members configuration.
     * @param executor Executor for retrying requests.
     * @return A new Raft group service.
     */
    public static RaftGroupService start(
            ReplicationGroupId groupId,
            ClusterService cluster,
            RaftMessagesFactory factory,
            RaftConfiguration configuration,
            PeersAndLearners membersConfiguration,
            ScheduledExecutorService executor,
            Marshaller commandsMarshaller,
            ThrottlingContextHolder throttlingContextHolder
    ) {
        return start(
                groupId,
                cluster,
                factory,
                configuration,
                membersConfiguration,
                executor,
                commandsMarshaller,
                StoppingExceptionFactories.indicateComponentStop(),
                throttlingContextHolder
        );
    }

    /**
     * Starts raft group service.
     *
     * @param groupId Raft group id.
     * @param cluster Cluster service.
     * @param factory Message factory.
     * @param configuration Raft configuration.
     * @param membersConfiguration Raft members configuration.
     * @param executor Executor for retrying requests
     * @param stoppingExceptionFactory Exception factory used to create exceptions thrown to indicate that the object is being stopped.
     * @return A new Raft group service.
     */
    public static RaftGroupService start(
            ReplicationGroupId groupId,
            ClusterService cluster,
            RaftMessagesFactory factory,
            RaftConfiguration configuration,
            PeersAndLearners membersConfiguration,
            ScheduledExecutorService executor,
            Marshaller commandsMarshaller,
            ExceptionFactory stoppingExceptionFactory,
            ThrottlingContextHolder throttlingContextHolder
    ) {
        boolean inBenchmark = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_SKIP_REPLICATION_IN_BENCHMARK);

        RaftGroupServiceImpl service;
        if (inBenchmark) {
            service = new RaftGroupServiceImpl(
                    groupId,
                    cluster,
                    factory,
                    configuration,
                    membersConfiguration,
                    null,
                    executor,
                    commandsMarshaller,
                    stoppingExceptionFactory,
                    throttlingContextHolder
            ) {
                @Override
                public <R> CompletableFuture<R> run(Command cmd) {
                    return cmd.getClass().getSimpleName().contains("UpdateCommand") ? nullCompletedFuture() : super.run(cmd);
                }
            };
        } else {
            service = new RaftGroupServiceImpl(
                    groupId,
                    cluster,
                    factory,
                    configuration,
                    membersConfiguration,
                    null,
                    executor,
                    commandsMarshaller,
                    stoppingExceptionFactory,
                    throttlingContextHolder
            );
        }

        return service;
    }

    @Override
    public ReplicationGroupId groupId() {
        return realGroupId;
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
        return refreshLeader(NO_DESCRIPTION);
    }

    private CompletableFuture<Void> refreshLeader(Supplier<String> originDescription) {
        return refreshLeader(defaultTimeout(), originDescription);
    }

    private CompletableFuture<Void> refreshLeader(long timeout, Supplier<String> originDescription) {
        Function<Peer, GetLeaderRequest> requestFactory = targetPeer -> factory.getLeaderRequest()
                .peerId(peerId(targetPeer))
                .groupId(groupId)
                .build();

        return this.<GetLeaderResponse>sendWithRetry(randomNode(), timeout, originDescription, requestFactory, false)
                .thenAccept(resp -> this.leader = parsePeer(resp.leaderId()));
    }

    @Override
    public CompletableFuture<LeaderWithTerm> refreshAndGetLeaderWithTerm() {
        Function<Peer, GetLeaderRequest> requestFactory = targetPeer -> factory.getLeaderRequest()
                .peerId(peerId(targetPeer))
                .groupId(groupId)
                .build();

        return this.<GetLeaderResponse>sendWithRetry(randomNode(), requestFactory, false)
                .thenApply(resp -> {
                    if (resp.leaderId() == null) {
                        return LeaderWithTerm.NO_LEADER;
                    }

                    Peer respLeader = parsePeer(resp.leaderId());

                    this.leader = respLeader;

                    return new LeaderWithTerm(respLeader, resp.currentTerm());
                });
    }

    @Override
    public CompletableFuture<Void> refreshMembers(boolean onlyAlive) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader(() -> "refreshMembers").thenCompose(res -> refreshMembers(onlyAlive));
        }

        Function<Peer, GetPeersRequest> requestFactory = targetPeer -> factory.getPeersRequest()
                .leaderId(peerId(targetPeer))
                .onlyAlive(onlyAlive)
                .groupId(groupId)
                .build();

        return this.<GetPeersResponse>sendWithRetry(leader, requestFactory, false)
                .thenAccept(resp -> {
                    this.peers = parsePeerList(resp.peersList());
                    this.learners = parsePeerList(resp.learnersList());
                });
    }

    @Override
    public CompletableFuture<Void> addPeer(Peer peer, long sequenceToken) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader(() -> "addPeer").thenCompose(res -> addPeer(peer, sequenceToken));
        }

        Function<Peer, AddPeerRequest> requestFactory = targetPeer -> factory.addPeerRequest()
                .leaderId(peerId(targetPeer))
                .groupId(groupId)
                .peerId(peerId(peer))
                .sequenceToken(sequenceToken)
                .build();

        return this.<AddPeerResponse>sendWithRetry(leader, requestFactory, true)
                .thenAccept(resp -> this.peers = parsePeerList(resp.newPeersList()));
    }

    @Override
    public CompletableFuture<Void> removePeer(Peer peer, long sequenceToken) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader(() -> "removePeer").thenCompose(res -> removePeer(peer, sequenceToken));
        }

        Function<Peer, RemovePeerRequest> requestFactory = targetPeer -> factory.removePeerRequest()
                .leaderId(peerId(targetPeer))
                .groupId(groupId)
                .peerId(peerId(peer))
                .sequenceToken(sequenceToken)
                .build();

        return this.<RemovePeerResponse>sendWithRetry(leader, requestFactory, false)
                .thenAccept(resp -> this.peers = parsePeerList(resp.newPeersList()));
    }

    @Override
    public CompletableFuture<Void> changePeersAndLearners(PeersAndLearners peersAndLearners, long term, long sequenceToken) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader(() -> "changePeersAndLearners")
                    .thenCompose(res -> changePeersAndLearners(peersAndLearners, term, sequenceToken));
        }

        Function<Peer, ChangePeersAndLearnersRequest> requestFactory = targetPeer -> factory.changePeersAndLearnersRequest()
                .leaderId(peerId(targetPeer))
                .groupId(groupId)
                .newPeersList(peerIds(peersAndLearners.peers()))
                .newLearnersList(peerIds(peersAndLearners.learners()))
                .term(term)
                .sequenceToken(sequenceToken)
                .build();

        LOG.info("Sending changePeersAndLearners request for group={} to peers={} and learners={} with leader term={}",
                groupId, peersAndLearners.peers(), peersAndLearners.learners(), term);

        return this.<ChangePeersAndLearnersResponse>sendWithRetry(leader, requestFactory, false)
                .thenAccept(resp -> {
                    this.peers = parsePeerList(resp.newPeersList());
                    this.learners = parsePeerList(resp.newLearnersList());
                });
    }

    @Override
    public CompletableFuture<Void> changePeersAndLearnersAsync(PeersAndLearners peersAndLearners, long term, long sequenceToken) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader(() -> "changePeersAndLearnersAsync")
                    .thenCompose(res -> changePeersAndLearnersAsync(peersAndLearners, term, sequenceToken));
        }

        Function<Peer, ChangePeersAndLearnersAsyncRequest> requestFactory = targetPeer -> factory.changePeersAndLearnersAsyncRequest()
                .leaderId(peerId(targetPeer))
                .groupId(groupId)
                .term(term)
                .newPeersList(peerIds(peersAndLearners.peers()))
                .newLearnersList(peerIds(peersAndLearners.learners()))
                .sequenceToken(sequenceToken)
                .build();

        LOG.info("Sending changePeersAndLearnersAsync request for group={} to peers={} and learners={} with leader term={}",
                groupId, peersAndLearners.peers(), peersAndLearners.learners(), term);

        return this.<ChangePeersAndLearnersAsyncResponse>sendWithRetry(leader, requestFactory, false)
                .thenAccept(resp -> {
                    // We expect that all raft related errors will be handled by sendWithRetry, means that
                    // such responses will initiate a retrying of the original request.
                    assert !(resp instanceof RpcRequests.ErrorResponse);
                });
    }

    @Override
    public CompletableFuture<Void> addLearners(Collection<Peer> learners, long sequenceToken) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader(() -> "addLearners").thenCompose(res -> addLearners(learners, sequenceToken));
        }

        Function<Peer, AddLearnersRequest> requestFactory = targetPeer -> factory.addLearnersRequest()
                .leaderId(peerId(targetPeer))
                .groupId(groupId)
                .learnersList(peerIds(learners))
                .sequenceToken(sequenceToken)
                .build();

        return this.<LearnersOpResponse>sendWithRetry(leader, requestFactory, false)
                .thenAccept(resp -> this.learners = parsePeerList(resp.newLearnersList()));
    }

    @Override
    public CompletableFuture<Void> removeLearners(Collection<Peer> learners, long sequenceToken) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader(() -> "removeLearners").thenCompose(res -> removeLearners(learners, sequenceToken));
        }

        Function<Peer, RemoveLearnersRequest> requestFactory = targetPeer -> factory.removeLearnersRequest()
                .leaderId(peerId(targetPeer))
                .groupId(groupId)
                .learnersList(peerIds(learners))
                .sequenceToken(sequenceToken)
                .build();

        return this.<LearnersOpResponse>sendWithRetry(leader, requestFactory, false)
                .thenAccept(resp -> this.learners = parsePeerList(resp.newLearnersList()));
    }

    @Override
    public CompletableFuture<Void> resetLearners(Collection<Peer> learners, long sequenceToken) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader(() -> "resetLearners").thenCompose(res -> resetLearners(learners, sequenceToken));
        }

        Function<Peer, ResetLearnersRequest> requestFactory = targetPeer -> factory.resetLearnersRequest()
                .leaderId(peerId(targetPeer))
                .groupId(groupId)
                .learnersList(peerIds(learners))
                .sequenceToken(sequenceToken)
                .build();

        return this.<LearnersOpResponse>sendWithRetry(leader, requestFactory, false)
                .thenAccept(resp -> this.learners = parsePeerList(resp.newLearnersList()));
    }

    @Override
    public CompletableFuture<Void> snapshot(Peer peer, boolean forced) {
        Function<Peer, ? extends NetworkMessage> requestFactory = (peer1) -> factory.snapshotRequest()
                .peerId(peerId(peer))
                .groupId(groupId)
                .forced(forced)
                .build();

        return sendWithRetry(peer, -1, Long.MAX_VALUE, NO_DESCRIPTION, requestFactory, false)
                .thenAccept(resp -> {});
    }

    @Override
    public CompletableFuture<Void> transferLeadership(Peer newLeader) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader(() -> "transferLeadership").thenCompose(res -> transferLeadership(newLeader));
        }

        Function<Peer, TransferLeaderRequest> requestFactory = targetPeer -> factory.transferLeaderRequest()
                .groupId(groupId)
                .leaderId(peerId(targetPeer))
                .peerId(peerId(newLeader))
                .build();

        return sendWithRetry(leader, requestFactory, false)
                .thenRun(() -> this.leader = newLeader);
    }

    @Override
    public <R> CompletableFuture<R> run(Command cmd) {
        return run(cmd, defaultTimeout());
    }

    @Override
    public <R> CompletableFuture<R> run(Command cmd, long timeoutMillis) {
        Peer leader = this.leader;

        if (leader == null) {
            return refreshLeader(timeoutMillis, cmd::toStringForLightLogging).thenCompose(res -> run(cmd));
        }

        Function<Peer, ActionRequest> requestFactory;

        if (cmd instanceof WriteCommand) {
            requestFactory = targetPeer -> factory.writeActionRequest()
                    .groupId(groupId)
                    .command(commandsMarshaller.marshall(cmd))
                    // Having prepared deserialized command makes its handling more efficient in the state machine.
                    // This saves us from extra-deserialization on a local machine, which would take precious time to do.
                    .deserializedCommand((WriteCommand) cmd)
                    .build();
        } else {
            requestFactory = targetPeer -> factory.readActionRequest()
                    .groupId(groupId)
                    .command((ReadCommand) cmd)
                    .readOnlySafe(true)
                    .build();
        }

        return this.<ActionResponse>sendWithRetry(leader, timeoutMillis, NO_DESCRIPTION, requestFactory, true)
                .thenApply(resp -> (R) resp.result());
    }

    // TODO: IGNITE-18636 Shutdown raft services on components' stop.
    @Override
    public void shutdown() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        clusterService().topologyService().removeEventHandler(topologyEventHandler);
    }

    @Override
    public CompletableFuture<Long> readIndex() {
        Function<Peer, ? extends NetworkMessage> requestFactory = p -> factory.readIndexRequest()
                .groupId(groupId)
                .peerId(p.consistentId())
                .serverId(p.consistentId())
                .build();

        Peer leader = leader();
        Peer node = leader == null ? randomNode() : leader;
        return this.<ReadIndexResponse>sendWithRetry(node, requestFactory, false)
                .thenApply(ReadIndexResponse::index);
    }

    @Override
    public ClusterService clusterService() {
        return cluster;
    }

    @Override
    public void updateConfiguration(PeersAndLearners configuration) {
        peers = List.copyOf(configuration.peers());
        learners = List.copyOf(configuration.learners());
        leader = null;
    }

    private long defaultTimeout() {
        return configuration.retryTimeoutMillis().value();
    }

    private <R extends NetworkMessage> CompletableFuture<R> sendWithRetry(
            Peer peer,
            Function<Peer, ? extends NetworkMessage> requestFactory,
            boolean throttleOnOverload
    ) {
        return sendWithRetry(peer, defaultTimeout(), NO_DESCRIPTION, requestFactory, throttleOnOverload);
    }

    private <R extends NetworkMessage> CompletableFuture<R> sendWithRetry(
            Peer peer,
            long timeoutMillis,
            Supplier<String> originDescription,
            Function<Peer, ? extends NetworkMessage> requestFactory,
            boolean throttleOnOverload
    ) {
        return sendWithRetry(peer, timeoutMillis, -1, originDescription, requestFactory, throttleOnOverload);
    }

    /**
     * Sends a request with retry until success or reaches a timeout.
     *
     * @param peer Target peer to which the request will be sent.
     * @param sendWithRetryTimeoutMillis Timeout for entire request sending (with retries) in milliseconds, a negative value means no
     *      timeout.
     * @param singleRequestTimeoutMillis Timeout for sending a single request in milliseconds, {@code -1} if there is no timeout.
     * @param originDescription Origin request description supplier for logging purposes.
     * @param requestFactory Factory for creating requests to the target peer.
     * @param throttleOnOverload Whether to throttle the request if the target peer is overloaded.
     * @param <R> Response type.
     * @return Future representing the result of the request.
     */
    private <R extends NetworkMessage> CompletableFuture<R> sendWithRetry(
            Peer peer,
            long sendWithRetryTimeoutMillis,
            long singleRequestTimeoutMillis,
            Supplier<String> originDescription,
            Function<Peer, ? extends NetworkMessage> requestFactory,
            boolean throttleOnOverload
    ) {
        var future = new CompletableFuture<R>();

        if (!busyLock.enterBusy()) {
            future.completeExceptionally(stoppingExceptionFactory.create("Raft client is stopping [" + groupId + "]."));

            return future;
        }

        try {
            ThrottlingContextHolder peerThrottlingContextHolder = throttlingContextHolder.peerContextHolder(peer.consistentId());

            if (throttleOnOverload && peerThrottlingContextHolder.isOverloaded()) {
                executor.schedule(
                        () -> future.completeExceptionally(new GroupOverloadedException(groupId, peer)),
                        100,
                        TimeUnit.MILLISECONDS
                );

                return future;
            }

            long stopTime = sendWithRetryTimeoutMillis >= 0 ? currentTimeMillis() + sendWithRetryTimeoutMillis : Long.MAX_VALUE;
            var context = new RetryContext(groupId, peer, originDescription, requestFactory, stopTime, singleRequestTimeoutMillis);

            sendWithRetry(future, context, peerThrottlingContextHolder);

            return future;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Retries a request until success or timeout.
     *
     * @param fut Result future.
     * @param retryContext Context.
     * @param <R> Response type.
     */
    private <R extends NetworkMessage> void sendWithRetry(CompletableFuture<R> fut, RetryContext retryContext) {
        ThrottlingContextHolder peerThrottlingContextHolder = throttlingContextHolder
                .peerContextHolder(retryContext.targetPeer().consistentId());

        sendWithRetry(fut, retryContext, peerThrottlingContextHolder);
    }

    /**
     * Retries a request until success or timeout.
     *
     * @param fut Result future.
     * @param retryContext Context.
     * @param peerThrottlingContextHolder Throttling context holder for the given peer.
     * @param <R> Response type.
     */
    private <R extends NetworkMessage> void sendWithRetry(
            CompletableFuture<R> fut,
            RetryContext retryContext,
            ThrottlingContextHolder peerThrottlingContextHolder
    ) {
        if (!busyLock.enterBusy()) {
            fut.completeExceptionally(stoppingExceptionFactory.create("Raft client is stopping [" + groupId + "]."));

            return;
        }

        try {
            long requestStartTime = currentTimeMillis();

            if (requestStartTime >= retryContext.stopTime()) {
                // TODO: https://issues.apache.org/jira/browse/IGNITE-26085 Remove, tmp hack
                if (stopping) {
                    fut.completeExceptionally(new NodeStoppingException());
                    return;
                }

                fut.completeExceptionally(retryContext.createTimeoutException());
                return;
            }

            peerThrottlingContextHolder.beforeRequest();

            long responseTimeout = retryContext.responseTimeoutMillis() == -1
                    ? peerThrottlingContextHolder.peerRequestTimeoutMillis() : retryContext.responseTimeoutMillis();

            resolvePeer(retryContext.targetPeer())
                    .thenCompose(node -> {
                        // TODO: https://issues.apache.org/jira/browse/IGNITE-26085 Remove, tmp hack
                        if (stopping) {
                            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
                        }

                        return cluster.messagingService()
                                .invoke(node, retryContext.request(), responseTimeout);
                    })
                    .whenComplete((resp, err) -> {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("sendWithRetry req={} resp={} from={} to={} err={}",
                                    retryContext.request(),
                                    resp,
                                    cluster.topologyService().localMember().address(),
                                    retryContext.targetPeer().consistentId(),
                                    err == null ? null : err.getMessage());
                        }

                        peerThrottlingContextHolder.afterRequest(requestStartTime, retriableError(err, resp));

                        if (!busyLock.enterBusy()) {
                            fut.completeExceptionally(stoppingExceptionFactory.create("Raft client is stopping [" + groupId + "]."));

                            return;
                        }

                        try {
                            if (err != null) {
                                handleThrowable(fut, err, retryContext);
                            } else if (resp instanceof ErrorResponse) {
                                handleErrorResponse(fut, (ErrorResponse) resp, retryContext);
                            } else if (resp instanceof SMErrorResponse) {
                                handleSmErrorResponse(fut, (SMErrorResponse) resp, retryContext);
                            } else {
                                leader = retryContext.targetPeer(); // The OK response was received from a leader.

                                fut.complete((R) resp);
                            }
                        } catch (Throwable e) {
                            fut.completeExceptionally(e);
                        } finally {
                            busyLock.leaveBusy();
                        }
                    });
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void handleThrowable(CompletableFuture<? extends NetworkMessage> fut, Throwable err, RetryContext retryContext) {
        err = unwrapCause(err);

        if (!recoverable(err)) {
            fut.completeExceptionally(err);

            return;
        }

        Peer randomPeer = randomNode(retryContext);

        if (LOG.isDebugEnabled()) {
            String msg;

            if (err instanceof TimeoutException) {
                msg = "Recoverable TimeoutException during the request occurred (will be retried on a randomly selected node) "
                        + "[request={}, peer={}, newPeer={}, traceId={}].";
            } else {
                msg = "Recoverable error during the request occurred (will be retried on a randomly selected node) "
                        + "[request={}, peer={}, newPeer={}, traceId={}].";
            }

            LOG.debug(
                    msg,
                    includeSensitive() ? retryContext.request() : retryContext.request().toStringForLightLogging(),
                    retryContext.targetPeer(),
                    randomPeer,
                    retryContext.errorTraceId()
            );
        }

        String shortReasonMessage = "Peer " + shortPeerString(retryContext.targetPeer()) + " threw " + err.getClass().getSimpleName();
        scheduleRetry(fut, retryContext.nextAttempt(randomPeer, shortReasonMessage));
    }

    private void handleErrorResponse(CompletableFuture<? extends NetworkMessage> fut, ErrorResponse resp, RetryContext retryContext) {
        RaftError error = RaftError.forNumber(resp.errorCode());

        switch (error) {
            case SUCCESS:
                leader = retryContext.targetPeer(); // The OK response was received from a leader.

                fut.complete(null); // Void response.

                break;

            case EBUSY:
            case EAGAIN:
                scheduleRetry(fut, retryContext.nextAttempt(retryContext.targetPeer(), getShortReasonMessage(retryContext, error, resp)));

                break;

            // TODO: IGNITE-15706
            case UNKNOWN:
            case EINTERNAL:
            case ENOENT: {
                NetworkMessage request = retryContext.request();

                Peer newTargetPeer;

                // If changing peers or requesting a leader and something is not found
                // probably target peer is doing rebalancing, try another peer.
                if (request instanceof GetLeaderRequest || request instanceof ChangePeersAndLearnersAsyncRequest) {
                    newTargetPeer = randomNode(retryContext);
                } else {
                    newTargetPeer = retryContext.targetPeer();
                }

                scheduleRetry(fut, retryContext.nextAttempt(newTargetPeer, getShortReasonMessage(retryContext, error, resp)));

                break;
            }

            case EHOSTDOWN:
            case ESHUTDOWN:
            case ENODESHUTDOWN:
            case ESTOP: {
                Peer newTargetPeer = randomNode(retryContext);

                scheduleRetry(fut, retryContext.nextAttemptForUnavailablePeer(
                        newTargetPeer,
                        getShortReasonMessage(retryContext, error, resp)
                ));

                break;
            }

            case EPERM: {
                Peer newTargetPeer;

                if (resp.leaderId() == null) {
                    newTargetPeer = randomNode(retryContext);
                } else {
                    newTargetPeer = parsePeer(resp.leaderId());

                    assert newTargetPeer != null;

                    leader = newTargetPeer;
                }

                scheduleRetry(fut, retryContext.nextAttempt(newTargetPeer, getShortReasonMessage(retryContext, error, resp)));

                break;
            }

            case ESTALE:
                fut.completeExceptionally(new RaftStaleUpdateException(resp.errorMsg()));

                break;

            default:
                fut.completeExceptionally(new RaftException(error, resp.errorMsg()));

                break;
        }
    }

    private static String shortPeerString(Peer peer) {
        return peer.consistentId() + ":" + peer.idx();
    }

    private static String getShortReasonMessage(RetryContext retryContext, RaftError error, ErrorResponse resp) {
        return format("Peer {} returned code {}: {}", shortPeerString(retryContext.targetPeer()), error, resp.errorMsg());
    }

    private static void handleSmErrorResponse(
            CompletableFuture<? extends NetworkMessage> fut, SMErrorResponse resp, RetryContext retryContext
    ) {
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

                fut.completeExceptionally(new IgniteInternalException(
                        retryContext.errorTraceId(), INTERNAL_ERR, compactedThrowable.throwableMessage()
                ));
            }
        } else if (th instanceof SMFullThrowable) {
            fut.completeExceptionally(((SMFullThrowable) th).throwable());
        } else {
            assert false : th;
        }
    }

    @Nullable
    private static Boolean retriableError(@Nullable Throwable e, NetworkMessage raftResponse) {
        int errorCode = raftResponse instanceof ErrorResponse ? ((ErrorResponse) raftResponse).errorCode() : 0;
        RaftError raftError = RaftError.forNumber(errorCode);

        if (raftError == RaftError.SUCCESS && e == null) {
            return null;
        }

        Throwable cause = e == null ? null : unwrapCause(e);
        if (cause instanceof TimeoutException) {
            return true;
        }

        return raftError == RaftError.EBUSY || raftError == RaftError.EAGAIN;
    }

    private void scheduleRetry(CompletableFuture<? extends NetworkMessage> fut, RetryContext retryContext) {
        executor.schedule(
                () -> {
                    retryContext.onNewAttempt();

                    sendWithRetry(fut, retryContext);
                },
                configuration.retryDelayMillis().value(),
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Checks if an error is recoverable.
     *
     * <p>An error is considered recoverable if it's an instance of {@link TimeoutException}, {@link IOException}
     * or {@link PeerUnavailableException}.
     *
     * @param t The throwable.
     * @return {@code True} if this is a recoverable exception.
     */
    private static boolean recoverable(Throwable t) {
        t = unwrapCause(t);

        return t instanceof TimeoutException
                || t instanceof IOException
                || t instanceof PeerUnavailableException
                || t instanceof RecipientLeftException;
    }

    private Peer randomNode() {
        return randomNode(null);
    }

    /**
     * Returns a random peer.
     *
     * <p>If the {@code retryContext} is not {@code null}, the random peer will be chosen as to not to match
     * {@link RetryContext#targetPeer()} and {@link RetryContext#unavailablePeers()}.
     */
    private Peer randomNode(@Nullable RetryContext retryContext) {
        List<Peer> localPeers = peers;

        var availablePeers = new ArrayList<Peer>(localPeers.size());

        if (retryContext == null) {
            availablePeers.addAll(localPeers);
        } else {
            for (Peer peer : localPeers) {
                if (!retryContext.targetPeer().equals(peer) && !retryContext.unavailablePeers().contains(peer)) {
                    availablePeers.add(peer);
                }
            }

            if (availablePeers.isEmpty()) {
                if (!peersAreUnavailable.getAndSet(true)) {
                    LOG.warn(
                            "All peers are unavailable, going to keep retrying until timeout [peers = {}, group = {}, trace ID: {}, "
                                    + "request {}, origin command {}, instance={}].",
                            localPeers,
                            groupId,
                            retryContext.errorTraceId(),
                            retryContext.request().toStringForLightLogging(),
                            retryContext.originCommandDescription(),
                            this
                    );
                }

                retryContext.resetUnavailablePeers();

                // Read the volatile field again, just in case it changed.
                availablePeers.addAll(peers);
            } else {
                peersAreUnavailable.set(false);
            }
        }

        if (availablePeers.isEmpty()) {
            throw new IgniteInternalException(INTERNAL_ERR, "No peers available [groupId=" + groupId + ']');
        }

        Collections.shuffle(availablePeers, ThreadLocalRandom.current());

        return availablePeers.stream()
                .filter(peer -> cluster.topologyService().getByConsistentId(peer.consistentId()) != null)
                .findAny()
                .orElse(availablePeers.get(0));
    }

    /**
     * Parse {@link Peer} from string representation of {@link PeerId}.
     *
     * @param peerId String representation of {@link PeerId}
     * @return Peer
     */
    private static @Nullable Peer parsePeer(@Nullable String peerId) {
        PeerId id = PeerId.parsePeer(peerId);

        return id == null ? null : new Peer(id.getConsistentId(), id.getIdx());
    }

    /**
     * Parse list of {@link PeerId} from list with string representations.
     *
     * @param peers List of {@link PeerId} string representations.
     * @return List of {@link PeerId}
     */
    private static List<Peer> parsePeerList(@Nullable Collection<String> peers) {
        if (peers == null) {
            return List.of();
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

    private CompletableFuture<InternalClusterNode> resolvePeer(Peer peer) {
        InternalClusterNode node = cluster.topologyService().getByConsistentId(peer.consistentId());

        if (node == null) {
            return CompletableFuture.failedFuture(new PeerUnavailableException(peer.consistentId()));
        }

        return completedFuture(node);
    }

    private TopologyEventHandler topologyEventHandler() {
        return new TopologyEventHandler() {
            @Override
            public void onDisappeared(InternalClusterNode member) {
                // Peers in throttling context are used for retries, so we use retry timeout here. Also, the retries themselves
                // also can be delayed for any reasons, so here is the multiplier.
                if (!busyLock.enterBusy()) {
                    // Replica is stopping, so we do not need to schedule the retry.
                    return;
                }

                try {
                    executor.schedule(
                            () -> throttlingContextHolder.onNodeLeft(member.name()),
                            configuration.retryTimeoutMillis().value() * 3,
                            TimeUnit.MILLISECONDS
                    );
                } finally {
                    busyLock.leaveBusy();
                }
            }
        };
    }

    @Override
    public void markAsStopping() {
        stopping = true;
    }
}
