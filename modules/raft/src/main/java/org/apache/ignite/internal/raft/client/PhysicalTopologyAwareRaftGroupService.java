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

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.tostring.IgniteToStringBuilder.includeSensitive;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.network.TopologyEventHandler;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.ExceptionFactory;
import org.apache.ignite.internal.raft.LeaderElectionListener;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeerUnavailableException;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.ReplicationGroupUnavailableException;
import org.apache.ignite.internal.raft.ThrottlingContextHolder;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.rebalance.RaftStaleUpdateException;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.service.TimeAwareRaftGroupService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.ActionResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.SubscriptionLeaderChangeRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ErrorResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.SMErrorResponse;
import org.apache.ignite.raft.jraft.rpc.impl.RaftException;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.raft.jraft.rpc.impl.SMCompactedThrowable;
import org.apache.ignite.raft.jraft.rpc.impl.SMFullThrowable;
import org.apache.ignite.raft.jraft.rpc.impl.SMThrowable;
import org.apache.ignite.raft.jraft.util.Utils;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * The RAFT group service is based on the cluster physical topology. This service has ability to subscribe of a RAFT group leader update.
 *
 * <p>This service implements leader-aware retry semantics for the {@link #run(Command, long)} method:
 * <ul>
 *     <li>{@code timeout == 0}: Single attempt mode - tries each peer once, throws {@link ReplicationGroupUnavailableException}
 *         if no leader is available.</li>
 *     <li>{@code timeout < 0} or {@code == Long.MAX_VALUE}: Infinite wait mode - waits indefinitely for leader,
 *         retries recoverable errors within {@code retryTimeoutMillis}.</li>
 *     <li>{@code 0 < timeout < Long.MAX_VALUE}: Bounded wait mode - waits up to {@code timeout} for leader,
 *         throws {@link ReplicationGroupUnavailableException} on timeout.</li>
 * </ul>
 */
public class PhysicalTopologyAwareRaftGroupService implements TimeAwareRaftGroupService {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(PhysicalTopologyAwareRaftGroupService.class);

    /** Raft message factory. */
    private static final RaftMessagesFactory MESSAGES_FACTORY = Loza.FACTORY;

    /** General leader election listener. */
    private final ServerEventHandler generalLeaderElectionListener;

    /** Failure manager. */
    private final FailureManager failureManager;

    /** Cluster service. */
    private final ClusterService clusterService;

    /** RPC RAFT client. */
    private final RaftGroupService raftClient;

    /** Executor to invoke RPC requests. */
    private final ScheduledExecutorService executor;

    /** RAFT configuration. */
    private final RaftConfiguration raftConfiguration;

    /** State machine for tracking leader availability. */
    private final LeaderAvailabilityState leaderAvailabilityState;

    /** Command marshaller. */
    private final Marshaller commandsMarshaller;

    /** Throttling context holder. */
    private final ThrottlingContextHolder throttlingContextHolder;

    /** Factory for creating stopping exceptions. */
    private final ExceptionFactory stoppingExceptionFactory;

    /** Current leader. */
    private volatile Peer leader;

    /** Busy lock for shutdown. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** This flag is used only for logging. */
    private final AtomicBoolean peersAreUnavailable = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param failureManager Failure manager.
     * @param clusterService Cluster service.
     * @param executor Executor to invoke RPC requests and notify listeners.
     * @param raftConfiguration RAFT configuration.
     * @param eventsClientListener Events client listener.
     */
    private PhysicalTopologyAwareRaftGroupService(
            ReplicationGroupId groupId,
            FailureManager failureManager,
            ClusterService clusterService,
            ScheduledExecutorService executor,
            RaftConfiguration raftConfiguration,
            PeersAndLearners configuration,
            Marshaller cmdMarshaller,
            ExceptionFactory stoppingExceptionFactory,
            ThrottlingContextHolder throttlingContextHolder,
            RaftGroupEventsClientListener eventsClientListener
    ) {
        this.failureManager = failureManager;
        this.clusterService = clusterService;
        this.executor = executor;
        this.raftConfiguration = raftConfiguration;
        this.commandsMarshaller = cmdMarshaller;
        this.stoppingExceptionFactory = stoppingExceptionFactory;
        this.throttlingContextHolder = throttlingContextHolder;
        this.raftClient = RaftGroupServiceImpl.start(
                groupId,
                clusterService,
                MESSAGES_FACTORY,
                raftConfiguration,
                configuration,
                executor,
                cmdMarshaller,
                stoppingExceptionFactory,
                throttlingContextHolder
        );

        this.leaderAvailabilityState = new LeaderAvailabilityState();
        this.generalLeaderElectionListener = new ServerEventHandler(executor);

        eventsClientListener.addLeaderElectionListener(raftClient.groupId(), generalLeaderElectionListener);

        // Subscribe the leader availability state to leader election notifications.
        subscribeLeader(leaderAvailabilityState::onLeaderElected);

        TopologyService topologyService = clusterService.topologyService();

        topologyService.addEventHandler(new TopologyEventHandler() {
            @Override
            public void onAppeared(InternalClusterNode member) {
                CompletableFuture<Boolean> fut = changeNodeSubscriptionIfNeed(member, true);

                requestLeaderManually(topologyService, executor, raftClient, fut);
            }
        });

        ArrayList<CompletableFuture<?>> futures = new ArrayList<>();

        for (InternalClusterNode member : topologyService.allMembers()) {
            futures.add(changeNodeSubscriptionIfNeed(member, true));
        }

        requestLeaderManually(topologyService, executor, raftClient, CompletableFutures.allOf(futures));
    }

    /**
     * Requests the leader information for the RAFT group.
     *
     * @param topologyService Topology service.
     * @param executor Executor to run asynchronous tasks.
     * @param raftClient RAFT client to interact with the RAFT group.
     * @param subscriptionsFut Future representing the completion of subscription updates.
     */
    // TODO: IGNITE-27256 Remove the method after implementing a notification after subscription.
    private void requestLeaderManually(
            TopologyService topologyService,
            Executor executor,
            RaftGroupService raftClient,
            CompletableFuture<?> subscriptionsFut
    ) {
        subscriptionsFut.thenRunAsync(
                () -> raftClient.refreshAndGetLeaderWithTerm().whenCompleteAsync(
                        (leaderWithTerm, throwable) -> {
                            if (throwable != null) {
                                LOG.warn("Could not refresh and get leader with term [grp={}].", groupId(), throwable);
                                return;
                            }

                            if (leaderWithTerm == null || leaderWithTerm.leader() == null) {
                                LOG.debug("No leader information available [grp={}].", groupId());
                                return;
                            }

                            InternalClusterNode leaderHost = topologyService
                                    .getByConsistentId(leaderWithTerm.leader().consistentId());

                            if (leaderHost != null) {
                                generalLeaderElectionListener.onLeaderElected(
                                        leaderHost,
                                        leaderWithTerm.term()
                                );
                            } else {
                                LOG.warn("Leader host occurred to leave the topology [nodeId = {}].",
                                        leaderWithTerm.leader().consistentId());
                            }
                        }, executor),
                executor);
    }

    private CompletableFuture<Boolean> changeNodeSubscriptionIfNeed(InternalClusterNode member, boolean subscribe) {
        Peer peer = new Peer(member.name());

        if (peers().contains(peer)) {
            SubscriptionLeaderChangeRequest msg = subscriptionLeaderChangeRequest(subscribe);

            return sendMessage(member, msg).whenComplete((isSent, err) -> {
                if (err != null) {
                    failureManager.process(new FailureContext(err, "Could not change subscription to leader updates [grp="
                            + groupId() + "]."));
                }

                LOG.info("Subscription status changed for the peer [grp={}, consistentId={}, subscribe={}, isSent={}].",
                        groupId(), member.name(), subscribe, isSent);
            });
        }

        return CompletableFutures.booleanCompletedFuture(false);
    }

    /**
     * Starts a new instance of the PhysicalTopologyAwareRaftGroupService.
     *
     * @param groupId The ID of the RAFT group.
     * @param cluster Cluster service for communication.
     * @param raftConfiguration Configuration for the RAFT group.
     * @param configuration Peers and learners configuration.
     * @param executor Executor for asynchronous tasks.
     * @param eventsClientListener Listener for RAFT group events.
     * @param cmdMarshaller Marshaller for RAFT commands.
     * @param stoppingExceptionFactory Factory for creating stopping exceptions.
     * @param throttlingContextHolder Context holder for throttling.
     * @param failureManager Manager for handling failures.
     * @return A new instance of PhysicalTopologyAwareRaftGroupService.
     */
    public static PhysicalTopologyAwareRaftGroupService start(
            ReplicationGroupId groupId,
            ClusterService cluster,
            RaftConfiguration raftConfiguration,
            PeersAndLearners configuration,
            ScheduledExecutorService executor,
            RaftGroupEventsClientListener eventsClientListener,
            Marshaller cmdMarshaller,
            ExceptionFactory stoppingExceptionFactory,
            ThrottlingContextHolder throttlingContextHolder,
            FailureManager failureManager
    ) {
        return new PhysicalTopologyAwareRaftGroupService(
                groupId,
                failureManager,
                cluster,
                executor,
                raftConfiguration,
                configuration,
                cmdMarshaller,
                stoppingExceptionFactory,
                throttlingContextHolder,
                eventsClientListener
        );
    }

    private void finishSubscriptions() {
        for (InternalClusterNode member : clusterService.topologyService().allMembers()) {
            changeNodeSubscriptionIfNeed(member, false);
        }
    }

    public void subscribeLeader(LeaderElectionListener callback) {
        generalLeaderElectionListener.addCallbackAndNotify(callback);
    }

    public void unsubscribeLeader(LeaderElectionListener callback) {
        generalLeaderElectionListener.removeCallbackAndNotify(callback);
    }

    private SubscriptionLeaderChangeRequest subscriptionLeaderChangeRequest(boolean subscribe) {
        return MESSAGES_FACTORY.subscriptionLeaderChangeRequest()
                .groupId(groupId())
                .subscribe(subscribe)
                .build();
    }

    private CompletableFuture<Boolean> sendMessage(InternalClusterNode node, SubscriptionLeaderChangeRequest msg) {
        var msgSendFut = new CompletableFuture<Boolean>();

        sendWithRetry(node, msg, msgSendFut);

        return msgSendFut;
    }

    private void sendWithRetry(InternalClusterNode node, SubscriptionLeaderChangeRequest msg, CompletableFuture<Boolean> msgSendFut) {
        Long responseTimeout = raftConfiguration.responseTimeoutMillis().value();

        clusterService.messagingService().invoke(node, msg, responseTimeout).whenCompleteAsync((unused, invokeThrowable) -> {
            if (invokeThrowable == null) {
                msgSendFut.complete(true);

                return;
            }

            Throwable invokeCause = unwrapCause(invokeThrowable);
            if (!msg.subscribe()) {
                // We don't want to propagate exceptions when unsubscribing (if it's not an Error!).
                if (invokeCause instanceof Error) {
                    msgSendFut.completeExceptionally(invokeThrowable);
                } else {
                    LOG.debug("An exception while trying to unsubscribe.", invokeThrowable);

                    msgSendFut.complete(false);
                }
            } else if (recoverable(invokeCause)) {
                sendWithRetry(node, msg, msgSendFut);
            } else if (invokeCause instanceof RecipientLeftException) {
                LOG.info(
                        "Could not subscribe to leader update from a specific node, because the node had left the cluster: [node={}].",
                        node
                );

                msgSendFut.complete(false);
            } else if (invokeCause instanceof NodeStoppingException) {
                msgSendFut.complete(false);
            } else {
                LOG.error("Could not send the subscribe message to the node: [node={}, msg={}].", invokeThrowable, node, msg);

                msgSendFut.completeExceptionally(invokeThrowable);
            }
        }, executor);
    }

    private static boolean recoverable(Throwable t) {
        t = unwrapCause(t);

        return t instanceof TimeoutException
                || t instanceof IOException
                || t instanceof PeerUnavailableException
                || t instanceof RecipientLeftException;
    }

    @Override
    public <R> CompletableFuture<R> run(Command cmd, long timeoutMillis) {
        // Normalize timeout: negative values mean infinite wait.
        long effectiveTimeout = (timeoutMillis < 0) ? Long.MAX_VALUE : timeoutMillis;
        // Wait for leader mode (bounded or infinite).
        long deadline = Utils.monotonicMsAfter(effectiveTimeout);

        return executeWithBusyLock(responseFuture -> {
            if (effectiveTimeout == 0) {
                tryAllPeersOnce(responseFuture, cmd);
            } else {
                startRetryPhase(responseFuture, cmd, deadline, leaderAvailabilityState.currentTerm());
            }
        });
    }

    /**
     * Resolves initial target peer for a command execution.
     *
     * <p>Tries the known leader first, falling back to a random peer if no leader is known.
     *
     * @return Initial target peer, or {@code null}.
     */
    @Nullable
    private Peer resolveInitialPeer() {
        Peer targetPeer = leader;
        if (targetPeer == null) {
            targetPeer = randomNode(null, false);
        }
        return targetPeer;
    }

    /**
     * Tries all peers once without waiting for leader.
     *
     * @param resultFuture Future that completes with the response, or fails with {@link ReplicationGroupUnavailableException} if no
     *         peer responds successfully.
     * @param cmd The command to execute.
     */
    private void tryAllPeersOnce(CompletableFuture<ActionResponse> resultFuture, Command cmd) {
        Peer targetPeer = resolveInitialPeer();
        if (targetPeer == null) {
            resultFuture.completeExceptionally(new ReplicationGroupUnavailableException(groupId()));

            return;
        }

        var context = new RetryContext(
                groupId().toString(),
                targetPeer,
                cmd::toStringForLightLogging,
                createRequestFactory(cmd),
                0,  // Single attempt - no retry timeout
                RetryContext.USE_DEFAULT_RESPONSE_TIMEOUT
        );

        sendWithRetrySingleAttempt(resultFuture, context);
    }

    /**
     * Executes an action within busy lock and transforms the response.
     */
    private <R> CompletableFuture<R> executeWithBusyLock(Consumer<CompletableFuture<ActionResponse>> action) {
        var responseFuture = new CompletableFuture<ActionResponse>();
        var resultFuture = new CompletableFuture<R>();

        if (!busyLock.enterBusy()) {
            resultFuture.completeExceptionally(stoppingExceptionFactory.create("Raft client is stopping [groupId=" + groupId() + "]."));

            return resultFuture;
        }

        try {
            action.accept(responseFuture);

            // Transform ActionResponse to result type.
            responseFuture.whenComplete((resp, err) -> {
                if (err != null) {
                    resultFuture.completeExceptionally(err);
                } else {
                    resultFuture.complete((R) resp.result());
                }
            });

            return resultFuture;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Applies deadline to a future using orTimeout.
     *
     * @param future The future to apply deadline to.
     * @param deadline Deadline in monotonic milliseconds, or Long.MAX_VALUE for no deadline.
     * @return The future with timeout applied, or the original future if no deadline.
     */
    private static <T> CompletableFuture<T> applyDeadline(CompletableFuture<T> future, long deadline) {
        if (deadline == Long.MAX_VALUE) {
            return future;
        }
        long remainingTime = deadline - Utils.monotonicMs();
        if (remainingTime <= 0) {
            return CompletableFuture.failedFuture(new TimeoutException());
        }
        return future.orTimeout(remainingTime, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a timeout exception for leader wait.
     */
    private ReplicationGroupUnavailableException createTimeoutException() {
        return new ReplicationGroupUnavailableException(
                groupId(),
                "Timeout waiting for leader [groupId=" + groupId() + "]."
        );
    }

    /**
     * Waits for leader to become available and then retries the command.
     */
    private void waitForLeaderAndRetry(CompletableFuture<ActionResponse> resultFuture, Command cmd, long deadline) {
        CompletableFuture<Long> leaderFuture = leaderAvailabilityState.awaitLeader();

        // Apply timeout if bounded.
        CompletableFuture<Long> timedLeaderFuture = applyDeadline(leaderFuture, deadline);

        timedLeaderFuture.whenCompleteAsync((term, waitError) -> {
            if (waitError != null) {
                Throwable cause = unwrapCause(waitError);
                if (cause instanceof TimeoutException) {
                    resultFuture.completeExceptionally(createTimeoutException());
                } else {
                    resultFuture.completeExceptionally(cause);
                }
                return;
            }

            if (!busyLock.enterBusy()) {
                resultFuture.completeExceptionally(stoppingExceptionFactory.create("Raft client is stopping [groupId=" + groupId() + "]."));
                return;
            }

            try {
                // Leader is available, now run the command with retry logic.
                startRetryPhase(resultFuture, cmd, deadline, term);
            } finally {
                busyLock.leaveBusy();
            }
        }, executor);
    }

    /**
     * Starts the retry phase after leader is available.
     */
    private void startRetryPhase(CompletableFuture<ActionResponse> resultFuture, Command cmd, long deadline, long term) {
        Peer targetPeer = resolveInitialPeer();
        if (targetPeer == null) {
            resultFuture.completeExceptionally(new ReplicationGroupUnavailableException(groupId()));

            return;
        }

        // Check deadline before starting retry phase.
        long now = Utils.monotonicMs();
        if (deadline != Long.MAX_VALUE && now >= deadline) {
            resultFuture.completeExceptionally(createTimeoutException());
            return;
        }

        // Use retry timeout bounded by remaining time until deadline.
        long configTimeout = raftConfiguration.retryTimeoutMillis().value();

        long sendWithRetryTimeoutMillis = deadline == Long.MAX_VALUE ? configTimeout : Math.min(configTimeout, deadline - now);

        var context = new RetryContext(
                groupId().toString(),
                targetPeer,
                cmd::toStringForLightLogging,
                createRequestFactory(cmd),
                sendWithRetryTimeoutMillis,
                RetryContext.USE_DEFAULT_RESPONSE_TIMEOUT
        );

        sendWithRetryWaitingForLeader(resultFuture, context, cmd, deadline, term);
    }

    /**
     * Creates a request factory for the given command.
     */
    private Function<Peer, ActionRequest> createRequestFactory(Command cmd) {
        if (cmd instanceof WriteCommand) {
            return targetPeer -> MESSAGES_FACTORY.writeActionRequest()
                    .groupId(groupId().toString())
                    .command(commandsMarshaller.marshall(cmd))
                    .deserializedCommand((WriteCommand) cmd)
                    .build();
        } else {
            return targetPeer -> MESSAGES_FACTORY.readActionRequest()
                    .groupId(groupId().toString())
                    .command((ReadCommand) cmd)
                    .readOnlySafe(true)
                    .build();
        }
    }

    /**
     * Sends a request with single attempt (no retry on timeout).
     *
     * <p>In single-attempt mode, each peer is tried at most once.
     */
    private <R extends NetworkMessage> void sendWithRetrySingleAttempt(
            CompletableFuture<R> fut,
            RetryContext retryContext
    ) {
        if (!busyLock.enterBusy()) {
            fut.completeExceptionally(stoppingExceptionFactory.create("Raft client is stopping [groupId=" + groupId() + "]."));
            return;
        }

        try {
            long responseTimeout = retryContext.responseTimeoutMillis() == RetryContext.USE_DEFAULT_RESPONSE_TIMEOUT
                    ? throttlingContextHolder.peerRequestTimeoutMillis() : retryContext.responseTimeoutMillis();

            retryContext.onNewAttempt();

            resolvePeer(retryContext.targetPeer())
                    .thenCompose(node -> clusterService.messagingService().invoke(node, retryContext.request(), responseTimeout))
                    .whenComplete((resp, err) -> {
                        if (!busyLock.enterBusy()) {
                            fut.completeExceptionally(
                                    stoppingExceptionFactory.create("Raft client is stopping [groupId=" + groupId() + "].")
                            );
                            return;
                        }

                        try {
                            if (err != null) {
                                handleThrowableSingleAttempt(fut, err, retryContext);
                            } else if (resp instanceof ErrorResponse) {
                                handleErrorResponseSingleAttempt(fut, (ErrorResponse) resp, retryContext);
                            } else if (resp instanceof SMErrorResponse) {
                                handleSmErrorResponse(fut, (SMErrorResponse) resp, retryContext);
                            } else {
                                leader = retryContext.targetPeer();
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

    /**
     * Validates throwable and returns next peer for retry.
     *
     * <p>If the error is not recoverable or no peers are available, completes the future exceptionally and returns null.
     *
     * @param fut Future to complete exceptionally if retry is not possible.
     * @param err The throwable to check.
     * @param retryContext Retry context for getting next peer.
     * @return Next peer for retry, or null if the future was already completed with an error.
     */
    @Nullable
    private Peer getNextPeerForRecoverableError(CompletableFuture<?> fut, Throwable err, RetryContext retryContext) {
        err = unwrapCause(err);

        if (!recoverable(err)) {
            fut.completeExceptionally(err);

            return null;
        }

        Peer nextPeer = randomNode(retryContext, false);

        if (nextPeer == null) {
            fut.completeExceptionally(new ReplicationGroupUnavailableException(groupId()));

            return null;
        }

        return nextPeer;
    }

    private static void logRecoverableError(RetryContext retryContext, Peer nextPeer) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Recoverable error during the request occurred (will be retried) [request={}, peer={}, newPeer={}, traceId={}].",
                    includeSensitive() ? retryContext.request() : retryContext.request().toStringForLightLogging(),
                    retryContext.targetPeer(),
                    nextPeer,
                    retryContext.errorTraceId()
            );
        }
    }

    /**
     * Handles throwable in single attempt mode.
     */
    private void handleThrowableSingleAttempt(
            CompletableFuture<? extends NetworkMessage> fut,
            Throwable err,
            RetryContext retryContext
    ) {
        Peer nextPeer = getNextPeerForRecoverableError(fut, err, retryContext);

        if (nextPeer == null) {
            return;
        }

        logRecoverableError(retryContext, nextPeer);

        String shortReasonMessage = "Peer " + retryContext.targetPeer().consistentId()
                + " threw " + unwrapCause(err).getClass().getSimpleName();
        sendWithRetrySingleAttempt(fut, retryContext.nextAttemptForUnavailablePeer(nextPeer, shortReasonMessage));
    }

    /**
     * Handles error response in single attempt mode.
     *
     * <p>In single-attempt mode, each peer is tried at most once. All errors mark the peer
     * as tried and move to the next peer. When all peers have been tried, the request fails
     * with {@link ReplicationGroupUnavailableException}.
     */
    private void handleErrorResponseSingleAttempt(
            CompletableFuture<? extends NetworkMessage> fut,
            ErrorResponse resp,
            RetryContext retryContext
    ) {
        RetryExecutionStrategy strategy = new RetryExecutionStrategy() {
            @Override
            public void executeRetry(RetryContext context, Peer nextPeer, @Nullable PeerTracking trackCurrentAs, String reason) {
                // Single-attempt mode: ALWAYS mark current peer as unavailable, regardless of trackCurrentAs.
                // This prevents retrying the same peer and matches original behavior where:
                // - transient errors: would select new peer, mark current unavailable
                // - peer unavailable: marks current unavailable
                // - no leader: marks current unavailable (no NO_LEADER distinction needed)
                // - leader redirect: marks current unavailable (to prevent redirect loops)
                sendWithRetrySingleAttempt(fut, context.nextAttemptForUnavailablePeer(nextPeer, reason));
            }

            @Override
            public void onAllPeersExhausted() {
                fut.completeExceptionally(new ReplicationGroupUnavailableException(groupId()));
            }
        };

        handleErrorResponseCommon(fut, resp, retryContext, strategy, false);
    }

    /**
     * Sends a request with retry and waits for leader on leader absence.
     */
    private void sendWithRetryWaitingForLeader(
            CompletableFuture<ActionResponse> fut,
            RetryContext retryContext,
            Command cmd,
            long deadline,
            long termWhenStarted
    ) {
        if (!busyLock.enterBusy()) {
            fut.completeExceptionally(stoppingExceptionFactory.create("Raft client is stopping [groupId=" + groupId() + "]."));
            return;
        }

        try {
            long requestStartTime = Utils.monotonicMs();
            long stopTime = retryContext.stopTime();

            if (requestStartTime >= stopTime) {
                // Retry timeout expired - fail with timeout exception.
                // For non-leader-related retriable errors, we don't wait for leader, just fail.
                fut.completeExceptionally(createTimeoutException());
                return;
            }

            ThrottlingContextHolder peerThrottlingContextHolder = throttlingContextHolder.peerContextHolder(
                    retryContext.targetPeer().consistentId()
            );

            peerThrottlingContextHolder.beforeRequest();
            retryContext.onNewAttempt();

            // Bound response timeout by remaining time until retry stop time.
            long responseTimeout = Math.min(peerThrottlingContextHolder.peerRequestTimeoutMillis(), stopTime - requestStartTime);

            resolvePeer(retryContext.targetPeer())
                    .thenCompose(node -> clusterService.messagingService().invoke(node, retryContext.request(), responseTimeout))
                    // Enforce timeout even if messaging service doesn't (e.g., in tests with mocks).
                    .orTimeout(responseTimeout > 0 ? responseTimeout : Long.MAX_VALUE, TimeUnit.MILLISECONDS)
                    .whenComplete((resp, err) -> {
                        peerThrottlingContextHolder.afterRequest(requestStartTime, retriableError(err, resp));

                        if (!busyLock.enterBusy()) {
                            fut.completeExceptionally(
                                    stoppingExceptionFactory.create("Raft client is stopping [groupId=" + groupId() + "].")
                            );
                            return;
                        }

                        try {
                            if (err != null) {
                                handleThrowableWithLeaderWait(fut, err, retryContext, cmd, deadline, termWhenStarted);
                            } else if (resp instanceof ErrorResponse) {
                                handleErrorResponseWithLeaderWait(fut, (ErrorResponse) resp, retryContext, cmd, deadline, termWhenStarted);
                            } else if (resp instanceof SMErrorResponse) {
                                handleSmErrorResponse(fut, (SMErrorResponse) resp, retryContext);
                            } else {
                                leader = retryContext.targetPeer();
                                fut.complete((ActionResponse) resp);
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

    /**
     * Handles throwable in leader-wait mode.
     */
    private void handleThrowableWithLeaderWait(
            CompletableFuture<ActionResponse> fut,
            Throwable err,
            RetryContext retryContext,
            Command cmd,
            long deadline,
            long termWhenStarted
    ) {
        Peer nextPeer = getNextPeerForRecoverableError(fut, err, retryContext);

        if (nextPeer == null) {
            return;
        }

        logRecoverableError(retryContext, nextPeer);

        String shortReasonMessage = "Peer " + retryContext.targetPeer().consistentId()
                + " threw " + unwrapCause(err).getClass().getSimpleName();
        scheduleRetryWithLeaderWait(fut, retryContext.nextAttempt(nextPeer, shortReasonMessage), cmd, deadline, termWhenStarted);
    }

    /**
     * Handles error response in leader-wait mode.
     *
     * <p>In leader-wait mode:
     * <ul>
     *     <li>Transient errors (EBUSY/EAGAIN) retry on the same peer after delay</li>
     *     <li>"No leader" errors try each peer once, then wait for leader notification</li>
     *     <li>Peer unavailability errors cycle through peers until timeout</li>
     * </ul>
     */
    private void handleErrorResponseWithLeaderWait(
            CompletableFuture<ActionResponse> fut,
            ErrorResponse resp,
            RetryContext retryContext,
            Command cmd,
            long deadline,
            long termWhenStarted
    ) {
        RetryExecutionStrategy strategy = new RetryExecutionStrategy() {
            @Override
            public void executeRetry(RetryContext context, Peer nextPeer, @Nullable PeerTracking trackCurrentAs, String reason) {
                RetryContext nextContext;
                if (trackCurrentAs == null) {
                    // Transient error or leader redirect - don't mark current peer.
                    nextContext = context.nextAttempt(nextPeer, reason);
                } else if (trackCurrentAs == PeerTracking.UNAVAILABLE) {
                    // Peer is down - mark as unavailable.
                    nextContext = context.nextAttemptForUnavailablePeer(nextPeer, reason);
                } else {
                    // No leader - mark as "no leader" (NOT unavailable).
                    // This allows the peer to be retried once a leader is known.
                    nextContext = context.nextAttemptForNoLeaderPeer(nextPeer, reason);
                }
                scheduleRetryWithLeaderWait(fut, nextContext, cmd, deadline, termWhenStarted);
            }

            @Override
            public void onAllPeersExhausted() {
                LOG.debug("All peers exhausted, waiting for leader [groupId={}, term={}]", groupId(), termWhenStarted);
                leaderAvailabilityState.onGroupUnavailable(termWhenStarted);
                waitForLeaderAndRetry(fut, cmd, deadline);
            }
        };

        handleErrorResponseCommon(fut, resp, retryContext, strategy, true);
    }

    /**
     * Schedules retry in leader-wait mode.
     */
    private void scheduleRetryWithLeaderWait(
            CompletableFuture<ActionResponse> fut,
            RetryContext retryContext,
            Command cmd,
            long deadline,
            long termWhenStarted
    ) {
        executor.schedule(
                () -> sendWithRetryWaitingForLeader(fut, retryContext, cmd, deadline, termWhenStarted),
                raftConfiguration.retryDelayMillis().value(),
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Handles state machine error response.
     */
    private static void handleSmErrorResponse(
            CompletableFuture<? extends NetworkMessage> fut,
            SMErrorResponse resp,
            RetryContext retryContext
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
            fut.completeExceptionally(new IgniteInternalException(
                    retryContext.errorTraceId(),
                    INTERNAL_ERR,
                    "Unknown SMThrowable type: " + (th == null ? "null" : th.getClass().getName())
            ));
        }
    }

    private static boolean retriableError(@Nullable Throwable e, NetworkMessage raftResponse) {
        int errorCode = raftResponse instanceof ErrorResponse ? ((ErrorResponse) raftResponse).errorCode() : 0;
        RaftError raftError = RaftError.forNumber(errorCode);

        if (e != null) {
            e = unwrapCause(e);

            // Retriable error but can be caused by an overload.
            return e instanceof TimeoutException || e instanceof IOException;
        }

        return raftError == RaftError.EBUSY || raftError == RaftError.EAGAIN;
    }

    /**
     * Resolves a peer to an internal cluster node.
     */
    private CompletableFuture<InternalClusterNode> resolvePeer(Peer peer) {
        InternalClusterNode node = clusterService.topologyService().getByConsistentId(peer.consistentId());

        if (node == null) {
            return CompletableFuture.failedFuture(new PeerUnavailableException(peer.consistentId()));
        }

        return CompletableFuture.completedFuture(node);
    }

    /**
     * Parses a peer ID string to a Peer object.
     *
     * @param peerId Peer ID string in format "consistentId:idx" or just "consistentId".
     * @return Parsed Peer object, or {@code null} if parsing fails.
     */
    @Nullable
    private static Peer parsePeer(@Nullable String peerId) {
        PeerId id = PeerId.parsePeer(peerId);

        return id == null ? null : new Peer(id.getConsistentId(), id.getIdx());
    }

    /**
     * Returns a random peer excluding unavailable peers and optionally "no leader" peers.
     *
     * <p>Behavior depends on parameters:
     * <ul>
     *     <li>If {@code retryContext} is null, returns any peer from the list.</li>
     *     <li>If {@code excludeNoLeaderPeers} is true, also excludes peers that returned "no leader"
     *         and does NOT reset exclusion sets when exhausted (returns null instead).</li>
     *     <li>If {@code excludeNoLeaderPeers} is false and all peers are unavailable in non-single-shot mode,
     *         resets unavailable peers and tries again.</li>
     * </ul>
     *
     * @param retryContext Retry context, or null for initial peer selection.
     * @param excludeNoLeaderPeers Whether to exclude peers that returned "no leader" response.
     * @return A random available peer, or null if none available.
     */
    @Nullable
    private Peer randomNode(@Nullable RetryContext retryContext, boolean excludeNoLeaderPeers) {
        List<Peer> localPeers = peers();

        if (localPeers == null || localPeers.isEmpty()) {
            return null;
        }

        var availablePeers = new ArrayList<Peer>(localPeers.size());

        if (retryContext == null) {
            availablePeers.addAll(localPeers);
        } else {
            for (Peer peer : localPeers) {
                if (retryContext.targetPeer().equals(peer) || retryContext.unavailablePeers().contains(peer)) {
                    continue;
                }
                if (excludeNoLeaderPeers && retryContext.noLeaderPeers().contains(peer)) {
                    continue;
                }
                availablePeers.add(peer);
            }

            // Reset unavailable peers only if NOT tracking no-leader peers separately.
            // When tracking no-leader peers, we want to exhaust all peers and then wait for leader.
            if (availablePeers.isEmpty() && !excludeNoLeaderPeers && !retryContext.singleShotRequest()) {
                if (!peersAreUnavailable.getAndSet(true)) {
                    LOG.warn(
                            "All peers are unavailable, going to keep retrying until timeout [peers = {}, group = {}, trace ID: {}, "
                                    + "request {}, origin command {}, instance={}].",
                            localPeers,
                            groupId(),
                            retryContext.errorTraceId(),
                            retryContext.request().toStringForLightLogging(),
                            retryContext.originCommandDescription(),
                            this
                    );
                }

                retryContext.resetUnavailablePeers();
                retryContext.resetNoLeaderPeers();

                // Read the volatile field again, just in case it changed.
                availablePeers.addAll(peers());
            } else {
                peersAreUnavailable.set(false);
            }
        }

        if (availablePeers.isEmpty()) {
            return null;
        }

        Collections.shuffle(availablePeers, ThreadLocalRandom.current());

        return availablePeers.stream()
                .filter(peer -> clusterService.topologyService().getByConsistentId(peer.consistentId()) != null)
                .findAny()
                .orElse(availablePeers.get(0));
    }

    private static String getShortReasonMessage(RetryContext retryContext, RaftError error, ErrorResponse resp) {
        return format("Peer {} returned code {}: {}", retryContext.targetPeer().consistentId(), error, resp.errorMsg());
    }

    /**
     * How to track the current peer when moving to a new one.
     */
    private enum PeerTracking {
        /** Mark as unavailable (down, shutting down). */
        UNAVAILABLE,
        /** Mark as "no leader" (working but doesn't know leader). */
        NO_LEADER
    }

    /**
     * Strategy for executing retries. Each method receives everything needed to perform the retry.
     *
     * <p>This interface abstracts the differences between single-attempt mode and leader-wait mode:
     * <ul>
     *     <li><b>Single-attempt mode</b>: Each peer is tried at most once. All errors mark the peer
     *         as unavailable. When all peers exhausted, fail immediately.</li>
     *     <li><b>Leader-wait mode</b>: Transient errors retry on the same peer with delay.
     *         "No leader" errors track peers separately. When exhausted, wait for leader notification.</li>
     * </ul>
     */
    private interface RetryExecutionStrategy {
        /**
         * Executes retry on the specified peer with the given tracking for the current peer.
         *
         * @param context Current retry context.
         * @param nextPeer Peer to retry on.
         * @param trackCurrentAs How to track the current peer (may be null for "don't track").
         * @param reason Human-readable reason for the retry.
         */
        void executeRetry(RetryContext context, Peer nextPeer, @Nullable PeerTracking trackCurrentAs, String reason);

        /**
         * Called when all peers have been exhausted.
         *
         * <p>In leader-wait mode, this triggers waiting for leader notification.
         * In single-attempt mode, this completes with {@link ReplicationGroupUnavailableException}.
         */
        void onAllPeersExhausted();
    }

    /**
     * Selects next peer and executes retry, or calls exhausted handler.
     * Centralizes the "select peer + null check + fallback" pattern.
     *
     * @param context Retry context.
     * @param excludeNoLeaderPeers Whether to also exclude peers that returned "no leader".
     * @param trackCurrentAs How to track the current peer.
     * @param reason Human-readable reason for the retry.
     * @param strategy Strategy for executing the retry.
     */
    private void selectPeerAndRetry(
            RetryContext context,
            boolean excludeNoLeaderPeers,
            PeerTracking trackCurrentAs,
            String reason,
            RetryExecutionStrategy strategy
    ) {
        Peer nextPeer = randomNode(context, excludeNoLeaderPeers);
        if (nextPeer != null) {
            strategy.executeRetry(context, nextPeer, trackCurrentAs, reason);
        } else {
            strategy.onAllPeersExhausted();
        }
    }

    /**
     * Handles error response using the provided strategy.
     *
     * <p>This method handles peer selection centrally. The strategy only decides HOW to retry
     * (with delay, marking peers, waiting for leader, etc.).
     *
     * <p>Error categories:
     * <ul>
     *     <li><b>SUCCESS</b>: Complete successfully</li>
     *     <li><b>Transient</b> (EBUSY/EAGAIN/UNKNOWN/EINTERNAL/ENOENT): Retry on same peer</li>
     *     <li><b>Unavailable</b> (EHOSTDOWN/ESHUTDOWN/ENODESHUTDOWN/ESTOP): Peer is down, try another</li>
     *     <li><b>EPERM with leader</b>: Redirect to actual leader</li>
     *     <li><b>EPERM without leader</b>: No leader known, try another peer or wait</li>
     *     <li><b>ESTALE</b>: Terminal error, stale update</li>
     *     <li><b>Other</b>: Terminal error</li>
     * </ul>
     *
     * @param fut Future to complete on terminal errors.
     * @param resp Error response from the peer.
     * @param retryContext Retry context.
     * @param strategy Strategy for executing retries.
     * @param trackNoLeaderSeparately Whether to track "no leader" peers separately from unavailable peers.
     */
    private void handleErrorResponseCommon(
            CompletableFuture<? extends NetworkMessage> fut,
            ErrorResponse resp,
            RetryContext retryContext,
            RetryExecutionStrategy strategy,
            boolean trackNoLeaderSeparately
    ) {
        RaftError error = RaftError.forNumber(resp.errorCode());
        String reason = getShortReasonMessage(retryContext, error, resp);

        switch (error) {
            case SUCCESS:
                leader = retryContext.targetPeer();
                fut.complete(null);
                break;

            case EBUSY:
            case EAGAIN:
            case UNKNOWN:
            case EINTERNAL:
            case ENOENT:
                // Transient errors - retry on same peer (null tracking = don't mark current peer).
                strategy.executeRetry(retryContext, retryContext.targetPeer(), null, reason);
                break;

            case EHOSTDOWN:
            case ESHUTDOWN:
            case ENODESHUTDOWN:
            case ESTOP:
                // Peer is down - select new peer, mark current as UNAVAILABLE.
                selectPeerAndRetry(retryContext, false, PeerTracking.UNAVAILABLE, reason, strategy);
                break;

            case EPERM:
                if (resp.leaderId() == null) {
                    // No leader known - select new peer, track based on mode.
                    PeerTracking tracking = trackNoLeaderSeparately ? PeerTracking.NO_LEADER : PeerTracking.UNAVAILABLE;
                    selectPeerAndRetry(retryContext, trackNoLeaderSeparately, tracking, reason, strategy);
                } else {
                    // Redirect to known leader (null tracking = don't mark current peer as bad).
                    Peer leaderPeer = parsePeer(resp.leaderId());

                    if (leaderPeer == null) {
                        throw new IllegalStateException("parsePeer returned null for non-null leaderId: " + resp.leaderId());
                    }

                    leader = leaderPeer;
                    strategy.executeRetry(retryContext, leaderPeer, null, reason);
                }
                break;

            case ESTALE:
                fut.completeExceptionally(new RaftStaleUpdateException(resp.errorMsg()));
                break;

            default:
                fut.completeExceptionally(new RaftException(error, resp.errorMsg()));
                break;
        }
    }

    @Override
    public ReplicationGroupId groupId() {
        return raftClient.groupId();
    }

    @Override
    public @Nullable Peer leader() {
        return raftClient.leader();
    }

    @Override
    public List<Peer> peers() {
        return raftClient.peers();
    }

    @Override
    public @Nullable List<Peer> learners() {
        return raftClient.learners();
    }

    @Override
    public CompletableFuture<Void> refreshLeader() {
        return raftClient.refreshLeader();
    }

    @Override
    public CompletableFuture<LeaderWithTerm> refreshAndGetLeaderWithTerm() {
        return raftClient.refreshAndGetLeaderWithTerm();
    }

    @Override
    public CompletableFuture<Void> refreshMembers(boolean onlyAlive) {
        return raftClient.refreshMembers(onlyAlive);
    }

    @Override
    public CompletableFuture<Void> addPeer(Peer peer, long sequenceToken) {
        return raftClient.addPeer(peer, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> removePeer(Peer peer, long sequenceToken) {
        return raftClient.removePeer(peer, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> changePeersAndLearners(PeersAndLearners peersAndLearners, long term, long sequenceToken) {
        return raftClient.changePeersAndLearners(peersAndLearners, term, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> changePeersAndLearnersAsync(PeersAndLearners peersAndLearners, long term, long sequenceToken) {
        return raftClient.changePeersAndLearnersAsync(peersAndLearners, term, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> addLearners(Collection<Peer> learners, long sequenceToken) {
        return raftClient.addLearners(learners, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> removeLearners(Collection<Peer> learners, long sequenceToken) {
        return raftClient.removeLearners(learners, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> resetLearners(Collection<Peer> learners, long sequenceToken) {
        return raftClient.resetLearners(learners, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> snapshot(Peer peer, boolean forced) {
        return raftClient.snapshot(peer, forced);
    }

    @Override
    public CompletableFuture<Void> transferLeadership(Peer newLeader) {
        return raftClient.transferLeadership(newLeader);
    }

    @Override
    public void shutdown() {
        busyLock.block();

        // Stop the leader availability state to cancel waiters and reject future requests.
        leaderAvailabilityState.stop(stoppingExceptionFactory.create("Raft client is stopping [groupId=" + groupId() + "]."));

        finishSubscriptions();

        raftClient.shutdown();
    }

    @Override
    public CompletableFuture<Long> readIndex() {
        return raftClient.readIndex();
    }

    @Override
    public ClusterService clusterService() {
        return raftClient.clusterService();
    }

    @Override
    public void updateConfiguration(PeersAndLearners configuration) {
        raftClient.updateConfiguration(configuration);
    }

    /**
     * Leader election handler.
     */
    private static class ServerEventHandler implements LeaderElectionListener {
        private final Executor executor;

        CompletableFuture<Void> fut = CompletableFutures.nullCompletedFuture();

        /** A leader elected callback. */
        private final ArrayList<LeaderElectionListener> callbacks = new ArrayList<>();

        /** Last elected leader. */
        private InternalClusterNode leaderNode = null;

        /** Term of the last elected leader. */
        private long leaderTerm = -1;

        /**
         * Constructor.
         *
         * @param executor Executor.
         */
        ServerEventHandler(Executor executor) {
            this.executor = executor;
        }

        /**
         * Notifies about a new leader elected, if it did not make before.
         *
         * @param node Node.
         * @param term Term.
         */
        @Override
        public synchronized void onLeaderElected(InternalClusterNode node, long term) {
            if (term > leaderTerm) {
                leaderTerm = term;
                leaderNode = node;

                if (callbacks.isEmpty()) {
                    return;
                }

                ArrayList<LeaderElectionListener> listeners = new ArrayList<>(callbacks);

                // Avoid notifying in the synchronized block.
                if (fut.isDone()) {
                    fut = runAsync(() -> {
                        for (LeaderElectionListener listener : listeners) {
                            listener.onLeaderElected(node, term);
                        }
                    }, executor);
                } else {
                    fut = fut.thenRunAsync(() -> {
                        for (LeaderElectionListener listener : listeners) {
                            listener.onLeaderElected(node, term);
                        }
                    }, executor);
                }
            }
        }

        synchronized void addCallbackAndNotify(LeaderElectionListener callback) {
            callbacks.add(callback);

            if (leaderTerm != -1) {
                long finalLeaderTerm = this.leaderTerm;
                InternalClusterNode finalLeaderNode = this.leaderNode;

                // Notify about the current leader outside of the synchronized block.
                if (fut.isDone()) {
                    fut = runAsync(() -> callback.onLeaderElected(finalLeaderNode, finalLeaderTerm), executor);
                } else {
                    fut = fut.thenRunAsync(() -> callback.onLeaderElected(finalLeaderNode, finalLeaderTerm), executor);
                }
            }
        }

        synchronized void removeCallbackAndNotify(LeaderElectionListener callback) {
            callbacks.remove(callback);
        }
    }

    @Override
    public void markAsStopping() {
        raftClient.markAsStopping();
    }

    /**
     * State machine for tracking leader availability.
     *
     * <p>Manages transitions between two states:
     * <ul>
     *     <li>{@link State#WAITING_FOR_LEADER} - No leader is currently known, operations must wait</li>
     *     <li>{@link State#LEADER_AVAILABLE} - A leader is known, operations can proceed</li>
     * </ul>
     *
     * <p>State transitions:
     * <pre>
     *     WAITING_FOR_LEADER --[onLeaderElected]--> LEADER_AVAILABLE
     *     LEADER_AVAILABLE --[onGroupUnavailable]--> WAITING_FOR_LEADER
     *     Any state --[destroy]--> destroyed (terminal state)
     * </pre>
     *
     * <p>Thread-safe: all public methods are synchronized or use proper synchronization.
     */
    @VisibleForTesting
    static class LeaderAvailabilityState {
        /** Possible states of leader availability. */
        enum State {
            /** No leader is known, waiting for leader election. */
            WAITING_FOR_LEADER,
            /** A leader is available and operations can proceed. */
            LEADER_AVAILABLE
        }

        /** Current state. Guarded by {@code this}. */
        private State currentState = State.WAITING_FOR_LEADER;

        /** Current leader term. Initialized to -1 to accept term 0 as the first valid term. Guarded by {@code this}. */
        private long currentTerm = -1;

        /** Future that waiters block on. Guarded by {@code this}. */
        private CompletableFuture<Long> waiters = new CompletableFuture<>();

        /** Whether the state machine has been stopped. Guarded by {@code this}. */
        private boolean stopped = false;

        /** Exception used to fail futures after destruction. Guarded by {@code this}. */
        private Throwable stopException;

        /**
         * Returns a future that completes when a leader becomes available.
         *
         * <p>If a leader is already available, returns an already-completed future with the current term.
         * Otherwise, returns a future that will complete when {@link #onLeaderElected} is called.
         * If the state machine has been destroyed, returns a failed future.
         *
         * @return Future that completes with the leader term when a leader is available.
         */
        synchronized CompletableFuture<Long> awaitLeader() {
            if (stopped) {
                return CompletableFuture.failedFuture(stopException);
            }
            if (currentState == State.LEADER_AVAILABLE) {
                return CompletableFuture.completedFuture(currentTerm);
            }
            return waiters;
        }

        /**
         * Handles leader election notification.
         *
         * <p>Transitions from {@link State#WAITING_FOR_LEADER} to {@link State#LEADER_AVAILABLE}
         * if the new term is greater than the current term. Stale notifications (with term <= current)
         * are ignored. Has no effect if the state machine has been stopped.
         *
         * @param leader The newly elected leader node.
         * @param term The term of the new leader.
         */
        void onLeaderElected(InternalClusterNode leader, long term) {
            CompletableFuture<Long> futureToComplete = null;

            synchronized (this) {
                if (stopped) {
                    LOG.debug("Ignoring leader election after stop [leader={}, term={}]", leader, term);
                    return;
                }

                // Ignore stale term notifications.
                if (term <= currentTerm) {
                    LOG.debug("Ignoring stale leader [newTerm={}, currentTerm={}]", term, currentTerm);
                    return;
                }

                long previousTerm = currentTerm;
                State previousState = currentState;

                currentTerm = term;

                if (currentState == State.WAITING_FOR_LEADER) {
                    currentState = State.LEADER_AVAILABLE;
                    futureToComplete = waiters;
                }

                LOG.debug("Leader elected [leader={}, term={}, previousTerm={}, stateChange={}->{}]",
                        leader, term, previousTerm, previousState, currentState);
            }

            // Complete outside the lock to avoid potential deadlocks with future callbacks.
            if (futureToComplete != null) {
                futureToComplete.complete(term);
            }
        }

        /**
         * Handles group unavailability notification.
         *
         * <p>Transitions from {@link State#LEADER_AVAILABLE} to {@link State#WAITING_FOR_LEADER}
         * only if the term hasn't changed since the unavailability was detected. This prevents
         * resetting the state if a new leader was already elected.
         * Has no effect if the state machine has been stopped.
         *
         * @param termWhenDetected The term at which unavailability was detected.
         */
        synchronized void onGroupUnavailable(long termWhenDetected) {
            if (stopped) {
                return;
            }

            // Only transition if we're still at the same term (no new leader elected in the meantime).
            if (currentTerm == termWhenDetected && currentState == State.LEADER_AVAILABLE) {
                State previousState = currentState;

                currentState = State.WAITING_FOR_LEADER;
                waiters = new CompletableFuture<>();

                LOG.debug("Group unavailable [term={}, stateChange={}->{}]",
                        termWhenDetected, previousState, currentState);
            }
        }

        /**
         * Returns the current leader term.
         *
         * @return Current leader term (-1 if no leader has been elected yet).
         */
        synchronized long currentTerm() {
            return currentTerm;
        }

        /**
         * Returns the current state.
         *
         * @return Current state.
         */
        synchronized State currentState() {
            return currentState;
        }

        /**
         * Returns whether the state machine has been stopped.
         *
         * @return True if destroyed.
         */
        synchronized boolean stopped() {
            return stopped;
        }

        /**
         * Stops the state machine, cancelling all pending waiters with the given exception.
         *
         * <p>After stop, the state machine is no longer active:
         * <ul>
         *     <li>{@link #awaitLeader()} returns a failed future</li>
         *     <li>{@link #onLeaderElected} is ignored</li>
         *     <li>{@link #onGroupUnavailable} is ignored</li>
         * </ul>
         *
         * <p>Called during shutdown to ensure waiters don't hang indefinitely.
         *
         * @param exception The exception to complete waiters with.
         */
        void stop(Throwable exception) {
            CompletableFuture<Long> futureToCancel;

            synchronized (this) {
                if (stopped) {
                    return;
                }

                stopped = true;
                stopException = exception;
                futureToCancel = waiters;
            }

            // Complete outside the lock to avoid potential deadlocks.
            futureToCancel.completeExceptionally(exception);
        }
    }

}
