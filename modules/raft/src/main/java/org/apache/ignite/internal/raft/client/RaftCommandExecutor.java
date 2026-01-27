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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.tostring.IgniteToStringBuilder.includeSensitive;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.ExceptionFactory;
import org.apache.ignite.internal.raft.LeaderElectionListener;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeerUnavailableException;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.ReplicationGroupUnavailableException;
import org.apache.ignite.internal.raft.ThrottlingContextHolder;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.rebalance.RaftStaleUpdateException;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.ActionResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ErrorResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.SMErrorResponse;
import org.apache.ignite.raft.jraft.rpc.impl.RaftException;
import org.apache.ignite.raft.jraft.rpc.impl.SMCompactedThrowable;
import org.apache.ignite.raft.jraft.rpc.impl.SMFullThrowable;
import org.apache.ignite.raft.jraft.rpc.impl.SMThrowable;
import org.apache.ignite.raft.jraft.util.Utils;
import org.jetbrains.annotations.Nullable;

/**
 * Executes RAFT commands with leader-aware retry semantics.
 *
 * <p>Supports three timeout modes:
 * <ul>
 *     <li>{@code timeout == 0}: Single attempt - tries each peer once, throws {@link ReplicationGroupUnavailableException}
 *         if no leader is available.</li>
 *     <li>{@code timeout < 0} or {@code == Long.MAX_VALUE}: Infinite wait mode - waits indefinitely for leader,
 *         retries recoverable errors within {@code retryTimeoutMillis}.</li>
 *     <li>{@code 0 < timeout < Long.MAX_VALUE}: Bounded wait mode - waits up to {@code timeout} for leader,
 *         throws {@link ReplicationGroupUnavailableException} on timeout.</li>
 * </ul>
 */
class RaftCommandExecutor {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RaftCommandExecutor.class);

    /** Raft message factory. */
    private static final RaftMessagesFactory MESSAGES_FACTORY = Loza.FACTORY;

    /** Replication group ID. */
    private final ReplicationGroupId groupId;

    /** Supplier for the current peer list. Returns an immutable snapshot backed by a volatile field. */
    private final Supplier<List<Peer>> peersSupplier;

    /** Cluster service. */
    private final ClusterService clusterService;

    /** Executor to invoke RPC requests. */
    private final ScheduledExecutorService executor;

    /** RAFT configuration. */
    private final RaftConfiguration raftConfiguration;

    /** Command marshaller. */
    private final Marshaller commandsMarshaller;

    /** Factory for creating stopping exceptions. */
    private final ExceptionFactory stoppingExceptionFactory;

    /** Throttling context holder. */
    private final ThrottlingContextHolder throttlingContextHolder;

    /** State machine for tracking leader availability. */
    private final LeaderAvailabilityState leaderAvailabilityState;

    /** Current leader. */
    private volatile Peer leader;

    /** Busy lock for shutdown. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** This flag is used only for logging. */
    private final AtomicBoolean peersAreUnavailable = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param groupId Replication group ID.
     * @param peersSupplier Supplier for the current peer list. Must return an immutable snapshot.
     * @param clusterService Cluster service.
     * @param executor Executor to invoke RPC requests.
     * @param raftConfiguration RAFT configuration.
     * @param commandsMarshaller Command marshaller.
     * @param stoppingExceptionFactory Factory for creating stopping exceptions.
     * @param throttlingContextHolder Throttling context holder.
     */
    RaftCommandExecutor(
            ReplicationGroupId groupId,
            Supplier<List<Peer>> peersSupplier,
            ClusterService clusterService,
            ScheduledExecutorService executor,
            RaftConfiguration raftConfiguration,
            Marshaller commandsMarshaller,
            ExceptionFactory stoppingExceptionFactory,
            ThrottlingContextHolder throttlingContextHolder
    ) {
        this.groupId = groupId;
        this.peersSupplier = peersSupplier;
        this.clusterService = clusterService;
        this.executor = executor;
        this.raftConfiguration = raftConfiguration;
        this.commandsMarshaller = commandsMarshaller;
        this.stoppingExceptionFactory = stoppingExceptionFactory;
        this.throttlingContextHolder = throttlingContextHolder;
        this.leaderAvailabilityState = new LeaderAvailabilityState();
    }

    /**
     * Returns a {@link LeaderElectionListener} that feeds the internal leader availability state machine.
     *
     * @return Leader election listener callback.
     */
    LeaderElectionListener leaderElectionListener() {
        return leaderAvailabilityState::onLeaderElected;
    }

    /**
     * Executes a command with leader-aware retry semantics.
     *
     * @param cmd Command to execute.
     * @param timeoutMillis Timeout in milliseconds (0 = single attempt, negative/MAX_VALUE = infinite, positive = bounded).
     * @return Future that completes with the command result.
     */
    <R> CompletableFuture<R> run(Command cmd, long timeoutMillis) {
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
     * Shuts down the executor, blocking new run() calls and cancelling leader waiters.
     *
     * <p>Shutdown ordering is critical:
     * <ol>
     *     <li>Block busyLock first - ensures no new run() calls enter the busy section.
     *         In-flight calls waiting on awaitLeader() have already exited the busy section.</li>
     *     <li>Stop leaderAvailabilityState second - completes awaitLeader() futures exceptionally,
     *         triggering callbacks that will be rejected by the blocked busyLock.</li>
     * </ol>
     *
     * @param stopException Exception to complete waiters with.
     */
    void shutdown(Throwable stopException) {
        busyLock.block();

        leaderAvailabilityState.stop(stopException);
    }

    /**
     * Resolves initial target peer for a command execution.
     *
     * <p>Tries the known leader first, falling back to a random peer if no leader is known.
     *
     * @return Initial target peer, or {@code null}.
     */
    private @Nullable Peer resolveInitialPeer() {
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
            resultFuture.completeExceptionally(new ReplicationGroupUnavailableException(groupId));

            return;
        }

        var context = new RetryContext(
                groupId.toString(),
                targetPeer,
                cmd::toStringForLightLogging,
                createRequestFactory(cmd),
                0,  // Single attempt - no retry timeout
                raftConfiguration.responseTimeoutMillis().value()
        );

        sendWithRetrySingleAttempt(resultFuture, context);
    }

    /**
     * Executes an action within busy lock and transforms the response.
     */
    @SuppressWarnings("unchecked")
    private <R> CompletableFuture<R> executeWithBusyLock(Consumer<CompletableFuture<ActionResponse>> action) {
        var responseFuture = new CompletableFuture<ActionResponse>();
        var resultFuture = new CompletableFuture<R>();

        if (!busyLock.enterBusy()) {
            resultFuture.completeExceptionally(stoppingExceptionFactory.create("Raft client is stopping [groupId=" + groupId + "]."));

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
                groupId,
                "Timeout waiting for leader [groupId=" + groupId + "]."
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
                resultFuture.completeExceptionally(stoppingExceptionFactory.create("Raft client is stopping [groupId=" + groupId + "]."));
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
            resultFuture.completeExceptionally(new ReplicationGroupUnavailableException(groupId));

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
                groupId.toString(),
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
                    .groupId(groupId.toString())
                    .command(commandsMarshaller.marshall(cmd))
                    .deserializedCommand((WriteCommand) cmd)
                    .build();
        } else {
            return targetPeer -> MESSAGES_FACTORY.readActionRequest()
                    .groupId(groupId.toString())
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
            fut.completeExceptionally(stoppingExceptionFactory.create("Raft client is stopping [groupId=" + groupId + "]."));
            return;
        }

        try {
            long responseTimeout = retryContext.responseTimeoutMillis();

            retryContext.onNewAttempt();

            resolvePeer(retryContext.targetPeer())
                    .thenCompose(node -> clusterService.messagingService().invoke(node, retryContext.request(), responseTimeout))
                    .whenComplete((resp, err) ->
                            handleResponse(
                                    fut,
                                    resp,
                                    err,
                                    retryContext,
                                    new SingleAttemptRetryStrategy(fut)
                            )
                    );
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Attempts to select a peer for retrying after a recoverable error.
     *
     * <p>This method combines error validation with peer selection for retry handling.
     *
     * @param err The throwable to check (will be unwrapped).
     * @param retryContext Retry context for getting next peer.
     * @return Result containing the next peer, or the exception to complete the future with.
     */
    private RetryPeerResult selectPeerForRetry(Throwable err, RetryContext retryContext) {
        Throwable unwrappedErr = unwrapCause(err);

        if (!RaftErrorUtils.recoverable(unwrappedErr)) {
            return RetryPeerResult.fail(unwrappedErr);
        }

        Peer nextPeer = randomNode(retryContext, false);

        if (nextPeer == null) {
            return RetryPeerResult.fail(new ReplicationGroupUnavailableException(groupId));
        }

        return RetryPeerResult.success(nextPeer);
    }

    /**
     * Result of attempting to select a peer for retry.
     */
    private static final class RetryPeerResult {
        private final @Nullable Peer peer;
        private final @Nullable Throwable error;

        private RetryPeerResult(@Nullable Peer peer, @Nullable Throwable error) {
            this.peer = peer;
            this.error = error;
        }

        static RetryPeerResult success(Peer peer) {
            return new RetryPeerResult(peer, null);
        }

        static RetryPeerResult fail(Throwable error) {
            return new RetryPeerResult(null, error);
        }

        boolean isSuccess() {
            return peer != null;
        }

        Peer peer() {
            assert peer != null : "Check isSuccess() before calling peer()";
            return peer;
        }

        Throwable error() {
            assert error != null : "Check isSuccess() before calling error()";
            return error;
        }
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
     * Handles the response from a raft peer request.
     *
     * <p>Dispatches the response to the appropriate handler based on its type:
     * throwable, error response, state machine error, or successful response.
     *
     * @param fut Future to complete with the result.
     * @param resp Response message, or {@code null} if an error occurred.
     * @param err Throwable if the request failed, or {@code null} on success.
     * @param retryContext Retry context.
     * @param strategy Strategy for executing retries.
     */
    @SuppressWarnings("unchecked")
    private <R extends NetworkMessage> void handleResponse(
            CompletableFuture<R> fut,
            @Nullable NetworkMessage resp,
            @Nullable Throwable err,
            RetryContext retryContext,
            RetryExecutionStrategy strategy
    ) {
        if (!busyLock.enterBusy()) {
            fut.completeExceptionally(stoppingExceptionFactory.create("Raft client is stopping [groupId=" + groupId + "]."));
            return;
        }

        try {
            if (err != null) {
                handleThrowableWithRetry(fut, err, retryContext, strategy);
            } else if (resp instanceof ErrorResponse) {
                handleErrorResponseCommon(fut, (ErrorResponse) resp, retryContext, strategy);
            } else if (resp instanceof SMErrorResponse) {
                fut.completeExceptionally(extractSmError((SMErrorResponse) resp, retryContext));
            } else {
                leader = retryContext.targetPeer();
                fut.complete((R) resp);
            }
        } catch (Throwable e) {
            fut.completeExceptionally(e);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void handleThrowableWithRetry(
            CompletableFuture<? extends NetworkMessage> fut,
            Throwable err,
            RetryContext retryContext,
            RetryExecutionStrategy strategy
    ) {
        RetryPeerResult result = selectPeerForRetry(err, retryContext);

        if (!result.isSuccess()) {
            fut.completeExceptionally(result.error());
            return;
        }

        Peer nextPeer = result.peer();
        logRecoverableError(retryContext, nextPeer);

        String shortReasonMessage = "Peer " + retryContext.targetPeer().consistentId()
                + " threw " + unwrapCause(err).getClass().getSimpleName();
        strategy.executeRetry(retryContext, nextPeer, PeerTracking.COMMON, shortReasonMessage);
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
            fut.completeExceptionally(stoppingExceptionFactory.create("Raft client is stopping [groupId=" + groupId + "]."));
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

                        handleResponse(
                                fut,
                                resp,
                                err,
                                retryContext,
                                new LeaderWaitRetryStrategy(fut, cmd, deadline, termWhenStarted)
                        );
                    });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Extracts the error from a state machine error response.
     *
     * @param resp State machine error response.
     * @param retryContext Retry context (used for error trace ID).
     * @return The throwable to complete the future with.
     */
    private static Throwable extractSmError(SMErrorResponse resp, RetryContext retryContext) {
        SMThrowable th = resp.error();

        if (th instanceof SMCompactedThrowable) {
            SMCompactedThrowable compactedThrowable = (SMCompactedThrowable) th;

            try {
                return (Throwable) Class.forName(compactedThrowable.throwableClassName())
                        .getConstructor(String.class)
                        .newInstance(compactedThrowable.throwableMessage());
            } catch (Exception e) {
                LOG.warn("Cannot restore throwable from user's state machine. "
                        + "Check if throwable " + compactedThrowable.throwableClassName()
                        + " is present in the classpath.");

                return new IgniteInternalException(
                        retryContext.errorTraceId(), INTERNAL_ERR, compactedThrowable.throwableMessage()
                );
            }
        } else if (th instanceof SMFullThrowable) {
            return ((SMFullThrowable) th).throwable();
        } else {
            return new IgniteInternalException(
                    retryContext.errorTraceId(),
                    INTERNAL_ERR,
                    "Unknown SMThrowable type: " + (th == null ? "null" : th.getClass().getName())
            );
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
    private static @Nullable Peer parsePeer(@Nullable String peerId) {
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
    private @Nullable Peer randomNode(@Nullable RetryContext retryContext, boolean excludeNoLeaderPeers) {
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
                            groupId,
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

    private List<Peer> peers() {
        return peersSupplier.get();
    }

    private static String getShortReasonMessage(RetryContext retryContext, RaftError error, ErrorResponse resp) {
        return format("Peer {} returned code {}: {}", retryContext.targetPeer().consistentId(), error, resp.errorMsg());
    }

    /**
     * How to track the current peer when moving to a new one.
     */
    private enum PeerTracking {
        /** Don't mark the current peer (transient errors, leader redirects). */
        COMMON,
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
         * @param trackCurrentAs How to track the current peer ({@link PeerTracking#COMMON} for "don't track").
         * @param reason Human-readable reason for the retry.
         */
        void executeRetry(RetryContext context, Peer nextPeer, PeerTracking trackCurrentAs, String reason);

        /**
         * Called when all peers have been exhausted.
         *
         * <p>In leader-wait mode, this triggers waiting for leader notification.
         * In single-attempt mode, this completes with {@link ReplicationGroupUnavailableException}.
         */
        void onAllPeersExhausted();

        /**
         * Whether to track "no leader" peers separately from unavailable peers.
         *
         * <p>In leader-wait mode, peers that return "no leader" are tracked separately so that
         * when all peers are exhausted, the strategy can wait for a leader notification rather
         * than failing immediately. In single-attempt mode, all errors are treated uniformly
         * as unavailable.
         */
        boolean trackNoLeaderSeparately();
    }

    /**
     * Retry strategy for single-attempt mode.
     *
     * <p>In single-attempt mode, each peer is tried at most once. All errors mark the peer
     * as unavailable. When all peers have been tried, the request fails with
     * {@link ReplicationGroupUnavailableException}.
     */
    private class SingleAttemptRetryStrategy implements RetryExecutionStrategy {
        private final CompletableFuture<? extends NetworkMessage> fut;

        SingleAttemptRetryStrategy(CompletableFuture<? extends NetworkMessage> fut) {
            this.fut = fut;
        }

        @Override
        public void executeRetry(RetryContext context, Peer nextPeer, PeerTracking trackCurrentAs, String reason) {
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
            fut.completeExceptionally(new ReplicationGroupUnavailableException(groupId));
        }

        @Override
        public boolean trackNoLeaderSeparately() {
            return false;
        }
    }

    /**
     * Retry strategy for leader-wait mode.
     *
     * <p>In leader-wait mode:
     * <ul>
     *     <li>Transient errors retry on the same peer after delay</li>
     *     <li>"No leader" errors track peers separately and try each peer once</li>
     *     <li>When all peers are exhausted, waits for leader notification</li>
     * </ul>
     */
    private class LeaderWaitRetryStrategy implements RetryExecutionStrategy {
        private final CompletableFuture<ActionResponse> fut;
        private final Command cmd;
        private final long deadline;
        private final long termWhenStarted;

        LeaderWaitRetryStrategy(
                CompletableFuture<ActionResponse> fut,
                Command cmd,
                long deadline,
                long termWhenStarted
        ) {
            this.fut = fut;
            this.cmd = cmd;
            this.deadline = deadline;
            this.termWhenStarted = termWhenStarted;
        }

        @Override
        public void executeRetry(RetryContext context, Peer nextPeer, PeerTracking trackCurrentAs, String reason) {
            RetryContext nextContext;
            switch (trackCurrentAs) {
                case COMMON:
                    nextContext = context.nextAttempt(nextPeer, reason);
                    break;
                case UNAVAILABLE:
                    nextContext = context.nextAttemptForUnavailablePeer(nextPeer, reason);
                    break;
                case NO_LEADER:
                    nextContext = context.nextAttemptForNoLeaderPeer(nextPeer, reason);
                    break;
                default:
                    throw new AssertionError("Unexpected tracking: " + trackCurrentAs);
            }

            executor.schedule(
                    () -> sendWithRetryWaitingForLeader(fut, nextContext, cmd, deadline, termWhenStarted),
                    raftConfiguration.retryDelayMillis().value(),
                    TimeUnit.MILLISECONDS
            );
        }

        @Override
        public void onAllPeersExhausted() {
            LOG.debug("All peers exhausted, waiting for leader [groupId={}, term={}]", groupId, termWhenStarted);
            leaderAvailabilityState.onGroupUnavailable(termWhenStarted);
            waitForLeaderAndRetry(fut, cmd, deadline);
        }

        @Override
        public boolean trackNoLeaderSeparately() {
            return true;
        }
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
     */
    private void handleErrorResponseCommon(
            CompletableFuture<? extends NetworkMessage> fut,
            ErrorResponse resp,
            RetryContext retryContext,
            RetryExecutionStrategy strategy
    ) {
        boolean trackNoLeaderSeparately = strategy.trackNoLeaderSeparately();
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
                // Transient errors - retry on same peer (COMMON = don't mark current peer).
                strategy.executeRetry(retryContext, retryContext.targetPeer(), PeerTracking.COMMON, reason);
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
                    // Redirect to known leader (COMMON = don't mark current peer as bad).
                    Peer leaderPeer = parsePeer(resp.leaderId());

                    if (leaderPeer == null) {
                        throw new IllegalStateException("parsePeer returned null for non-null leaderId: " + resp.leaderId());
                    }

                    leader = leaderPeer;
                    strategy.executeRetry(retryContext, leaderPeer, PeerTracking.COMMON, reason);
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
}
