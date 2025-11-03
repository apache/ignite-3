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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.util.FastTimestamps;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a context containing data for {@code RaftGroupServiceImpl#sendWithRetry} methods.
 *
 * <p>Not thread-safe. It is expected that every context is confined within a single {@code sendWithRetry} chain and, therefore,
 * happens-before relationship (i.e. visibility of changes to the mutable state) is achieved through consecutive {@code Executor.submit}
 * calls.
 */
class RetryContext {
    private static final int MAX_RETRY_REASONS = 25;

    private final String groupId;

    private Peer targetPeer;

    private final Supplier<@Nullable String> originDescription;

    private final Function<Peer, ? extends NetworkMessage> requestFactory;

    /**
     * Request that will be sent to the target peer.
     */
    private NetworkMessage request;

    /**
     * Timestamp that denotes the point in time up to which retry attempts will be made.
     */
    private final long stopTime;

    /**
     * Number of retries made. sendWithRetry method has a recursion nature, in case of recoverable exceptions or peer
     * unavailability it'll be scheduled for a next attempt. Generally a request will be retried until success or timeout.
     */
    private int retryCount = 0;

    /**
     * Set of peers that should be excluded when choosing a node to send a request to.
     */
    private final Set<Peer> unavailablePeers = new HashSet<>();

    /**
     * List of last {@value MAX_RETRY_REASONS} retry reasons. {@link LinkedList} in order to allow fast head removal upon overflow.
     */
    private final List<RetryReason> retryReasons = new LinkedList<>();

    /**
     * Trace ID that is used to track exceptions that happened during a particular chain of retries.
     *
     * <p>Will be generated on first access.
     */
    @Nullable
    private UUID errorTraceId;

    private final long startTime;

    private long attemptScheduleTime;

    private long attemptStartTime;

    private final long responseTimeoutMillis;

    /**
     * Creates a context.
     *
     * @param groupId Replication group ID.
     * @param targetPeer Target peer to send the request to.
     * @param originDescription Supplier describing the origin request from which this one depends, or returning {@code null} if
     *         this request is independent.
     * @param requestFactory Factory for creating requests to the target peer.
     * @param stopTime Timestamp that denotes the point in time up to which retry attempts will be made.
     * @param responseTimeoutMillis Response timeout for each attempt (up to {@code stopTime}) in milliseconds, {@code -1} if using
     *      {@link ThrottlingContextHolder#peerRequestTimeoutMillis} (default).
     */
    RetryContext(
            String groupId,
            Peer targetPeer,
            Supplier<@Nullable String> originDescription,
            Function<Peer, ? extends NetworkMessage> requestFactory,
            long stopTime,
            long responseTimeoutMillis
    ) {
        this.groupId = groupId;
        this.targetPeer = targetPeer;
        this.originDescription = originDescription;
        this.requestFactory = requestFactory;
        this.request = requestFactory.apply(targetPeer);
        this.stopTime = stopTime;
        this.startTime = System.currentTimeMillis();
        this.attemptScheduleTime = this.startTime;
        this.attemptStartTime = this.startTime;
        this.responseTimeoutMillis = responseTimeoutMillis;
    }

    Peer targetPeer() {
        return targetPeer;
    }

    NetworkMessage request() {
        return request;
    }

    @Nullable String originCommandDescription() {
        return originDescription.get();
    }

    long stopTime() {
        return stopTime;
    }

    Set<Peer> unavailablePeers() {
        return unavailablePeers;
    }

    UUID errorTraceId() {
        if (errorTraceId == null) {
            errorTraceId = UUID.randomUUID();
        }

        return errorTraceId;
    }

    void resetUnavailablePeers() {
        unavailablePeers.clear();
    }

    /**
     * Updates this context by changing the target peer.
     *
     * @return {@code this}.
     */
    RetryContext nextAttempt(Peer newTargetPeer, String shortReasonMessage) {
        long currentTime = System.currentTimeMillis();

        String reasonMessage = shortReasonMessage
                + "; attemptWaitDuration=" + (attemptStartTime - attemptScheduleTime)
                + ", attemptDuration=" + (currentTime - attemptStartTime)
                + ", attemptStartTime=" + timestampToString(currentTime);

        retryReasons.add(new RetryReason(reasonMessage, currentTime));

        attemptScheduleTime = currentTime;

        if (retryReasons.size() > MAX_RETRY_REASONS) {
            retryReasons.remove(0);
        }

        request = requestFactory.apply(newTargetPeer);

        targetPeer = newTargetPeer;

        retryCount++;

        return this;
    }

    private static String timestampToString(long timestamp) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss,SSS")
                .withZone(ZoneId.systemDefault());
        Instant instant = Instant.ofEpochMilli(timestamp);
        return formatter.format(instant);
    }

    /**
     * Updates this context by changing the target peer and adding the previous target peer to the "unavailable set".
     *
     * @return {@code this}.
     */
    RetryContext nextAttemptForUnavailablePeer(Peer newTargetPeer, String shortReasonMessage) {
        unavailablePeers.add(targetPeer);

        return nextAttempt(newTargetPeer, shortReasonMessage);
    }

    TimeoutException createTimeoutException() {
        long ct = System.currentTimeMillis();

        return new TimeoutException(format(
                "Send with retry timed out [retryCount = {}, groupId = {}, traceId = {}, request = {}, originCommand = {},"
                        + " retryReasons = {}, stopTime = {}, currentTime = {}, startTime = {}, duration = {}].",
                retryCount,
                groupId,
                errorTraceId,
                request.toStringForLightLogging(),
                originDescription.get(),
                retryReasons.toString(),
                stopTime,
                ct,
                startTime,
                ct - startTime
        ));
    }

    /**
     * Called when a new attempt is started (sends a request).
     */
    void onNewAttempt() {
        attemptStartTime = FastTimestamps.coarseCurrentTimeMillis();
    }

    private static class RetryReason {
        final long timestamp;
        final String reason;

        RetryReason(String reason, long currentTime) {
            this.timestamp = currentTime;
            this.reason = reason;
        }

        @Override
        public String toString() {
            // Purposefully make it shorter than "S.toString".
            return "[time=" + timestamp + ", msg=" + reason + "]";
        }
    }

    /**
     * Returns response timeout for each attempt (up to {@code stopTime}) in milliseconds, {@code -1} if using
     * {@link ThrottlingContextHolder#peerRequestTimeoutMillis} (default).
     */
    long responseTimeoutMillis() {
        return responseTimeoutMillis;
    }
}
