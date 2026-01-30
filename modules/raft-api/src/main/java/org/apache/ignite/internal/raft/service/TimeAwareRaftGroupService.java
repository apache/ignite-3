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

package org.apache.ignite.internal.raft.service;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.jetbrains.annotations.Nullable;

/**
 * A service providing operations on a replication group with explicit timeout control.
 *
 * <p>Most of operations require a known group leader. The group leader can be refreshed at any time by calling
 * {@link #refreshLeader(long)} method, otherwise it will happen automatically on a first call.
 *
 * <p>If a leader has been changed while the operation in progress, the operation will be transparently retried until timeout is reached.
 * The current leader will be refreshed automatically (maybe several times) in the process.
 *
 * <p>Each asynchronous method takes a {@code timeoutMillis} parameter with the following semantics:
 * <ul>
 *     <li>{@code 0} - single attempt without retries</li>
 *     <li>{@code Long.MAX_VALUE} - infinite wait</li>
 *     <li>negative values - treated as infinite for compatibility</li>
 *     <li>positive values - bounded wait up to the specified timeout</li>
 * </ul>
 * If a result is not available within the timeout, the future will be completed with a {@link TimeoutException}.
 *
 * <p>If an error occurs during operation execution, the future will be completed with the corresponding IgniteException having an
 * error code and a related message.
 *
 * <p>All async operations provided by the service are not cancellable.
 */
public interface TimeAwareRaftGroupService {

    /**
     * Runs a command on a replication group leader with the given timeout.
     *
     * <p>Read commands always see up to date data.
     *
     * @param cmd The command.
     * @param timeoutMillis Timeout in milliseconds. {@code 0} means single attempt without retries;
     *         {@code Long.MAX_VALUE} means infinite wait; negative values are treated as infinite for compatibility.
     * @param <R> Execution result type.
     * @return A future with the execution result.
     */
    <R> CompletableFuture<R> run(Command cmd, long timeoutMillis);

    /**
     * Returns group id.
     */
    ReplicationGroupId groupId();

    /**
     * Returns current leader id or {@code null} if it has not been yet initialized.
     */
    @Nullable Peer leader();

    /**
     * Returns a list of voting peers. The order is corresponding to the time of joining to the replication group.
     */
    List<Peer> peers();

    /**
     * Returns a list of leaners or {@code null} if it has not been yet initialized. The order is corresponding to the time of joining to
     *      the replication group.
     */
    @Nullable List<Peer> learners();

    /**
     * Refreshes a replication group leader with the given timeout.
     *
     * <p>After the future completion the method {@link #leader()} can be used to retrieve a current group leader.
     *
     * <p>This operation is executed on a group leader.
     *
     * @param timeoutMillis Timeout in milliseconds. {@code 0} means single attempt without retries;
     *         {@code Long.MAX_VALUE} means infinite wait; negative values are treated as infinite for compatibility.
     * @return A future.
     */
    CompletableFuture<Void> refreshLeader(long timeoutMillis);

    /**
     * Refreshes a replication group leader and returns (leader, term) tuple with the given timeout.
     *
     * <p>This operation is executed on a group leader.
     *
     * @param timeoutMillis Timeout in milliseconds. {@code 0} means single attempt without retries;
     *         {@code Long.MAX_VALUE} means infinite wait; negative values are treated as infinite for compatibility.
     * @return A future, with (leader, term) tuple.
     */
    CompletableFuture<LeaderWithTerm> refreshAndGetLeaderWithTerm(long timeoutMillis);

    /**
     * Refreshes replication group members with the given timeout.
     *
     * <p>After the future completion methods like {@link #peers()} and {@link #learners()} can be used to retrieve current members of a
     * group.
     *
     * <p>This operation is executed on a group leader.
     *
     * @param onlyAlive {@code True} to exclude dead nodes.
     * @param timeoutMillis Timeout in milliseconds. {@code 0} means single attempt without retries;
     *         {@code Long.MAX_VALUE} means infinite wait; negative values are treated as infinite for compatibility.
     * @return A future.
     */
    CompletableFuture<Void> refreshMembers(boolean onlyAlive, long timeoutMillis);

    /**
     * Adds a voting peer to the replication group with the given timeout.
     *
     * <p>After the future completion methods like {@link #peers()} and {@link #learners()} can be used to retrieve current members of a
     * group.
     *
     * <p>This operation is executed on a group leader.
     *
     * @param peer Peer
     * @param sequenceToken Sequence token of the current change.
     * @param timeoutMillis Timeout in milliseconds. {@code 0} means single attempt without retries;
     *         {@code Long.MAX_VALUE} means infinite wait; negative values are treated as infinite for compatibility.
     *
     * @return A future.
     */
    CompletableFuture<Void> addPeer(Peer peer, long sequenceToken, long timeoutMillis);

    /**
     * Removes peer from the replication group with the given timeout.
     *
     * <p>After the future completion methods like {@link #peers()} and {@link #learners()} can be used to retrieve current members of a
     * group.
     *
     * <p>This operation is executed on a group leader.
     *
     * @param peer Peer.
     * @param sequenceToken Sequence token of the current change.
     * @param timeoutMillis Timeout in milliseconds. {@code 0} means single attempt without retries;
     *         {@code Long.MAX_VALUE} means infinite wait; negative values are treated as infinite for compatibility.
     *
     * @return A future.
     */
    CompletableFuture<Void> removePeer(Peer peer, long sequenceToken, long timeoutMillis);

    /**
     * Changes peers and learners of a replication group with the given timeout.
     *
     * <p>After the future completion methods like {@link #peers()} and {@link #learners()} can be used to retrieve current members of a
     * group.
     *
     * <p>This operation is executed on a group leader.
     *
     * @param peersAndLearners New peers and Learners of the Raft group.
     * @param term Current known leader term.
     *             If real raft group term will be different - configuration update will be skipped.
     * @param sequenceToken Sequence token of the current change.
     * @param timeoutMillis Timeout in milliseconds. {@code 0} means single attempt without retries;
     *         {@code Long.MAX_VALUE} means infinite wait; negative values are treated as infinite for compatibility.
     *
     * @return A future.
     */
    CompletableFuture<Void> changePeersAndLearners(
            PeersAndLearners peersAndLearners, @Deprecated long term, long sequenceToken, long timeoutMillis);

    /**
     * Changes peers and learners of a replication group with the given timeout.
     *
     * <p>Asynchronous variant of the previous method.
     * When the future completed, it just means, that {@code changePeers} process has successfully started.
     *
     * <p>The results of rebalance itself will be processed by the listener of Raft reconfiguration event
     * (from raft/server module).
     *
     * <p>This operation is executed on a group leader.
     *
     * @param peersAndLearners New peers and Learners of the Raft group.
     * @param term Current known leader term.
     *             If real raft group term will be different - configuration update will be skipped.
     * @param sequenceToken Sequence token of the current change.
     * @param timeoutMillis Timeout in milliseconds. {@code 0} means single attempt without retries;
     *         {@code Long.MAX_VALUE} means infinite wait; negative values are treated as infinite for compatibility.
     *
     * @return A future.
     */
    CompletableFuture<Void> changePeersAndLearnersAsync(
            PeersAndLearners peersAndLearners, @Deprecated long term, long sequenceToken, long timeoutMillis);

    /**
     * Adds learners (non-voting members) with the given timeout.
     *
     * <p>After the future completion methods like {@link #peers()} and {@link #learners()} can be used to retrieve current members of a
     * group.
     *
     * <p>This operation is executed on a group leader.
     *
     * @param learners Collection of learners.
     * @param sequenceToken Sequence token of the current change.
     * @param timeoutMillis Timeout in milliseconds. {@code 0} means single attempt without retries;
     *         {@code Long.MAX_VALUE} means infinite wait; negative values are treated as infinite for compatibility.
     *
     * @return A future.
     */
    CompletableFuture<Void> addLearners(Collection<Peer> learners, long sequenceToken, long timeoutMillis);

    /**
     * Removes learners with the given timeout.
     *
     * <p>After the future completion methods like {@link #peers()} and {@link #learners()} can be used to retrieve current members of a
     * group.
     *
     * <p>This operation is executed on a group leader.
     *
     * @param learners Collection of learners.
     * @param sequenceToken Sequence token of the current change.
     * @param timeoutMillis Timeout in milliseconds. {@code 0} means single attempt without retries;
     *         {@code Long.MAX_VALUE} means infinite wait; negative values are treated as infinite for compatibility.
     *
     * @return A future.
     */
    CompletableFuture<Void> removeLearners(Collection<Peer> learners, long sequenceToken, long timeoutMillis);

    /**
     * Set learners of the raft group to needed list of learners with the given timeout.
     *
     * <p>After the future completion methods like {@link #peers()} and {@link #learners()} can be used to retrieve current members of a
     * group.
     *
     * <p>This operation is executed on a group leader.
     *
     * @param learners Collection of learners.
     * @param sequenceToken Sequence token of the current change.
     * @param timeoutMillis Timeout in milliseconds. {@code 0} means single attempt without retries;
     *         {@code Long.MAX_VALUE} means infinite wait; negative values are treated as infinite for compatibility.
     *
     * @return A future.
     */
    CompletableFuture<Void> resetLearners(Collection<Peer> learners, long sequenceToken, long timeoutMillis);

    /**
     * Takes a state machine snapshot on a given group peer with the given timeout.
     *
     * @param peer Peer.
     * @param forced {@code True} to force snapshot and log truncation.
     * @param timeoutMillis Timeout in milliseconds. {@code 0} means single attempt without retries;
     *         {@code Long.MAX_VALUE} means infinite wait; negative values are treated as infinite for compatibility.
     * @return A future.
     */
    CompletableFuture<Void> snapshot(Peer peer, boolean forced, long timeoutMillis);

    /**
     * Transfers leadership to other peer with the given timeout.
     *
     * <p>This operation is executed on a group leader.
     *
     * @param newLeader New leader.
     * @param timeoutMillis Timeout in milliseconds. {@code 0} means single attempt without retries;
     *         {@code Long.MAX_VALUE} means infinite wait; negative values are treated as infinite for compatibility.
     * @return A future.
     */
    CompletableFuture<Void> transferLeadership(Peer newLeader, long timeoutMillis);

    /**
     * Shutdown and cleanup resources for this instance.
     */
    void shutdown();

    /**
     * Reads index from the group leader with the given timeout.
     *
     * @param timeoutMillis Timeout in milliseconds. {@code 0} means single attempt without retries;
     *         {@code Long.MAX_VALUE} means infinite wait; negative values are treated as infinite for compatibility.
     * @return Future containing the index.
     */
    CompletableFuture<Long> readIndex(long timeoutMillis);

    /**
     * Returns a cluster service.
     *
     * @return Cluster service.
     */
    ClusterService clusterService();

    /**
     * Updates peers and learners lists in raft client.
     *
     * @param configuration Peers and learners configuration.
     */
    void updateConfiguration(PeersAndLearners configuration);

    // TODO: https://issues.apache.org/jira/browse/IGNITE-26085 Remove, tmp hack
    /**
     * Mark service as stopping.
     */
    default void markAsStopping() {
        // No-op
    }
}
