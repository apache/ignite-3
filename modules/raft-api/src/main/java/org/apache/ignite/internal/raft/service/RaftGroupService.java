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
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.jetbrains.annotations.Nullable;

/**
 * A service providing operations on a replication group.
 *
 * <p>Most of operations require a known group leader. The group leader can be refreshed at any time by calling {@link #refreshLeader()}
 * method, otherwise it will happen automatically on a first call.
 *
 * <p>If a leader has been changed while the operation in progress, the operation will be transparently retried until timeout is reached.
 * The current leader will be refreshed automatically (maybe several times) in the process.
 *
 * <p>Each asynchronous method (returning a future) uses a default timeout to finish, see {@link RaftConfiguration#retryTimeoutMillis()}.
 * If a result is not available within the timeout, the future will be completed with a {@link TimeoutException}
 *
 * <p>If an error is occurred during operation execution, the future will be completed with the corresponding IgniteException having an
 * error code and a related message.
 *
 * <p>All async operations provided by the service are not cancellable.
 */
public interface RaftGroupService extends RaftCommandRunner {
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
     * Refreshes a replication group leader.
     *
     * <p>After the future completion the method {@link #leader()} can be used to retrieve a current group leader.
     *
     * <p>This operation is executed on a group leader.
     *
     * @return A future.
     */
    CompletableFuture<Void> refreshLeader();

    /**
     * Refreshes a replication group leader and returns (leader, term) tuple.
     *
     * <p>This operation is executed on a group leader.
     *
     * @return A future, with (leader, term) tuple.
     */
    CompletableFuture<LeaderWithTerm> refreshAndGetLeaderWithTerm();

    /**
     * Refreshes replication group members.
     *
     * <p>After the future completion methods like {@link #peers()} and {@link #learners()} can be used to retrieve current members of a
     * group.
     *
     * <p>This operation is executed on a group leader.
     *
     * @param onlyAlive {@code True} to exclude dead nodes.
     * @return A future.
     */
    CompletableFuture<Void> refreshMembers(boolean onlyAlive);

    /**
     * Adds a voting peer to the replication group.
     *
     * <p>After the future completion methods like {@link #peers()} and {@link #learners()} can be used to retrieve current members of a
     * group.
     *
     * <p>This operation is executed on a group leader.
     *
     * @param peer Peer
     * @param sequenceToken Sequence token of the current change.
     *
     * @return A future.
     */
    CompletableFuture<Void> addPeer(Peer peer, long sequenceToken);

    /**
     * Removes peer from the replication group.
     *
     * <p>After the future completion methods like {@link #peers()} and {@link #learners()} can be used to retrieve current members of a
     * group.
     *
     * <p>This operation is executed on a group leader.
     *
     * @param peer Peer.
     * @param sequenceToken Sequence token of the current change.
     *
     * @return A future.
     */
    CompletableFuture<Void> removePeer(Peer peer, long sequenceToken);

    /**
     * Changes peers and learners of a replication group.
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
     *
     * @return A future.
     */
    CompletableFuture<Void> changePeersAndLearners(PeersAndLearners peersAndLearners, @Deprecated long term, long sequenceToken);

    /**
     * Changes peers and learners of a replication group.
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
     *
     * @return A future.
     */
    CompletableFuture<Void> changePeersAndLearnersAsync(PeersAndLearners peersAndLearners, @Deprecated long term, long sequenceToken);

    /**
     * Adds learners (non-voting members).
     *
     * <p>After the future completion methods like {@link #peers()} and {@link #learners()} can be used to retrieve current members of a
     * group.
     *
     * <p>This operation is executed on a group leader.
     *
     * @param learners Collection of learners.
     * @param sequenceToken Sequence token of the current change.
     *
     * @return A future.
     */
    CompletableFuture<Void> addLearners(Collection<Peer> learners, long sequenceToken);

    /**
     * Removes learners.
     *
     * <p>After the future completion methods like {@link #peers()} and {@link #learners()} can be used to retrieve current members of a
     * group.
     *
     * <p>This operation is executed on a group leader.
     *
     * @param learners Collection of learners.
     * @param sequenceToken Sequence token of the current change.
     *
     * @return A future.
     */
    CompletableFuture<Void> removeLearners(Collection<Peer> learners, long sequenceToken);

    /**
     * Set learners of the raft group to needed list of learners.
     *
     * <p>After the future completion methods like {@link #peers()} and {@link #learners()} can be used to retrieve current members of a
     * group.
     *
     * <p>This operation is executed on a group leader.
     *
     * @param learners Collection of learners.
     * @param sequenceToken Sequence token of the current change.
     *
     * @return A future.
     */
    CompletableFuture<Void> resetLearners(Collection<Peer> learners, long sequenceToken);

    /**
     * Takes a state machine snapshot on a given group peer.
     *
     * @param peer Peer.
     * @param forced {@code True} to force snapshot and log truncation.
     * @return A future.
     */
    CompletableFuture<Void> snapshot(Peer peer, boolean forced);

    /**
     * Transfers leadership to other peer.
     *
     * <p>This operation is executed on a group leader.
     *
     * @param newLeader New leader.
     * @return A future.
     */
    CompletableFuture<Void> transferLeadership(Peer newLeader);

    /**
     * Shutdown and cleanup resources for this instance.
     */
    void shutdown();

    /**
     * Reads index from the group leader.
     *
     * @return Future containing the index.
     */
    CompletableFuture<Long> readIndex();

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
    default void markAsStopping() {
        // No-op
    }
}
