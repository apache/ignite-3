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

package org.apache.ignite.raft.client.service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.raft.client.PeerId;
import org.apache.ignite.raft.client.State;
import org.jetbrains.annotations.Nullable;

/**
 * Replication group management service.
 */
public interface RaftGroupManagmentService {
    /**
     * @return Leader id or null if it has not been yet initialized.
     */
    @Nullable PeerId getLeader();

    /**
     * @return List of peers or null if it has not been yet initialized.
     */
    @Nullable List<PeerId> getPeers();

    /**
     * @return List of leaners or null if it has not been yet initialized.
     */
    @Nullable List<PeerId> getLearners();

    /**
     * Refreshes a replication group leader.
     *
     * @param groupId Group id.
     * @return A future with the result.
     */
    CompletableFuture<PeerId> refreshLeader(String groupId);

    /**
     * Refreshes a replication group members (excluding a leader).
     *
     * @param groupId Group id.
     * @return A future with the result.
     */
    CompletableFuture<State> refreshMembers(String groupId);

    /**
     * Adds a voting peer to the raft group.
     *
     * @param peerId Peer id.
     * @return A future with the result.
     */
    CompletableFuture<PeersChangeState> addPeer(PeerId peerId);

    /**
     * Removes a peer from the raft group.
     *
     * @param peerId Peer id.
     * @return A future with the result.
     */
    CompletableFuture<PeersChangeState> removePeer(PeerId peerId);

    /**
     * Locally resets raft group peers. Intended for recovering from a group unavailability at the price of consistency.
     *
     * @param peerId Peer id.
     * @param peers List of peers.
     * @return A future with the result.
     */
    CompletableFuture<Void> resetPeers(PeerId peerId, List<PeerId> peers);

    /**
     * Takes a local snapshot.
     *
     * @param peerId Peer id.
     * @return A future with the result.
     */
    CompletableFuture<Void> snapshot(PeerId peerId);

    /**
     * Change peers.
     *
     * @param peers List of peers.
     * @return A future with the result.
     */
    CompletableFuture<PeersChangeState> changePeers(List<PeerId> peers);

    /**
     * Adds learners.
     *
     * @param learners List of learners.
     * @return A future with the result.
     */
    CompletableFuture<PeersChangeState> addLearners(List<PeerId> learners);

    /**
     * Removes learners.
     *
     * @param learners List of learners.
     * @return A future with the result.
     */
    CompletableFuture<PeersChangeState> removeLearners(List<PeerId> learners);

    /**
     * Resets learners to new set.
     *
     * @param learners List of learners.
     * @return A future with the result.
     */
    CompletableFuture<PeersChangeState> resetLearners(List<PeerId> learners);

    /**
     * Transfer leadership to other peer.
     *
     * @param newLeader New leader.
     * @return A future with the result.
     */
    CompletableFuture<Void> transferLeader(PeerId newLeader);

    /**
     * Represents a change in peers list.
     */
    interface PeersChangeState {
        List<PeerId> getOld();

        List<PeerId> getNew();
    }
}
