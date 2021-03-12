/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.raft.client.rpc;

import java.util.concurrent.CompletableFuture;
import javax.validation.constraints.NotNull;
import org.apache.ignite.raft.client.State;
import org.apache.ignite.raft.client.PeerId;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactory;

import org.apache.ignite.raft.client.message.AddLearnersRequest;

import org.apache.ignite.raft.client.message.AddPeerRequest;
import org.apache.ignite.raft.client.message.AddPeerResponse;
import org.apache.ignite.raft.client.message.ChangePeersRequest;
import org.apache.ignite.raft.client.message.ChangePeersResponse;
import org.apache.ignite.raft.client.message.LearnersOpResponse;

import org.apache.ignite.raft.client.message.RemoveLearnersRequest;

import org.apache.ignite.raft.client.message.RemovePeerRequest;
import org.apache.ignite.raft.client.message.RemovePeerResponse;
import org.apache.ignite.raft.client.message.ResetLearnersRequest;

import org.apache.ignite.raft.client.message.ResetPeerRequest;

import org.apache.ignite.raft.client.message.SnapshotRequest;
import org.apache.ignite.raft.client.message.TransferLeaderRequest;
import org.apache.ignite.raft.client.message.UserRequest;
import org.apache.ignite.raft.client.message.UserResponse;

/**
 * Replication group RPC client.
 */
public interface RaftGroupRpcClient {
    /**
     * @param groupId Group id.
     * @return Group state snapshot.
     */
    @NotNull State state(String groupId);

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
     * Adds a voring peer to the raft group.
     *
     * @param request Request data.
     * @return A future with the result.
     */
    CompletableFuture<AddPeerResponse> addPeer(AddPeerRequest request);

    /**
     * Removes a peer from the raft group.
     *
     * @param request Request data.
     * @return A future with the result.
     */
    CompletableFuture<RemovePeerResponse> removePeer(RemovePeerRequest request);

    /**
     * Locally resets raft group peers. Intended for recovering from a group unavailability at the price of consistency.
     *
     * @param peerId Node to execute the configuration reset.
     * @param request Request data.
     * @return A future with result.
     */
    CompletableFuture<Void> resetPeers(PeerId peerId, ResetPeerRequest request);

    /**
     * Takes a local snapshot.
     *
     * @param peerId Peer id.
     * @param request Request data.
     * @return A future with the result.
     */
    CompletableFuture<Void> snapshot(PeerId peerId, SnapshotRequest request);

    /**
     * Change peers.
     *
     * @param request Request data.
     * @return A future with the result.
     */
    CompletableFuture<ChangePeersResponse> changePeers(ChangePeersRequest request);

    /**
     * Adds learners.
     *
     * @param request Request data.
     * @return A future with the result.
     */
    CompletableFuture<LearnersOpResponse> addLearners(AddLearnersRequest request);

    /**
     * Removes learners.
     *
     * @param request Request data.
     * @return A future with the result.
     */
    CompletableFuture<LearnersOpResponse> removeLearners(RemoveLearnersRequest request);

    /**
     * Resets learners to new set.
     *
     * @param request Request data.
     * @return A future with the result.
     */
    CompletableFuture<LearnersOpResponse> resetLearners(ResetLearnersRequest request);

    /**
     * Transfer leadership to other peer.
     *
     * @param request Request data.
     * @return A future with the result.
     */
    CompletableFuture<Void> transferLeader(TransferLeaderRequest request);

    /**
     * Submits a user request to the replication group leader.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    <R> CompletableFuture<UserResponse<R>> submit(UserRequest request);

    /**
     * @return An underlying message builder factory.
     */
    RaftClientMessageFactory factory();
}
