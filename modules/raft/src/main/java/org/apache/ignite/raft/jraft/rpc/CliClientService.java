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
package org.apache.ignite.raft.jraft.rpc;

import java.util.concurrent.Future;
import org.apache.ignite.raft.jraft.entity.PeerId;

/**
 * Cli RPC client service.
 */
public interface CliClientService extends ClientService {

    /**
     * Adds a peer.
     *
     * @param peerId peer ID
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> addPeer(PeerId peerId, CliRequests.AddPeerRequest request,
        RpcResponseClosure<CliRequests.AddPeerResponse> done);

    /**
     * Removes a peer.
     *
     * @param peerId peer ID
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> removePeer(PeerId peerId, CliRequests.RemovePeerRequest request,
        RpcResponseClosure<CliRequests.RemovePeerResponse> done);

    /**
     * Reset a peer.
     *
     * @param peerId peer ID
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> resetPeer(PeerId peerId, CliRequests.ResetPeerRequest request,
        RpcResponseClosure<RpcRequests.ErrorResponse> done);

    /**
     * Do a snapshot.
     *
     * @param peerId peer ID
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> snapshot(PeerId peerId, CliRequests.SnapshotRequest request,
        RpcResponseClosure<RpcRequests.ErrorResponse> done);

    /**
     * Change peers.
     *
     * @param peerId peer ID
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> changePeers(PeerId peerId, CliRequests.ChangePeersRequest request,
        RpcResponseClosure<CliRequests.ChangePeersResponse> done);

    /**
     * Add learners
     *
     * @param peerId peer ID
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> addLearners(PeerId peerId, CliRequests.AddLearnersRequest request,
        RpcResponseClosure<CliRequests.LearnersOpResponse> done);

    /**
     * Remove learners
     *
     * @param peerId peer ID
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> removeLearners(PeerId peerId, CliRequests.RemoveLearnersRequest request,
        RpcResponseClosure<CliRequests.LearnersOpResponse> done);

    /**
     * Reset learners
     *
     * @param peerId peer ID
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> resetLearners(PeerId peerId, CliRequests.ResetLearnersRequest request,
        RpcResponseClosure<CliRequests.LearnersOpResponse> done);

    /**
     * Get the group leader.
     *
     * @param peerId peer ID
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> getLeader(PeerId peerId, CliRequests.GetLeaderRequest request,
        RpcResponseClosure<CliRequests.GetLeaderResponse> done);

    /**
     * Transfer leadership to other peer.
     *
     * @param peerId peer ID
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> transferLeader(PeerId peerId, CliRequests.TransferLeaderRequest request,
        RpcResponseClosure<RpcRequests.ErrorResponse> done);

    /**
     * Get all peers of the replication group.
     *
     * @param peerId peer ID
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> getPeers(PeerId peerId, CliRequests.GetPeersRequest request,
        RpcResponseClosure<CliRequests.GetPeersResponse> done);
}
