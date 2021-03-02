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

package org.apache.ignite.raft.client.service.impl;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.raft.client.PeerId;
import org.apache.ignite.raft.client.State;
import org.apache.ignite.raft.client.rpc.RaftGroupRpcClient;
import org.apache.ignite.raft.client.service.RaftGroupManagmentService;

public class RaftGroupManagementServiceImpl implements RaftGroupManagmentService {
    /** */
    private final RaftGroupRpcClient rpcClient;

    /** */
    private final String groupId;

    /**
     * @param rpcClient Client.
     * @param groupId Group id.
     */
    public RaftGroupManagementServiceImpl(RaftGroupRpcClient rpcClient, String groupId) {
        this.rpcClient = rpcClient;
        this.groupId = groupId;
    }

    @Override public PeerId getLeader() {
        return rpcClient.state(groupId).leader();
    }

    @Override public List<PeerId> getPeers() {
        return rpcClient.state(groupId).peers();
    }

    @Override public List<PeerId> getLearners() {
        return rpcClient.state(groupId).learners();
    }

    @Override public CompletableFuture<PeerId> refreshLeader() {
        return null;
    }

    @Override public CompletableFuture<State> refreshMembers() {
        return null;
    }

    @Override public CompletableFuture<PeersChangeState> addPeer(PeerId peerId) {
        return null;
    }

    @Override public CompletableFuture<PeersChangeState> removePeer(PeerId peerId) {
        return null;
    }

    @Override public CompletableFuture<Void> resetPeers(PeerId peerId, List<PeerId> peers) {
        return null;
    }

    @Override public CompletableFuture<Void> snapshot(PeerId peerId) {
        return null;
    }

    @Override public CompletableFuture<PeersChangeState> changePeers(List<PeerId> peers) {
        return null;
    }

    @Override public CompletableFuture<PeersChangeState> addLearners(List<PeerId> learners) {
        return null;
    }

    @Override public CompletableFuture<PeersChangeState> removeLearners(List<PeerId> learners) {
        return null;
    }

    @Override public CompletableFuture<PeersChangeState> resetLearners(List<PeerId> learners) {
        return null;
    }

    @Override public CompletableFuture<Void> transferLeader(PeerId newLeader) {
        return null;
    }
}
