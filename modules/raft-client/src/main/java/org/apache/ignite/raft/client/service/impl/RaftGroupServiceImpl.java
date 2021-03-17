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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.PeerId;
import org.apache.ignite.raft.client.message.GetLeaderRequest;
import org.apache.ignite.raft.client.message.GetLeaderResponse;
import org.apache.ignite.raft.client.message.UserRequest;
import org.apache.ignite.raft.client.message.UserResponse;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactory;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class RaftGroupServiceImpl implements RaftGroupService {
    /** */
    private final int timeout;

    /** */
    private final String groupId;

    /** Where to ask for raft leader. */
    private final Set<NetworkMember> initialMembers;

    private final RaftClientMessageFactory factory;

    /** */
    private volatile PeerId leader;

    /** */
    private volatile List<PeerId> peers;

    /** */
    private volatile List<PeerId> learners;

    /** */
    private final NetworkCluster cluster;

    /**
     * @param groupId Group id.
     * @param cluster A cluster.
     * @param factory A message factory.
     * @param timeout Request timeout.
     * @param initialMembers Initial members of a group.
     */
    public RaftGroupServiceImpl(
        String groupId,
        NetworkCluster cluster,
        RaftClientMessageFactory factory,
        int timeout,
        Set<NetworkMember> initialMembers
    ) {
        this.cluster = cluster;
        this.initialMembers = initialMembers;
        this.factory = factory;
        this.timeout = timeout;
        this.groupId = groupId;
    }

    @Override public @NotNull String groupId() {
        return groupId;
    }

    @Override public PeerId getLeader() {
        return leader;
    }

    @Override public List<PeerId> getPeers() {
        return peers;
    }

    @Override public List<PeerId> getLearners() {
        return learners;
    }

    @Override public CompletableFuture<Void> refreshLeader() {
        return cluster.<GetLeaderResponse>sendWithResponse(
            initialMembers.iterator().next(),
            factory.createGetLeaderRequest().setGroupId(groupId).build(),
            timeout).thenApply(resp -> {
            leader = resp.getLeaderId();

            return null;
        });
    }

    @Override public CompletableFuture<Void> refreshMembers() {
        return null;
    }

    @Override public CompletableFuture<Void> addPeers(Collection<PeerId> peerIds) {
        return null;
    }

    @Override public CompletableFuture<Void> removePeers(Collection<PeerId> peerIds) {
        return null;
    }

    @Override public CompletableFuture<Void> addLearners(List<PeerId> learners) {
        return null;
    }

    @Override public CompletableFuture<Void> removeLearners(List<PeerId> learners) {
        return null;
    }

    @Override public CompletableFuture<Void> resetPeers(PeerId peerId, List<PeerId> peers) {
        return null;
    }

    @Override public CompletableFuture<Void> snapshot(PeerId peerId) {
        return null;
    }

    @Override public CompletableFuture<Void> transferLeadership(PeerId newLeader) {
        return null;
    }

    @Override public <R> CompletableFuture<R> submit(Command cmd) {
        UserRequest req = factory.createUserRequest().setRequest(cmd).setGroupId(groupId).build();

        PeerId leader = this.leader;

        CompletableFuture<Void> fut0 = leader == null ? refreshLeader() : completedFuture(null);

        CompletableFuture<UserResponse<R>> fut1 = fut0.thenCompose(peerId -> cluster.sendWithResponse(leader.getNode(), req, timeout));

        return fut1.thenApply(resp -> resp.response());
    }
}
