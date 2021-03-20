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
import java.util.function.Function;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.PeerId;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.message.AddPeersRequest;
import org.apache.ignite.raft.client.message.ChangePeersResponse;
import org.apache.ignite.raft.client.message.GetLeaderResponse;
import org.apache.ignite.raft.client.message.GetPeersRequest;
import org.apache.ignite.raft.client.message.GetPeersResponse;
import org.apache.ignite.raft.client.message.UserRequest;
import org.apache.ignite.raft.client.message.UserResponse;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactory;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;

/**
 * The implementation of {@link RaftGroupService}
 */
public class RaftGroupServiceImpl implements RaftGroupService {
    /** */
    private static System.Logger LOG = System.getLogger(RaftGroupService.class.getName());

    /** */
    private static CompletableFuture<Void> FINISHED_FUT = CompletableFuture.completedFuture(null);

    /** */
    private volatile int timeout;

    /** */
    private final String groupId;

    /** Where to ask for leader. */
    private final Function<String, Set<NetworkMember>> initialMembersResolver;

    /** */
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
     * @param initialMembersResolver A closure to resolve network members for a group.
     * @param refreshLeader {@code True} to synchronously refresh leader on service creation.
     */
    public RaftGroupServiceImpl(
        String groupId,
        NetworkCluster cluster,
        RaftClientMessageFactory factory,
        int timeout,
        Function<String, Set<NetworkMember>> initialMembersResolver,
        boolean refreshLeader
    ) {
        this.cluster = cluster;
        this.initialMembersResolver = initialMembersResolver;
        this.factory = factory;
        this.timeout = timeout;
        this.groupId = groupId;

        if (refreshLeader) {
            try {
                refreshLeader().get();
            }
            catch (Exception e) {
                error("Failed to refresh a leader", e);
            }
        }
    }

    @Override public @NotNull String groupId() {
        return groupId;
    }

    @Override public long timeout() {
        return timeout;
    }

    @Override public void timeout(long newTimeout) {
        this.timeout = timeout;
    }

    @Override public PeerId leader() {
        return leader;
    }

    @Override public List<PeerId> peers() {
        return peers;
    }

    @Override public List<PeerId> learners() {
        return learners;
    }

    @Override public CompletableFuture<Void> refreshLeader() {
        Set<NetworkMember> members = initialMembersResolver.apply(groupId);

        // TODO Search all members.
        return cluster.<GetLeaderResponse>sendWithResponse(
            members.iterator().next(),
            factory.createGetLeaderRequest().setGroupId(groupId).build(),
            timeout).thenApply(resp -> {
            leader = resp.getLeaderId();

            return null;
        });
    }

    @Override public CompletableFuture<Void> refreshMembers(boolean onlyAlive) {
        GetPeersRequest req = factory.createGetPeersRequest().setOnlyAlive(onlyAlive).setGroupId(groupId).build();

        CompletableFuture<GetPeersResponse> fut = sendWithRetry(req);

        return fut.thenApply(resp -> {
            peers = resp.getPeersList();
            learners = resp.getLearnersList();

            return null;
        });
    }

    @Override public CompletableFuture<Void> addPeers(Collection<PeerId> peerIds) {
        AddPeersRequest.Builder builder = factory.createAddPeersRequest().setGroupId(groupId);

        for (PeerId peerId : peerIds)
            builder.addPeer(peerId);

        AddPeersRequest req = builder.build();

        CompletableFuture<ChangePeersResponse> future = sendWithRetry(req);

        return future.thenApply(resp -> {
            peers = resp.getNewPeersList();

            return null;
        });
    }

    @Override public CompletableFuture<Void> removePeers(Collection<PeerId> peerIds) {
        return null;
    }

    @Override public CompletableFuture<Void> addLearners(Collection<PeerId> learners) {
        return null;
    }

    @Override public CompletableFuture<Void> removeLearners(Collection<PeerId> learners) {
        return null;
    }

    @Override public CompletableFuture<Void> snapshot(PeerId peerId) {
        return null;
    }

    @Override public CompletableFuture<Void> transferLeadership(PeerId newLeader) {
        return null;
    }

    @Override public <R> CompletableFuture<R> run(Command cmd) {
        return sendWithRetry(factory.createUserRequest().setRequest(cmd).setGroupId(groupId).build());
    }

    @Override public <R> CompletableFuture<R> run(PeerId peerId, ReadCommand cmd) {
        UserRequest req = factory.createUserRequest().setRequest(cmd).setGroupId(groupId).build();

        return cluster.sendWithResponse(peerId.getNode(), req, timeout);
    }

    private <R> CompletableFuture<R> sendWithRetry(Object req) {
        PeerId leader = this.leader;

        CompletableFuture<Void> refreshLeaderFut = leader == null ? refreshLeader() : FINISHED_FUT;

        // TODO retries.
        return refreshLeaderFut.<UserResponse<R>>
            thenCompose(peerId -> cluster.sendWithResponse(leader.getNode(), req, timeout)).
            thenApply(resp -> resp.response());
    }

    private void info(String msg, Object... params) {
        LOG.log(System.Logger.Level.INFO, msg, params);
    }

    private void warn(String msg, Object... params) {
        LOG.log(System.Logger.Level.WARNING, msg, params);
    }

    private void error(String msg, Object... params) {
        LOG.log(System.Logger.Level.ERROR, msg, params);
    }

    private void error(String msg, Exception e) {
        LOG.log(System.Logger.Level.ERROR, msg, e);
    }
}
