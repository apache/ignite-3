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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.jetbrains.annotations.Nullable;

/**
 * An implementation of {@link RaftGroupService} that delegates all calls to the provided {@code raftClient}.
 */
public class WrappedRaftGroupService implements RaftGroupService {

    /** RPC RAFT client. */
    private final RaftGroupService raftClient;

    public WrappedRaftGroupService(RaftGroupService raftClient) {
        this.raftClient = raftClient;
    }

    @Override
    public ReplicationGroupId groupId() {
        return raftClient.groupId();
    }

    @Override
    public @Nullable Peer leader() {
        return raftClient.leader();
    }

    @Override
    public List<Peer> peers() {
        return raftClient.peers();
    }

    @Override
    public @Nullable List<Peer> learners() {
        return raftClient.learners();
    }

    @Override
    public CompletableFuture<Void> refreshLeader() {
        return raftClient.refreshLeader();
    }

    @Override
    public CompletableFuture<LeaderWithTerm> refreshAndGetLeaderWithTerm() {
        return raftClient.refreshAndGetLeaderWithTerm();
    }

    @Override
    public CompletableFuture<Void> refreshMembers(boolean onlyAlive) {
        return raftClient.refreshMembers(onlyAlive);
    }

    @Override
    public CompletableFuture<Void> addPeer(Peer peer, long sequenceToken) {
        return raftClient.addPeer(peer, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> removePeer(Peer peer, long sequenceToken) {
        return raftClient.removePeer(peer, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> changePeersAndLearners(PeersAndLearners peersAndLearners, long term, long sequenceToken) {
        return raftClient.changePeersAndLearners(peersAndLearners, term, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> changePeersAndLearnersAsync(PeersAndLearners peersAndLearners, long term, long sequenceToken) {
        return raftClient.changePeersAndLearnersAsync(peersAndLearners, term, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> addLearners(Collection<Peer> learners, long sequenceToken) {
        return raftClient.addLearners(learners, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> removeLearners(Collection<Peer> learners, long sequenceToken) {
        return raftClient.removeLearners(learners, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> resetLearners(Collection<Peer> learners, long sequenceToken) {
        return raftClient.resetLearners(learners, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> snapshot(Peer peer, boolean forced) {
        return raftClient.snapshot(peer, forced);
    }

    @Override
    public CompletableFuture<Void> transferLeadership(Peer newLeader) {
        return raftClient.transferLeadership(newLeader);
    }

    @Override
    public void shutdown() {
        raftClient.shutdown();
    }

    @Override
    public CompletableFuture<Long> readIndex() {
        return raftClient.readIndex();
    }

    @Override
    public ClusterService clusterService() {
        return raftClient.clusterService();
    }

    @Override
    public void updateConfiguration(PeersAndLearners configuration) {
        raftClient.updateConfiguration(configuration);
    }

    @Override
    public <R> CompletableFuture<R> run(Command cmd) {
        return raftClient.run(cmd);
    }

    @Override
    public <R> CompletableFuture<R> run(Command cmd, long timeoutMillis) {
        return raftClient.run(cmd, timeoutMillis);
    }

    @Override
    public void markAsStopping() {
        raftClient.markAsStopping();
    }

    protected RaftGroupService raftClient() {
        return raftClient;
    }
}
