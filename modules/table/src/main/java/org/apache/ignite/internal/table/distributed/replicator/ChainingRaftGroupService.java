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

package org.apache.ignite.internal.table.distributed.replicator;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.network.ClusterService;
import org.jetbrains.annotations.Nullable;

/**
 * {@link RaftGroupService} that chains all {@link #run(Command)} invocations one after another.
 */
public class ChainingRaftGroupService implements RaftGroupService {
    private final RaftGroupService service;

    private CompletableFuture<?> runChainHead = completedFuture(null);

    public ChainingRaftGroupService(RaftGroupService service) {
        this.service = service;
    }

    @Override
    public ReplicationGroupId groupId() {
        return service.groupId();
    }

    @Override
    public @Nullable Peer leader() {
        return service.leader();
    }

    @Override
    public @Nullable List<Peer> peers() {
        return service.peers();
    }

    @Override
    public @Nullable List<Peer> learners() {
        return service.learners();
    }

    @Override
    public CompletableFuture<Void> refreshLeader() {
        return service.refreshLeader();
    }

    @Override
    public CompletableFuture<LeaderWithTerm> refreshAndGetLeaderWithTerm() {
        return service.refreshAndGetLeaderWithTerm();
    }

    @Override
    public CompletableFuture<Void> refreshMembers(boolean onlyAlive) {
        return service.refreshMembers(onlyAlive);
    }

    @Override
    public CompletableFuture<Void> addPeer(Peer peer) {
        return service.addPeer(peer);
    }

    @Override
    public CompletableFuture<Void> removePeer(Peer peer) {
        return service.removePeer(peer);
    }

    @Override
    public CompletableFuture<Void> changePeers(Collection<Peer> peers) {
        return service.changePeers(peers);
    }

    @Override
    public CompletableFuture<Void> changePeersAsync(PeersAndLearners peersAndLearners, long term) {
        return service.changePeersAsync(peersAndLearners, term);
    }

    @Override
    public CompletableFuture<Void> addLearners(Collection<Peer> learners) {
        return service.addLearners(learners);
    }

    @Override
    public CompletableFuture<Void> removeLearners(Collection<Peer> learners) {
        return service.removeLearners(learners);
    }

    @Override
    public CompletableFuture<Void> resetLearners(Collection<Peer> learners) {
        return service.resetLearners(learners);
    }

    @Override
    public CompletableFuture<Void> snapshot(Peer peer) {
        return service.snapshot(peer);
    }

    @Override
    public CompletableFuture<Void> transferLeadership(Peer newLeader) {
        return service.transferLeadership(newLeader);
    }

    @Override
    public synchronized <R> CompletableFuture<R> run(Command cmd) {
        CompletableFuture<R> newFuture = runChainHead.thenCompose(unused -> service.run(cmd));

        runChainHead = newFuture;

        return newFuture;
    }

    @Override
    public void shutdown() {
        service.shutdown();
    }

    @Override
    public CompletableFuture<Long> readIndex() {
        return service.readIndex();
    }

    @Override
    public ClusterService clusterService() {
        return service.clusterService();
    }
}
