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

package org.apache.ignite.internal.metastorage.impl;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.TestOnly;

/**
 * Meta Storage learner manager.
 */
class MetaStorageLearnerManager {
    private static final IgniteLogger LOG = Loggers.forClass(MetaStorageLearnerManager.class);

    private final IgniteSpinBusyLock busyLock;

    private final LogicalTopologyService logicalTopologyService;

    private final FailureProcessor failureProcessor;

    private final CompletableFuture<MetaStorageServiceImpl> metaStorageSvcFut;

    private volatile boolean learnersAdditionEnabled = true;

    MetaStorageLearnerManager(
            IgniteSpinBusyLock busyLock,
            LogicalTopologyService logicalTopologyService,
            FailureProcessor failureProcessor,
            CompletableFuture<MetaStorageServiceImpl> metaStorageSvcFut
    ) {
        this.busyLock = busyLock;
        this.logicalTopologyService = logicalTopologyService;
        this.failureProcessor = failureProcessor;
        this.metaStorageSvcFut = metaStorageSvcFut;
    }

    CompletableFuture<Void> updateLearners(long term) {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-26854.
        return metaStorageSvcFut.thenCompose(service -> resetLearners(service.raftGroupService(), term, 0));
    }

    CompletableFuture<Void> addLearner(RaftGroupService raftService, InternalClusterNode learner) {
        if (!learnersAdditionEnabled) {
            return nullCompletedFuture();
        }

        return updateConfigUnderLock(() -> isPeer(raftService, learner)
                ? nullCompletedFuture() // TODO: https://issues.apache.org/jira/browse/IGNITE-26854.
                : raftService.addLearners(List.of(new Peer(learner.name())), 0));
    }

    private static boolean isPeer(RaftGroupService raftService, InternalClusterNode node) {
        return raftService.peers().stream().anyMatch(peer -> peer.consistentId().equals(node.name()));
    }

    CompletableFuture<Void> removeLearner(RaftGroupService raftService, InternalClusterNode learner) {
        return updateConfigUnderLock(() -> logicalTopologyService.validatedNodesOnLeader()
                .thenCompose(validatedNodes -> updateConfigUnderLock(() -> {
                    if (isPeer(raftService, learner)) {
                        return nullCompletedFuture();
                    }

                    // Due to possible races, we can have multiple versions of the same node in the validated set. We only remove
                    // a learner if there are no such versions left.
                    if (validatedNodes.stream().anyMatch(n -> n.name().equals(learner.name()))) {
                        return nullCompletedFuture();
                    }

                    // TODO: https://issues.apache.org/jira/browse/IGNITE-26854.
                    return raftService.removeLearners(List.of(new Peer(learner.name())), 0);
                })));
    }

    CompletableFuture<Void> resetLearners(RaftGroupService raftService, long term, long sequenceToken) {
        return updateConfigUnderLock(() -> logicalTopologyService.validatedNodesOnLeader()
                .thenCompose(validatedNodes -> updateConfigUnderLock(() -> {
                    Set<String> peers = raftService.peers().stream().map(Peer::consistentId).collect(toSet());

                    Set<String> learners = validatedNodes.stream()
                            .map(InternalClusterNode::name)
                            .filter(name -> !peers.contains(name))
                            .collect(toSet());

                    PeersAndLearners newPeerConfiguration = PeersAndLearners.fromConsistentIds(peers, learners);

                    // We can't use 'resetLearners' call here because it does not support empty lists of learners.
                    return raftService.changePeersAndLearnersAsync(newPeerConfiguration, term, sequenceToken);
                })));
    }

    private CompletableFuture<Void> updateConfigUnderLock(Supplier<CompletableFuture<Void>> action) {
        if (!busyLock.enterBusy()) {
            LOG.info("Skipping Meta Storage configuration update because the node is stopping");

            return nullCompletedFuture();
        }

        try {
            return action.get()
                    .whenComplete((v, e) -> {
                        if (e != null) {
                            if (!hasCause(e, NodeStoppingException.class)) {
                                failureProcessor.process(new FailureContext(e, "Unable to change peers on topology update"));
                            }
                        }
                    });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Disables addition of learners one by one (as a reaction to nodes joining the validated nodes set).
     *
     * <p>This does NOT affect other ways of changing the learners.
     *
     * <p>This is only used by test code, and there is no method for enabling learners addition back as this is not needed in our tests.
     */
    @TestOnly
    void disableLearnersAddition() {
        learnersAdditionEnabled = false;
    }
}
