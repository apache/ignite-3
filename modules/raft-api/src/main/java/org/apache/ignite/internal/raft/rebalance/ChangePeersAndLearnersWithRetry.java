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

package org.apache.ignite.internal.raft.rebalance;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.util.IgniteBusyLock;
import org.jetbrains.annotations.Nullable;

/**
 * Helper class that executes change peers and learners async with retries.
 */
public class ChangePeersAndLearnersWithRetry {

    private final RaftCommandWithRetry raftCommand;

    private final Supplier<CompletableFuture<RaftGroupService>> raftGroupServiceSupplier;

    /**
     * Creates a new instance of ChangePeersAndLearnersWithRetry.
     *
     * @param busyLock The busy lock.
     * @param rebalanceScheduler The scheduler for rebalance tasks.
     * @param raftGroupServiceSupplier The supplier of raft group service.
     */
    public ChangePeersAndLearnersWithRetry(
            IgniteBusyLock busyLock,
            ScheduledExecutorService rebalanceScheduler,
            Supplier<CompletableFuture<RaftGroupService>> raftGroupServiceSupplier
    ) {
        this.raftGroupServiceSupplier = raftGroupServiceSupplier;

        raftCommand = new RaftCommandWithRetry(busyLock, rebalanceScheduler);
    }

    /**
     * Performs {@link RaftGroupService#changePeersAndLearnersAsync} on a provided raft group service of a partition, so nodes of the
     * corresponding raft group can be reconfigured. Retry mechanism is applied to repeat
     * {@link RaftGroupService#changePeersAndLearnersAsync} if previous one failed with some exception.
     *
     * @return Function which performs {@link RaftGroupService#changePeersAndLearnersAsync}.
     */
    public CompletableFuture<Void> execute(
            PeersAndLearners peersAndLearners,
            long sequenceToken,
            Function<RaftGroupService, CompletableFuture<@Nullable RaftWithTerm>> leaderFilter) {

        return raftCommand.execute(() ->
                raftGroupServiceSupplier
                        .get()
                        .thenCompose(leaderFilter)
                        .thenCompose(raftWithTerm -> {
                            if (raftWithTerm == null) {
                                return nullCompletedFuture();
                            }

                            return raftWithTerm.raftClient()
                                    .changePeersAndLearnersAsync(peersAndLearners, raftWithTerm.term(), sequenceToken);
                        }));
    }

    /**
     * Performs {@link RaftGroupService#changePeersAndLearnersAsync} on a provided raft group service of a partition, so nodes of the
     * corresponding raft group can be reconfigured. Retry mechanism is applied to repeat
     * {@link RaftGroupService#changePeersAndLearnersAsync} if previous one failed with some exception.
     *
     * @return Function which performs {@link RaftGroupService#changePeersAndLearnersAsync}.
     */
    public CompletableFuture<Void> executeOnLeader(PeersAndLearners peersAndLearners, long term, long sequenceToken) {
        return raftCommand.execute(() ->
                raftGroupServiceSupplier
                        .get()
                        .thenCompose(raftClient -> {
                            if (raftClient == null) {
                                return nullCompletedFuture();
                            }

                            return raftClient.changePeersAndLearnersAsync(peersAndLearners, term, sequenceToken);
                        }));
    }
}
