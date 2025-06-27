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

package org.apache.ignite.internal.distributionzones.rebalance;

import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.recoverable;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/**
 * Class for moving partitions.
 */
public class PartitionMover {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(PartitionMover.class);

    private final IgniteSpinBusyLock busyLock;

    private final Supplier<CompletableFuture<RaftGroupService>> raftGroupServiceSupplier;

    /**
     * Constructor.
     */
    public PartitionMover(IgniteSpinBusyLock busyLock, Supplier<CompletableFuture<RaftGroupService>> raftGroupServiceSupplier) {
        this.busyLock = busyLock;
        this.raftGroupServiceSupplier = raftGroupServiceSupplier;
    }

    /**
     * Performs {@link RaftGroupService#changePeersAndLearnersAsync} on a provided raft group service of a partition, so nodes of the
     * corresponding raft group can be reconfigured. Retry mechanism is applied to repeat
     * {@link RaftGroupService#changePeersAndLearnersAsync} if previous one failed with some exception.
     *
     * @return Function which performs {@link RaftGroupService#changePeersAndLearnersAsync}.
     */
    public CompletableFuture<Void> movePartition(PeersAndLearners peersAndLearners, long term) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            return raftGroupServiceSupplier
                    .get()
                    .thenCompose(raftGroupService -> raftGroupService.changePeersAndLearnersAsync(peersAndLearners, term))
                    .handle((resp, err) -> {
                        if (!busyLock.enterBusy()) {
                            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
                        }

                        try {
                            if (err != null) {
                                if (recoverable(err)) {
                                    LOG.debug("Recoverable error received during changePeersAndLearnersAsync invocation, retrying", err);
                                } else {
                                    // TODO: IGNITE-19087 Ideally, rebalance, which has initiated this invocation should be canceled,
                                    // TODO: Also it might be reasonable to delegate such exceptional case to a general failure handler.
                                    // TODO: At the moment, we repeat such intents as well.
                                    LOG.debug("Unrecoverable error received during changePeersAndLearnersAsync invocation, retrying", err);
                                }

                                return movePartition(peersAndLearners, term);
                            }

                            return CompletableFutures.<Void>nullCompletedFuture();
                        } finally {
                            busyLock.leaveBusy();
                        }
                    })
                    .thenCompose(Function.identity());
        } finally {
            busyLock.leaveBusy();
        }
    }
}
