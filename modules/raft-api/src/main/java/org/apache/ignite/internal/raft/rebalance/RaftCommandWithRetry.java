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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.raft.rebalance.ExceptionUtils.recoverable;
import static org.apache.ignite.internal.util.CompletableFutures.copyStateTo;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.IgniteBusyLock;

/**
 * Helper class that executes raft command with retries.
 */
public class RaftCommandWithRetry {
    private static final IgniteLogger LOG = Loggers.forClass(RaftCommandWithRetry.class);

    private static final long MOVE_RESCHEDULE_DELAY_MILLIS = 100;

    private final IgniteBusyLock busyLock;

    private final ScheduledExecutorService rebalanceScheduler;

    /**
     * Creates a new instance of RaftCommandWithRetry.
     *
     * @param busyLock The busy lock.
     * @param rebalanceScheduler The scheduler for rebalance tasks.
     */
    public RaftCommandWithRetry(
            IgniteBusyLock busyLock,
            ScheduledExecutorService rebalanceScheduler
    ) {
        this.busyLock = busyLock;
        this.rebalanceScheduler = rebalanceScheduler;
    }

    /**
     * Performs {@link RaftCommand} with retry. Retry mechanism is applied to repeat {@link RaftCommand} if previous one failed with some
     * exception.
     *
     * @return Function which performs {@link RaftCommand}.
     */
    public CompletableFuture<Void> execute(RaftCommand raftCommand) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            return raftCommand.execute()
                    .handle((resp, err) -> {
                        if (!busyLock.enterBusy()) {
                            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
                        }

                        try {
                            if (err != null) {
                                return handleError(raftCommand, err);
                            }

                            return CompletableFutures.<Void>nullCompletedFuture();
                        } finally {
                            busyLock.leaveBusy();
                        }
                    })
                    .thenCompose(Function.identity());
        } catch (Throwable ex) {
            return handleError(raftCommand, ex);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Void> handleError(RaftCommand raftCommand, Throwable err) {
        if (recoverable(err)) {
            LOG.debug("Recoverable error received during raft command invocation, retrying.", err);
        } else {
            // TODO: IGNITE-19087 Ideally, rebalance, which has initiated this invocation should be canceled,
            // TODO: Also it might be reasonable to delegate such exceptional case to a general failure handler.
            // TODO: At the moment, there is only one type of unrecoverable error - stale configuration update.
            LOG.debug(
                    "Unrecoverable error received during raft command invocation. Stop retrying.",
                    err
            );
            return failedFuture(err);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();

        // We don't bother with ScheduledFuture as the delay is very short, so it will not delay the scheduler
        // stop for long.
        rebalanceScheduler.schedule(() -> {
            execute(raftCommand).whenComplete(copyStateTo(future));
        }, MOVE_RESCHEDULE_DELAY_MILLIS, MILLISECONDS);

        return future;
    }
}
