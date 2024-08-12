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

package org.apache.ignite.internal.raft;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.function.Function.identity;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.thread.PublicApiThreading;

/**
 * Decorates a {@link RaftCommandRunner} to make sure that completion stages depending on the returned futures are always completed
 * either using the provided {@link Executor} or in the thread that executed the addition of the corresponding stage to the returned future.
 */
public class ExecutorInclinedRaftCommandRunner implements RaftCommandRunner {
    private final RaftCommandRunner commandRunner;

    private final Executor completionExecutor;

    public ExecutorInclinedRaftCommandRunner(RaftCommandRunner commandRunner, Executor completionExecutor) {
        this.commandRunner = commandRunner;
        this.completionExecutor = completionExecutor;
    }

    @Override
    public <R> CompletableFuture<R> run(Command cmd) {
        CompletableFuture<R> future = commandRunner.run(cmd);
        if (future.isDone()) {
            return future;
        }

        // We can wait for replication completion right here, because client thread waits the entire operation in synchronous API in any
        // case. Moreover, this code guarantees that the rest of the operation will execute outside a replication thread.
        if (PublicApiThreading.executingSyncPublicApi()) {
            try {
                return completedFuture(future.get());
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        return future.thenApplyAsync(identity(), completionExecutor);
    }

    /** Returns decorated Raft-client. */
    public RaftCommandRunner decoratedCommandRunner() {
        return commandRunner;
    }
}
