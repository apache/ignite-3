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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Executor {@link #awaitPendingTasksFinished() waiting for the completion} of tasks added via {@link #execute(Runnable)}.
 *
 * <p>Not thread-safe.
 */
class AwaitTasksCompletionExecutor implements Executor {
    private final Executor executor;

    private final Runnable updateHeartbeat;

    private List<CompletableFuture<?>> pendingTaskFutures = new ArrayList<>();

    /**
     * Constructor.
     *
     * @param executor Executor in which the tasks will be performed.
     * @param updateHeartbeat Update heartbeat callback that will be executed on completion of each task.
     */
    AwaitTasksCompletionExecutor(Executor executor, Runnable updateHeartbeat) {
        this.executor = executor;
        this.updateHeartbeat = updateHeartbeat;
    }

    /** {@inheritDoc} */
    @Override
    public void execute(Runnable command) {
        CompletableFuture<?> future = new CompletableFuture<>();

        CompletableFuture<?> heartbeatFuture = future.whenComplete((o, throwable) -> updateHeartbeat.run());

        pendingTaskFutures.add(heartbeatFuture);

        executor.execute(() -> {
            try {
                command.run();

                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
    }

    /**
     * Await all async tasks from executor have finished.
     */
    void awaitPendingTasksFinished() {
        List<CompletableFuture<?>> futures = pendingTaskFutures;

        if (!futures.isEmpty()) {
            pendingTaskFutures = new ArrayList<>();

            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        }
    }
}
