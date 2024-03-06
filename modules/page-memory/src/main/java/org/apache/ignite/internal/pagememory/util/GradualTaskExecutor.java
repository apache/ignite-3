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

package org.apache.ignite.internal.pagememory.util;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.future.InFlightFutures;

/**
 * Executor for {@link GradualTask}s. See GradualTask documentation for details.
 *
 * <p>This executor owns an executor service that it is passed, so it shuts it down when being closed.
 *
 * @see GradualTask
 */
public class GradualTaskExecutor implements ManuallyCloseable {
    private final ExecutorService executor;

    private final InFlightFutures inFlightFutures = new InFlightFutures();

    private volatile boolean cancelled = false;

    public GradualTaskExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    /**
     * Starts execution of a {@link GradualTask} and returns a future that completes when the task completes.
     *
     * @param task Task to execute.
     * @return Future that completes when the task completes.
     */
    public CompletableFuture<Void> execute(GradualTask task) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        inFlightFutures.registerFuture(future);

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    if (cancelled) {
                        future.completeExceptionally(new CancellationException("The executor has been closed"));

                        return;
                    }

                    if (task.isCompleted()) {
                        future.complete(null);

                        return;
                    }

                    runStep(task);

                    if (task.isCompleted()) {
                        future.complete(null);
                    } else if (cancelled) {
                        future.completeExceptionally(new CancellationException("The executor has been closed"));
                    } else {
                        executor.execute(this);
                    }
                } catch (Error e) {
                    future.completeExceptionally(e);

                    throw e;
                } catch (Exception e) {
                    future.completeExceptionally(e);
                }
            }
        };

        executor.execute(runnable);

        return future.whenComplete((res, ex) -> task.cleanup());
    }

    /**
     * Runs a single step from the given {@code task}.
     */
    protected void runStep(GradualTask task) throws Exception {
        task.runStep();
    }

    @Override
    public void close() {
        cancelled = true;

        inFlightFutures.cancelInFlightFutures();
    }

    /**
     * Returns the internal thread pool that is actually used to execute tasks.
     */
    public ExecutorService executorService() {
        return executor;
    }
}
