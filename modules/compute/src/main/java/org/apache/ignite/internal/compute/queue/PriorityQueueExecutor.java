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

package org.apache.ignite.internal.compute.queue;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Compute job executor with priority mechanism.
 */
public class PriorityQueueExecutor {
    private static final long THREAD_KEEP_ALIVE_SECONDS = 60;

    private final ComputeConfiguration configuration;

    private final ThreadPoolExecutor executor;

    /**
     * Constructor.
     *
     * @param configuration Compute configuration.
     * @param threadFactory Thread factory.
     */
    public PriorityQueueExecutor(
            ComputeConfiguration configuration,
            ThreadFactory threadFactory
    ) {
        this.configuration = configuration;
        executor = new ThreadPoolExecutor(
                configuration.threadPoolSize().value(),
                configuration.threadPoolSize().value(),
                THREAD_KEEP_ALIVE_SECONDS,
                TimeUnit.SECONDS,
                new PriorityBlockingQueue<>(),
                threadFactory
        );
    }

    /**
     * Submit job for execution. Job can be started immediately if queue is empty or will be added to queue with provided priority.
     *
     * @param job Execute job callable.
     * @param priority Job priority.
     * @param <R> Job result type.
     * @return Completable future which will be finished when compute job finished.
     */
    public <R> CompletableFuture<R> submit(Callable<R> job, int priority) {
        Objects.requireNonNull(job);

        QueueEntry<R> queueEntry = new QueueEntry<>(job, priority);
        executor.execute(queueEntry);
        return queueEntry.toFuture();
    }

    /**
     * Submit job for execution. Job can be started immediately if queue is empty or will be added to queue with default priority.
     *
     * @param job Execute job callable.
     * @param <R> Job result type.
     * @return Completable future which will be finished when compute job finished.
     */
    public <R> CompletableFuture<R> submit(Callable<R> job) {
        return submit(job, 0);
    }

    /**
     * Shutdown executor. After shutdown executor is not usable anymore.
     */
    public void shutdown() {
        Long stopTimeout = configuration.threadPoolStopTimeoutMillis().value();
        IgniteUtils.shutdownAndAwaitTermination(executor, stopTimeout, TimeUnit.MILLISECONDS);
    }
}
