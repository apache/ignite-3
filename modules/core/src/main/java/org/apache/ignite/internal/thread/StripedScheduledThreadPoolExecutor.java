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

package org.apache.ignite.internal.thread;

import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * An {@link ScheduledExecutorService} that executes submitted tasks using pooled grid threads.
 */
public class StripedScheduledThreadPoolExecutor extends AbstractStripedThreadPoolExecutor<ScheduledExecutorService>
        implements ScheduledExecutorService {
    /**
     * Create striped scheduled thread pool.
     *
     * @param concurrencyLvl Concurrency level.
     * @param threadFactory The factory to use when the executor creates a new thread.
     * @param executionHandler The handler to use when execution is blocked
     *        because the thread bounds and queue capacities are reached.
     */
    public StripedScheduledThreadPoolExecutor(
            int concurrencyLvl,
            ThreadFactory threadFactory,
            RejectedExecutionHandler executionHandler
    ) {
        super(createExecutors(concurrencyLvl, threadFactory, executionHandler));
    }

    private static ScheduledExecutorService[] createExecutors(
            int concurrencyLvl,
            ThreadFactory threadFactory,
            RejectedExecutionHandler executionHandler
    ) {
        ScheduledExecutorService[] execs = new ScheduledExecutorService[concurrencyLvl];

        for (int i = 0; i < concurrencyLvl; i++) {
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
                    1,
                    threadFactory,
                    executionHandler
            );

            execs[i] = executor;
        }

        return execs;
    }

    /**
     * Executes the given command at some time in the future. The command with the same {@code index} will be executed in the same thread.
     *
     * @param command The task to execute.
     * @param delay The time from now to delay execution.
     * @param unit The time unit of the delay parameter.
     * @param idx Striped index.
     * @return A ScheduledFuture representing pending completion of the task and whose {@code get()} method will return
     *         {@code null} upon completion.
     * @throws RejectedExecutionException If the task cannot be scheduled for execution.
     * @throws NullPointerException If command or unit is null.
     */
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit, int idx) {
        return stripeExecutor(idx).schedule(command, delay, unit);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }
}
