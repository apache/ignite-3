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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Wrapper for {@link ThreadPoolExecutor}. Allows removing task from work queue.
 */
public class ComputeThreadPoolExecutor {

    private final BlockingQueue<Runnable> workQueue;

    private final ThreadPoolExecutor executor;

    ComputeThreadPoolExecutor(int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            ThreadFactory threadFactory) {
        this.workQueue = workQueue;
        executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    /**
     * Executes the given task sometime in the future.
     * {@link ThreadPoolExecutor#execute(Runnable command)}
     */
    public void execute(Runnable command) {
        executor.execute(command);
    }

    /**
     * Removes this task from the executor's internal queue if it is present.
     * {@link ThreadPoolExecutor#remove(Runnable command)}
     */
    public boolean remove(Runnable task) {
        return executor.remove(task);
    }

    /**
     * Removes this task from the executor's internal queue if it is
     * present, thus causing it not to be run if it has not already
     * started.
     *
     * @param task the task to remove
     * @return {@code true} if the task was removed
     */
    public boolean removeFromQueue(Runnable task) {
        return workQueue.remove(task);
    }

    /**
     * Shuts down the given executor service gradually, first disabling new submissions and later, if necessary, cancelling remaining
     * tasks.
     *
     * {@link IgniteUtils#shutdownAndAwaitTermination}
     */
    public void shutdown(long stopTimeout) {
        IgniteUtils.shutdownAndAwaitTermination(executor, stopTimeout, TimeUnit.MILLISECONDS);
    }
}
