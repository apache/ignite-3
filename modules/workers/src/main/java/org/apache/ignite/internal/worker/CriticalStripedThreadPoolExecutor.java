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

package org.apache.ignite.internal.worker;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toUnmodifiableList;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.internal.thread.AbstractStripedThreadPoolExecutor;
import org.apache.ignite.internal.thread.StripedExecutor;
import org.apache.ignite.internal.thread.StripedThreadPoolExecutor;

/**
 * Same as {@link StripedThreadPoolExecutor}, but each stripe is a critical worker monitored for being blocked.
 */
public class CriticalStripedThreadPoolExecutor extends AbstractStripedThreadPoolExecutor<ExecutorService> implements StripedExecutor {
    /**
     * Create a blockage-monitored striped thread pool.
     *
     * @param concurrencyLvl          Concurrency level.
     * @param threadFactory Factory used to create threads.
     * @param allowCoreThreadTimeOut Sets the policy governing whether core threads may time out and terminate if no tasks arrive within the
     *                               keep-alive time.
     * @param keepAliveTime          When the number of threads is greater than the core, this is the maximum time that excess idle threads
     *                               will wait for new tasks before terminating.
     */
    public CriticalStripedThreadPoolExecutor(
            int concurrencyLvl,
            ThreadFactory threadFactory,
            boolean allowCoreThreadTimeOut,
            long keepAliveTime) {
        super(createExecutors(concurrencyLvl, threadFactory, allowCoreThreadTimeOut, keepAliveTime));
    }

    private static ExecutorService[] createExecutors(
            int concurrencyLvl,
            ThreadFactory threadFactory,
            boolean allowCoreThreadTimeOut,
            long keepAliveTime) {
        ExecutorService[] execs = new ExecutorService[concurrencyLvl];

        for (int i = 0; i < concurrencyLvl; i++) {
            ThreadPoolExecutor executor = new CriticalSingleThreadExecutor(
                    keepAliveTime,
                    MILLISECONDS,
                    new LinkedBlockingQueue<>(),
                    threadFactory
            );

            executor.allowCoreThreadTimeOut(allowCoreThreadTimeOut);

            execs[i] = executor;
        }

        return execs;
    }

    /**
     * Returns workers corresponding to this thread pool.
     */
    public Collection<CriticalWorker> workers() {
        return Arrays.stream(execs)
                .map(CriticalWorker.class::cast)
                .collect(toUnmodifiableList());
    }
}
