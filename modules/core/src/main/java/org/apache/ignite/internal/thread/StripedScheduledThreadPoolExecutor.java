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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.IgniteLogger;

public class StripedScheduledThreadPoolExecutor extends AbstractStripedThreadPoolExecutor<ScheduledExecutorService> implements ScheduledExecutorService {
    /**
     * Create striped thread pool.
     *
     * @param concurrentLvl Concurrency level.
     */
    public StripedScheduledThreadPoolExecutor(
            int concurrentLvl,
            String nodeName,
            String poolName,
            IgniteLogger log) {
        super(createExecutors(concurrentLvl, nodeName, poolName, log));
    }

    private static ScheduledExecutorService[] createExecutors(
            int concurrentLvl,
            String nodeName,
            String poolName,
            IgniteLogger log) {
        ScheduledExecutorService[] execs = new ScheduledExecutorService[concurrentLvl];

        for (int i = 0; i < concurrentLvl; i++) {
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
                    1,
                    new NamedThreadFactory(NamedThreadFactory.threadPrefix(nodeName, poolName + "-1"), log),
                    new ThreadPoolExecutor.DiscardPolicy()
            );

            execs[i] = executor;
        }

        return execs;
    }

    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit, int idx) {
        return commandExecutor(idx).schedule(command, delay, unit);
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
