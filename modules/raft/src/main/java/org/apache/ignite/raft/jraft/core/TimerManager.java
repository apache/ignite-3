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
package org.apache.ignite.raft.jraft.core;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.raft.jraft.util.ThreadPoolUtil;

/**
 * The global timer manager.
 */
public class TimerManager implements Scheduler {
    private static final IgniteLogger LOG = Loggers.forClass(TimerManager.class);

    private final ScheduledExecutorService executor;

    public TimerManager(int workerNum) {
        this(workerNum, "JRaft-Node-ScheduleThreadPool");
    }

    public TimerManager(int workerNum, String name) {
        this.executor = ThreadPoolUtil.newScheduledBuilder() //
            .poolName(name) //
            .coreThreads(workerNum) //
            .enableMetric(true) //
            .threadFactory(IgniteThreadFactory.createWithFixedPrefix(name, true, LOG)) //
            .build();
    }

    @Override
    public ScheduledFuture<?> schedule(final Runnable command, final long delay, final TimeUnit unit) {
        return this.executor.schedule(command, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command, final long initialDelay, final long period,
        final TimeUnit unit) {
        return this.executor.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command, final long initialDelay, final long delay,
        final TimeUnit unit) {
        return this.executor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    @Override
    public void shutdown() {
        this.executor.shutdownNow();
    }
}
