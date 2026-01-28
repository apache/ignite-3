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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricSource;
import org.apache.ignite.internal.metrics.sources.ThreadPoolMetricSource;
import org.jetbrains.annotations.Nullable;

/**
 * Single thread executor instrumented to be used as a {@link CriticalWorker} and being monitored by the {@link CriticalWorkerWatchdog}.
 * Must be registered with the watchdog explicitly.
 */
public class CriticalSingleThreadExecutor extends ThreadPoolExecutor implements CriticalWorker {

    private volatile Thread lastSeenThread;
    private volatile long heartbeatNanos = NOT_MONITORED;

    private @Nullable MetricSource metricSource;
    private @Nullable MetricManager metricManager;

    /** Constructor. */
    public CriticalSingleThreadExecutor(ThreadFactory threadFactory) {
        this(0, SECONDS, new LinkedBlockingQueue<>(), threadFactory);
    }

    /** Constructor. */
    public CriticalSingleThreadExecutor(long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(1, 1, keepAliveTime, unit, workQueue, threadFactory);
    }

    /**
     * Initialize the metric source to track this thread pool's metrics.
     *
     * @param metricManager the metric manager used to register the source.
     * @param name the name of the metric.
     * @param description the metric description.
     */
    public void initMetricSource(MetricManager metricManager, String name, String description) {
        if (this.metricManager == null) {
            this.metricManager = metricManager;

            metricSource = new ThreadPoolMetricSource(name, description, null, this);

            metricManager.registerSource(metricSource);
            metricManager.enable(metricSource);
        }
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        lastSeenThread = t;
        heartbeatNanos = System.nanoTime();

        super.beforeExecute(t, r);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        try {
            super.afterExecute(r, t);
        } finally {
            heartbeatNanos = NOT_MONITORED;
        }
    }

    @Override
    public long threadId() {
        Thread thread = lastSeenThread;

        assert thread != null;

        return thread.getId();
    }

    @Override
    public long heartbeatNanos() {
        return heartbeatNanos;
    }

    @Override
    public void shutdown() {
        if (metricManager != null) {
            assert metricSource != null;

            if (metricManager.metricSources().contains(metricSource)) {
                metricManager.unregisterSource(metricSource);
            }
        }

        super.shutdown();
    }
}
