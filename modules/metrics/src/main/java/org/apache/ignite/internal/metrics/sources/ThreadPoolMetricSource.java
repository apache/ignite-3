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

package org.apache.ignite.internal.metrics.sources;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.sources.ThreadPoolMetricSource.Holder;
import org.jetbrains.annotations.Nullable;

/** Metric source for monitoring of {@link ThreadPoolExecutor}. */
public class ThreadPoolMetricSource extends AbstractMetricSource<Holder> {
    /** Default source group for a thread pools. */
    public static final String THREAD_POOLS_GROUP_NAME = "thread.pools";

    /** Default prefix source name for a thread pools. */
    public static final String THREAD_POOLS_METRICS_SOURCE_NAME = "thread.pools.";

    /** Thread pool to be monitored. */
    private final ThreadPoolExecutor exec;

    /**
     * Creates a new thread pool metric source with the given {@code name} to monitor the provided executor {@code exec},
     * using the default {@link #THREAD_POOLS_GROUP_NAME} group.
     *
     * @param name Metric source name.
     * @param description Metric source description.
     * @param exec Thread pool executor to monitor.
     */
    public ThreadPoolMetricSource(String name, @Nullable String description, ThreadPoolExecutor exec) {
        this(name, description, THREAD_POOLS_GROUP_NAME, exec);
    }

    /**
     * Creates a new thread pool metric source with the given {@code name} to monitor the provided executor {@code exec}.
     *
     * @param name Metric source name.
     * @param description Metric source description.
     * @param group Metric source group.
     * @param exec Thread pool executor to monitor.
     */
    public ThreadPoolMetricSource(String name, @Nullable String description, @Nullable String group, ThreadPoolExecutor exec) {
        super(name, description, group);

        this.exec = exec;
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    /** Holder class. */
    protected class Holder implements AbstractMetricSource.Holder<Holder> {
        private final IntGauge activeCount = new IntGauge(
                "ActiveCount",
                "Approximate number of threads that are actively executing tasks.",
                exec::getActiveCount
        );

        private final LongGauge completedTaskCount = new LongGauge(
                "CompletedTaskCount",
                "Approximate total number of tasks that have completed execution.",
                exec::getCompletedTaskCount
        );

        private final IntGauge corePoolSize = new IntGauge(
                "CorePoolSize",
                "The core number of threads.",
                exec::getCorePoolSize
        );

        private final IntGauge largestPoolSize = new IntGauge(
                "LargestPoolSize",
                "Largest number of threads that have ever simultaneously been in the pool.",
                exec::getLargestPoolSize
        );

        private final IntGauge maximumPoolSize = new IntGauge(
                "MaximumPoolSize",
                "The maximum allowed number of threads.",
                exec::getMaximumPoolSize
        );

        private final IntGauge poolSize = new IntGauge(
                "PoolSize",
                "Current number of threads in the pool.",
                exec::getPoolSize
        );

        private final LongGauge taskCount = new LongGauge(
                "TaskCount",
                "Approximate total number of tasks that have been scheduled for execution.",
                exec::getTaskCount
        );

        private final IntGauge queueSize = new IntGauge(
                "QueueSize",
                "Current size of the execution queue.",
                () -> exec.getQueue().size()
        );

        private final LongGauge keepAliveTime = new LongGauge(
                "KeepAliveTime",
                "Thread keep-alive time, which is the amount of time which threads in excess of "
                        + "the core pool size may remain idle before being terminated.",
                () -> exec.getKeepAliveTime(MILLISECONDS)
        );

        @Override
        public Iterable<Metric> metrics() {
            return List.of(
                    activeCount,
                    completedTaskCount,
                    corePoolSize,
                    largestPoolSize,
                    maximumPoolSize,
                    poolSize,
                    taskCount,
                    queueSize,
                    keepAliveTime
            );
        }
    }
}
