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
import static org.apache.ignite.internal.metrics.sources.ThreadPoolMetricSource.THREAD_POOLS_GROUP_NAME;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.thread.StripedThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

/** Metric source for monitoring of {@link StripedThreadPoolExecutor}. */
public class StripedThreadPoolMetricSource extends AbstractMetricSource<StripedThreadPoolMetricSource.Holder> {
    /** Striped thread pool to be monitored. */
    private final StripedThreadPoolExecutor exec;

    /**
     * Creates a new thread pool metric source with the given {@code name} to monitor the provided striped executor {@code exec},
     * using the default {@link ThreadPoolMetricSource#THREAD_POOLS_GROUP_NAME} group.
     *
     * @param name Metric source name.
     * @param description Metric source description.
     * @param exec Striped thread pool executor to monitor.
     * @see StripedThreadPoolExecutor
     */
    public StripedThreadPoolMetricSource(String name, @Nullable String description, StripedThreadPoolExecutor exec) {
        this(name, null, THREAD_POOLS_GROUP_NAME, exec);
    }

    /**
     * Creates a new thread pool metric source with the given {@code name} to monitor the provided executor {@code exec}.
     *
     * @param name Metric source name.
     * @param description Metric source description.
     * @param group Metric source group.
     * @param exec Striped thread pool executor to monitor.
     */
    public StripedThreadPoolMetricSource(
            String name,
            @Nullable String description,
            @Nullable String group,
            StripedThreadPoolExecutor exec
    ) {
        super(name, description, group);

        this.exec = exec;
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    /** Holder class. */
    protected class Holder implements AbstractMetricSource.Holder<Holder> {
        final List<Metric> executorMetrics;

        Holder() {
            executorMetrics = new ArrayList<>(exec.concurrencyLevel() * 9);

            executorMetrics.add(new IntGauge(
                    "ConcurrencyLevel",
                    "Concurrency level of the striped thread pool executor.",
                    exec::concurrencyLevel
            ));

            for (int i = 0; i < exec.concurrencyLevel(); i++) {
                assert exec.stripeExecutor(i) instanceof ThreadPoolExecutor :
                        "Stripe executor should be an instance of ThreadPoolExecutor ["
                                + "class=" + exec.stripeExecutor(i).getClass() + ", idx=" + i + ']';

                ThreadPoolExecutor stripe = (ThreadPoolExecutor) exec.stripeExecutor(i);

                String stripeName = "stripe." + i + '.';

                executorMetrics.add(new IntGauge(
                        stripeName + "ActiveCount",
                        "Approximate number of threads that are actively executing tasks.",
                        stripe::getActiveCount
                ));

                executorMetrics.add(new LongGauge(
                        stripeName + "CompletedTaskCount",
                        "Approximate total number of tasks that have completed execution.",
                        stripe::getCompletedTaskCount
                ));

                executorMetrics.add(new IntGauge(
                        stripeName + "CorePoolSize",
                        "The core number of threads.",
                        stripe::getCorePoolSize
                ));

                executorMetrics.add(new IntGauge(
                        stripeName + "LargestPoolSize",
                        "Largest number of threads that have ever simultaneously been in the pool.",
                        stripe::getLargestPoolSize
                ));

                executorMetrics.add(new IntGauge(
                        stripeName + "MaximumPoolSize",
                        "The maximum allowed number of threads.",
                        stripe::getMaximumPoolSize
                ));

                executorMetrics.add(new IntGauge(
                        stripeName + "PoolSize",
                        "Current number of threads in the pool.",
                        stripe::getPoolSize
                ));

                executorMetrics.add(new LongGauge(
                        stripeName + "TaskCount",
                        "Approximate total number of tasks that have been scheduled for execution.",
                        stripe::getTaskCount
                ));

                executorMetrics.add(new IntGauge(
                        stripeName + "QueueSize",
                        "Current size of the execution queue.",
                        () -> stripe.getQueue().size()
                ));

                executorMetrics.add(new LongGauge(
                        stripeName + "KeepAliveTime",
                        "Thread keep-alive time, which is the amount of time which threads in excess of "
                                + "the core pool size may remain idle before being terminated.",
                        () -> stripe.getKeepAliveTime(MILLISECONDS)
                ));
            }
        }

        @Override
        public Iterable<Metric> metrics() {
            return executorMetrics;
        }
    }
}
