/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.metrics;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * Metric source for {@link ThreadPoolMetricTest}.
 */
public class ThreadPoolMetricSource extends AbstractMetricSource<ThreadPoolMetricSource.Holder> {
    private final ThreadPoolExecutor exec;

    public ThreadPoolMetricSource(String name, ThreadPoolExecutor exec) {
        super(name);

        this.exec = exec;
    }


    /** {@inheritDoc} */
    @Override protected Holder createHolder() {
        return new Holder();
    }

    /** {@inheritDoc} */
    @Override protected void init(MetricSetBuilder bldr, Holder holder) {
        bldr.intGauge(
                "ActiveCount",
                "Approximate number of threads that are actively executing tasks.",
                exec::getActiveCount
        );

        bldr.longGauge(
                "CompletedTaskCount",
                "Approximate total number of tasks that have completed execution.",
                exec::getCompletedTaskCount
        );

        bldr.intGauge("CorePoolSize", "The core number of threads.", exec::getCorePoolSize);

        bldr.intGauge(
                "LargestPoolSize",
                "Largest number of threads that have ever simultaneously been in the pool.",
                exec::getLargestPoolSize
        );

        bldr.intGauge(
                "MaximumPoolSize",
                "The maximum allowed number of threads.",
                exec::getMaximumPoolSize
        );

        bldr.intGauge("PoolSize", "Current number of threads in the pool.", exec::getPoolSize);

        bldr.longGauge(
                "TaskCount",
                "Approximate total number of tasks that have been scheduled for execution.",
                exec::getTaskCount
        );

        bldr.intGauge("QueueSize", "Current size of the execution queue.", () -> exec.getQueue().size());

        bldr.longGauge(
                "KeepAliveTime",
                "Thread keep-alive time, which is the amount of time which threads in excess of " +
                "the core pool size may remain idle before being terminated.",
                () -> exec.getKeepAliveTime(MILLISECONDS)
        );
    }

    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        // No-op.
    }
}
