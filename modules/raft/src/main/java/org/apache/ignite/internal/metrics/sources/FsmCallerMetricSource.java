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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.AtomicLongMetric;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.raft.jraft.core.FSMCallerImpl;
import org.apache.ignite.raft.jraft.core.FSMCallerImpl.TaskType;

/**
 * Metrics of FSM caller.
 */
public class FsmCallerMetricSource extends AbstractMetricSource<FsmCallerMetricSource.Holder> {
    public static final String SOURCE_NAME = "raft.fsmcaller";

    /**
     * Constructor.
     */
    public FsmCallerMetricSource(String groupId) {
        super(sourceName(groupId));
    }

    private static String sourceName(String groupId) {
        return SOURCE_NAME + '.' + groupId;
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    /**
     * Called on FSM commit.
     *
     * @param time Duration of the commit operation.
     */
    public void onFsmCommit(long time) {
        Holder holder = holder();

        if (holder != null) {
            holder.lastCommitTime.value(time);
        }
    }

    /**
     * Called on applying tasks.
     *
     * @param time Duration of the apply operation.
     * @param size Number of tasks applied.
     */
    public void onApplyTasks(long time, long size) {
        Holder holder = holder();

        if (holder != null) {
            holder.lastApplyTasksTime.value(time);
            holder.applyTasksSize.add(size);
        }
    }

    /** Metric holder for FSM caller metrics. */
    static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final DistributionMetric applyTasksSize = new DistributionMetric(
                "ApplyTasksSize",
                "Sizes of applied tasks batches",
                new long[] {10, 20, 30, 40, 50}
        );

        private final AtomicLongMetric lastApplyTasksTime = new AtomicLongMetric("ApplyTasksTime", "Time to apply tasks");

        private final AtomicLongMetric lastCommitTime = new AtomicLongMetric("CommitTime", "Time to apply tasks");

        private final List<Metric> metrics;

        Holder() {
            metrics = new ArrayList<>();

            metrics.add(applyTasksSize);
            metrics.add(lastApplyTasksTime);
            metrics.add(lastCommitTime);

            for (TaskType type : FSMCallerImpl.TaskType.values()) {
                metrics.add(new LongGauge(type.metricName(), "Time to execute " + type.name() + " task", type::getApplyDuration));
            }
        }

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
