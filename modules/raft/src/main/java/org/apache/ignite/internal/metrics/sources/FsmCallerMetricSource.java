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
import java.util.HashMap;
import java.util.List;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.DistributionMetric;
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
     * @param duration Duration of the commit operation.
     */
    public void onFsmCommit(long duration) {
        Holder holder = holder();

        if (holder != null) {
            holder.commitTime.add(duration);
        }
    }

    /**
     * Called on applying tasks.
     *
     * @param duration Duration of the apply operation.
     * @param size Number of tasks applied.
     */
    public void onApplyTasks(long duration, long size) {
        Holder holder = holder();

        if (holder != null) {
            holder.applyTasksTime.add(duration);
            holder.applyTasksSize.add(size);
        }
    }

    /**
     * Called on applying task.
     *
     * @param type Type of the applied task.
     * @param duration Duration of the apply operation.
     * */
    public void onApplyTask(TaskType type, long duration) {
        Holder holder = holder();

        if (holder != null) {
            holder.taskDurations.get(type).add(duration);
        }
    }

    /** Metric holder for FSM caller metrics. */
    static class Holder implements AbstractMetricSource.Holder<Holder> {
        private static final long[] HISTOGRAM_BUCKETS =
                {10, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000};

        private final DistributionMetric applyTasksSize = new DistributionMetric(
                "ApplyTasksSize",
                "Sizes of applied tasks batches",
                new long[] {10, 20, 30, 40, 50}
        );

        private final DistributionMetric applyTasksTime = new DistributionMetric(
                "ApplyTasksTime",
                "Duration of applying tasks in milliseconds",
                HISTOGRAM_BUCKETS
        );

        private final DistributionMetric commitTime = new DistributionMetric(
                "CommitTime",
                "Duration of committing in milliseconds",
                HISTOGRAM_BUCKETS
        );

        private final List<Metric> metrics;

        private final HashMap<TaskType, DistributionMetric> taskDurations = new HashMap<>();

        Holder() {
            metrics = new ArrayList<>();

            metrics.add(applyTasksSize);
            metrics.add(applyTasksTime);
            metrics.add(commitTime);

            for (TaskType type : FSMCallerImpl.TaskType.values()) {
                DistributionMetric metric = new DistributionMetric(
                        type.metricName,
                        "Time to execute " + type.name() + " task in milliseconds",
                        HISTOGRAM_BUCKETS
                );

                taskDurations.put(type, metric);
            }
        }

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
