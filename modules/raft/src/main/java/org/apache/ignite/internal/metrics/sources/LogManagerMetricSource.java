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

import java.util.List;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.AtomicLongMetric;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.Metric;

/** Metrics of log manager. */
public class LogManagerMetricSource extends AbstractMetricSource<LogManagerMetricSource.Holder> {
    public static final String SOURCE_NAME = "raft.logmanager";

    /**
     * Constructor.
     */
    public LogManagerMetricSource(String groupId) {
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
     * Called when log suffix is truncated.
     *
     * @param duration Duration of the truncate operation.
     */
    public void onTruncateLogSuffix(long duration) {
        Holder holder = holder();

        if (holder != null) {
            holder.truncateLogSuffixTime.add(duration);
        }
    }

    /**
     * Called when log prefix is truncated.
     *
     * @param duration Duration of the truncate operation.
     */
    public void onTruncateLogPrefix(long duration) {
        Holder holder = holder();

        if (holder != null) {
            holder.truncateLogPrefixTime.add(duration);
        }
    }

    /**
     * Called when logs are appended.
     *
     * @param entriesCount Number of entries appended.
     * @param writtenSize Written size in bytes.
     * @param duration Duration of the append operation.
     */
    public void onAppendLogs(int entriesCount, int writtenSize, long duration) {
        Holder holder = holder();

        if (holder != null) {
            holder.appendLogsCount.add(entriesCount);
            holder.appendLogsSize.add(writtenSize);
            holder.appendLogsDuration.add(duration);
        }
    }

    /** Metric holder for log manager metrics. */
    static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final DistributionMetric truncateLogSuffixTime = new DistributionMetric(
                "TruncateLogSuffixDuration",
                "Duration of truncating log suffix in milliseconds",
                new long[]{1, 5, 10, 25, 50, 100, 250, 500, 1000}
        );

        private final DistributionMetric truncateLogPrefixTime = new DistributionMetric(
                "TruncateLogPrefixDuration",
                "Duration of truncating log prefix in milliseconds",
                new long[]{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000}
        );

        private final AtomicLongMetric appendLogsCount = new AtomicLongMetric(
                "AppendLogsCount",
                "Total number of entries appended to logs"
        );

        private final AtomicLongMetric appendLogsSize = new AtomicLongMetric(
                "AppendLogsSize",
                "Total size of entries appended to logs"
        );

        private final DistributionMetric appendLogsDuration = new DistributionMetric(
                "AppendLogsDuration",
                "Duration of appending logs operation in milliseconds",
                new long[]{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000}
        );

        private final List<Metric> metrics = List.of(
                truncateLogSuffixTime,
                truncateLogPrefixTime,
                appendLogsCount,
                appendLogsSize,
                appendLogsDuration
        );

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
