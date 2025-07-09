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

package org.apache.ignite.internal.tx.metrics;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.tx.TransactionIds.beginTimestamp;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.tx.metrics.TransactionMetricsSource.Holder;

/**
 * TODO.
 **/
public class TransactionMetricsSource extends AbstractMetricSource<Holder> {
    IgniteLogger log = Loggers.forClass(TransactionMetricsSource.class);

    /** Histogram buckets for duration metrics in milliseconds. */
    private static final long[] HISTOGRAM_BUCKETS = new long[] {
            MILLISECONDS.convert(1, MILLISECONDS),
            MILLISECONDS.convert(10, MILLISECONDS),
            MILLISECONDS.convert(100, MILLISECONDS),
            MILLISECONDS.convert(250, MILLISECONDS),
            MILLISECONDS.convert(500, MILLISECONDS),
            MILLISECONDS.convert(1, SECONDS),
            MILLISECONDS.convert(5, SECONDS)
    };

    /** Source name. */
    public static final String SOURCE_NAME = "transactions";

    /** Clock service to calculate a timestamp for rolled back transactions. */
    private final ClockService clockService;

    /**
     * Creates a new instance of {@link TransactionMetricsSource}.
     */
    public TransactionMetricsSource(ClockService clockService) {
        // TODO IGNITE-25526
        //  super(SOURCE_NAME, "Transaction metrics.", null);
        super(SOURCE_NAME);
        this.clockService = clockService;
    }

    /**
     * Updates read-write related metrics.
     *
     * @param transactionId Transaction identifier.
     * @param commit {@code true} if a transaction was committed, and {@code false} otherwise.
     */
    public void readWriteTxFinish(UUID transactionId, boolean commit) {
        Holder holder = holder();

        log.warn(">>>>> readWriteTxFinish [enabled=" + (holder != null));
        if (holder != null) {
            holder.rwDuration.add(calculateTransactionDuration(transactionId));

            if (commit) {
                holder.totalCommits.increment();
                holder.rwCommits.increment();
            } else {
                holder.totalRollbacks.increment();
                holder.rwRollbacks.increment();
            }
        }
    }

    /**
     * Updates read-only related metrics.
     *
     * @param transactionId Transaction identifier.
     * @param commit {@code true} if a transaction was committed, and {@code false} otherwise.
     */
    public void readOnlyTxFinish(UUID transactionId, boolean commit) {
        Holder holder = holder();

        log.warn(">>>>> readOnlyTxFinish [enabled=" + (holder != null));
        if (holder != null) {
            holder.roDuration.add(calculateTransactionDuration(transactionId));

            if (commit) {
                holder.totalCommits.increment();
                holder.roCommits.increment();
            } else {
                holder.totalRollbacks.increment();
                holder.roRollbacks.increment();
            }
        }
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    private long calculateTransactionDuration(UUID transactionId) {
        // beginTimestamp(transactionId).getPhysical() == transactionId.getMostSignificantBits()
        return clockService.currentLong() - beginTimestamp(transactionId).getPhysical();
    }

    /** Holder. */
    public class Holder implements AbstractMetricSource.Holder<Holder> {
        public final LongAdderMetric totalCommits = new LongAdderMetric(
                "TotalCommits",
                "Total number of commits.");

        public final LongAdderMetric totalRollbacks = new LongAdderMetric(
                "TotalRollbacks",
                "Total number of rollbacks.");

        public final LongAdderMetric rwCommits = new LongAdderMetric(
                "RwCommits",
                "Total number of read-write transaction commits.");

        public final LongAdderMetric roCommits = new LongAdderMetric(
                "RoCommits",
                "Total number of read-only transaction commits.");

        public final LongAdderMetric rwRollbacks = new LongAdderMetric(
                "RwRollbacks",
                "Total number of rolled-back read-write transactions.");

        public final LongAdderMetric roRollbacks = new LongAdderMetric(
                "RoRollbacks",
                "Total number of rolled-back read-only transactions.");

        public final DistributionMetric rwDuration = new DistributionMetric(
                "RwDuration",
                ".",
                HISTOGRAM_BUCKETS);

        public final DistributionMetric roDuration = new DistributionMetric(
                "RoDuration",
                ".",
                HISTOGRAM_BUCKETS);

        private final List<Metric> metrics = List.of(
                totalCommits,
                rwCommits,
                roCommits,
                totalRollbacks,
                rwRollbacks,
                roRollbacks,
                rwDuration,
                roDuration);

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
