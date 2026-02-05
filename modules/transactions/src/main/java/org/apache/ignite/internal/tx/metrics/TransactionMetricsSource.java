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

import static org.apache.ignite.internal.tx.TransactionIds.beginTimestamp;

import java.util.List;
import java.util.UUID;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.Metric;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Transaction metric source, that contains a set of transaction metrics.
 **/
public class TransactionMetricsSource extends AbstractMetricSource<TransactionMetricsSource.Holder> {
    /** Metric name for unresolved (uncommitted) write intents. */
    public static final String METRIC_PENDING_WRITE_INTENTS = "pendingWriteIntents";

    /** Histogram buckets for duration metrics in milliseconds. */
    private static final long[] HISTOGRAM_BUCKETS =
            {1, 2, 4, 8, 16, 25, 50, 75, 100, 250, 500, 750, 1000, 3000, 5000, 10000, 25000, 60000};

    /** Source name. */
    public static final String SOURCE_NAME = "transactions";

    /** Clock service to calculate a timestamp for rolled back transactions. */
    private final ClockService clockService;

    private volatile LongSupplier pendingWriteIntentsSupplier;

    /**
     * Creates a new instance of {@link TransactionMetricsSource}.
     */
    public TransactionMetricsSource(ClockService clockService) {
        super(SOURCE_NAME, "Transaction metrics.");

        this.clockService = clockService;
    }

    /**
     * Sets a supplier of the total number of unresolved (uncommitted) write intents local to this node.
     *
     * @param supplier Supplier, or {@code null} to reset to default (always returns {@code 0}).
     */
    public void setPendingWriteIntentsSupplier(@Nullable LongSupplier supplier) {
        pendingWriteIntentsSupplier = supplier == null ? () -> 0L : supplier;
    }

    /**
     * Updates read-write related metrics.
     *
     * @param transactionId Transaction identifier.
     * @param commit {@code true} if a transaction was committed, and {@code false} otherwise.
     */
    public void onReadWriteTransactionFinished(UUID transactionId, boolean commit) {
        Holder holder = holder();

        if (holder != null) {
            holder.rwDuration.add(calculateTransactionDuration(transactionId));

            holder.activeTransactions.decrement();

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
    public void onReadOnlyTransactionFinished(UUID transactionId, boolean commit) {
        Holder holder = holder();

        if (holder != null) {
            holder.roDuration.add(calculateTransactionDuration(transactionId));

            holder.activeTransactions.decrement();

            if (commit) {
                holder.totalCommits.increment();
                holder.roCommits.increment();
            } else {
                holder.totalRollbacks.increment();
                holder.roRollbacks.increment();
            }
        }
    }

    /**
     * Tracks a number of active transactions.
     */
    public void onTransactionStarted() {
        Holder holder = holder();

        if (holder != null) {
            holder.activeTransactions.increment();
        }
    }

    /**
     * Returns a number of active transactions.
     * If this metric source is not enabled, then always returns {@code 0}.
     *
     * @return Number of active transactions.
     */
    public long activeTransactions() {
        Holder holder = holder();

        if (holder != null) {
            return holder.activeTransactions.value();
        }

        return 0L;
    }

    /**
     * Returns a number of finished transactions.
     * If this metric source is not enabled, then always returns {@code 0}.
     *
     * @return Number of finished transactions.
     */
    public long finishedTransactions() {
        Holder holder = holder();

        if (holder != null) {
            return holder.totalCommits.value() + holder.totalRollbacks.value();
        }

        return 0L;
    }

    @Override
    protected Holder createHolder() {
        return new Holder(() -> pendingWriteIntentsSupplier.getAsLong());
    }

    private long calculateTransactionDuration(UUID transactionId) {
        return clockService.currentLong() - beginTimestamp(transactionId).getPhysical();
    }

    /** Holder. */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final LongAdderMetric totalCommits = new LongAdderMetric(
                "TotalCommits",
                "Total number of commits.");

        private final LongAdderMetric totalRollbacks = new LongAdderMetric(
                "TotalRollbacks",
                "Total number of rollbacks.");

        private final LongAdderMetric rwCommits = new LongAdderMetric(
                "RwCommits",
                "Total number of read-write transaction commits.");

        private final LongAdderMetric roCommits = new LongAdderMetric(
                "RoCommits",
                "Total number of read-only transaction commits.");

        private final LongAdderMetric rwRollbacks = new LongAdderMetric(
                "RwRollbacks",
                "Total number of rolled-back read-write transactions.");

        private final LongAdderMetric roRollbacks = new LongAdderMetric(
                "RoRollbacks",
                "Total number of rolled-back read-only transactions.");

        private final DistributionMetric rwDuration = new DistributionMetric(
                "RwDuration",
                ".",
                HISTOGRAM_BUCKETS);

        private final DistributionMetric roDuration = new DistributionMetric(
                "RoDuration",
                ".",
                HISTOGRAM_BUCKETS);

        @TestOnly
        private final LongAdderMetric activeTransactions = new LongAdderMetric(
                "Active",
                "Number of running transactions.");

        private final LongGauge pendingWriteIntents;

        private final List<Metric> metrics;

        private Holder(LongSupplier pendingWriteIntentsSupplier) {
            pendingWriteIntents = new LongGauge(
                    METRIC_PENDING_WRITE_INTENTS,
                    "Total number of unresolved (uncommitted) write intents across all local partitions on this node.",
                    pendingWriteIntentsSupplier
            );

            metrics = List.of(
                    totalCommits,
                    rwCommits,
                    roCommits,
                    totalRollbacks,
                    rwRollbacks,
                    roRollbacks,
                    rwDuration,
                    roDuration,
                    pendingWriteIntents
            );
        }

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
