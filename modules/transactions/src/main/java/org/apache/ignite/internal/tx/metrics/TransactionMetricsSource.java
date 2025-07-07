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
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.tx.metrics.TransactionMetricsSource.Holder;
import org.jetbrains.annotations.Nullable;

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
    static final String SOURCE_NAME = "transactions";

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
     * @param txId Transaction identifier.
     * @param commitTimestamp Commit timestamp. {@code null} when the transaction is rolled back.
     * @param commit {@code true} if a transaction was committed, and {@code false} otherwise.
     * @param implicit {@code true} if a transaction is implicit, and {@code false} otherwise.
     * @param user {@code true} if a transaction was rolled back by the user.
     */
    public void readWriteTxFinish(UUID txId, @Nullable HybridTimestamp commitTimestamp, boolean commit, boolean implicit, boolean user) {
        Holder holder = holder();

        if (holder != null) {
            if (commit) {
                holder.totalCommits.increment();

                if (implicit) {
                    holder.rwImplicitCommits.increment();
                } else {
                    holder.rwExplicitCommits.increment();
                }

                assert commitTimestamp != null :
                        "Commit timestamp must not be null [txId=" + txId + ", commit=" + commit + ", implicit=" + implicit + ']';

                // beginTimestamp(txId).getPhysical() == transactionId.getMostSignificantBits()
                long duration = commitTimestamp.getPhysical() - beginTimestamp(txId).getPhysical();

                holder.rwDuration.add(duration);
            } else {
                holder.totalRollbacks.increment();

                if (user) {
                    holder.userRollbacks.increment();
                } else {
                    holder.abnormalRollbacks.increment();
                }

                if (implicit) {
                    holder.rwImplicitRollbacks.increment();
                } else {
                    holder.rwExplicitRollbacks.increment();
                }

                long duration = clockService.current().getPhysical() - beginTimestamp(txId).getPhysical();

                holder.rwDuration.add(duration);
            }
        }
    }

    /**
     * Updates read-only related metrics.
     *
     * @param txId Transaction identifier.
     * @param commitTimestamp Commit timestamp. {@code null} when the transaction is rolled back.
     * @param commit {@code true} if a transaction was committed, and {@code false} otherwise.
     * @param implicit {@code true} if a transaction is implicit, and {@code false} otherwise.
     * @param user {@code true} if a transaction was rolled back by the user.
     */
    public void readOnlyTxFinish(UUID txId, @Nullable HybridTimestamp commitTimestamp, boolean commit, boolean implicit, boolean user) {
        Holder holder = holder();

        if (holder != null) {
            if (commit) {
                holder.totalCommits.increment();

                if (implicit) {
                    holder.roImplicitCommits.increment();
                } else {
                    holder.roExplicitCommits.increment();
                }

                assert commitTimestamp != null :
                        "Commit timestamp must not be null [txId=" + txId + ", commit=" + commit + ", implicit=" + implicit + ']';

                // beginTimestamp(txId).getPhysical() == transactionId.getMostSignificantBits()
                long duration = commitTimestamp.getPhysical() - beginTimestamp(txId).getPhysical();

                holder.roDuration.add(duration);
            } else {
                holder.totalRollbacks.increment();

                if (user) {
                    holder.userRollbacks.increment();
                } else {
                    holder.abnormalRollbacks.increment();
                }

                if (implicit) {
                    holder.roImplicitRollbacks.increment();
                } else {
                    holder.roExplicitRollbacks.increment();
                }

                long duration = clockService.current().getPhysical() - beginTimestamp(txId).getPhysical();

                holder.roDuration.add(duration);
            }
        }
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    /** Holder. */
    protected class Holder implements AbstractMetricSource.Holder<Holder> {
        private final LongAdderMetric totalCommits = new LongAdderMetric(
                "TotalCommits",
                "Total number of commits.");

        private final LongAdderMetric totalRollbacks = new LongAdderMetric(
                "TotalRollbacks",
                "Total number of rollbacks.");

        private final LongAdderMetric userRollbacks = new LongAdderMetric(
                "UserRollbacks",
                "Total number of rolled-back transactions by the user.");

        private final LongAdderMetric abnormalRollbacks = new LongAdderMetric(
                "AbnormalRollbacks",
                "Total number of rolled-back transactions due to error, timeout, etc.");

        private final LongAdderMetric rwExplicitCommits = new LongAdderMetric(
                "RwExplicitCommits",
                "Total number of commits for explicit read-write transactions.");

        private final LongAdderMetric rwImplicitCommits = new LongAdderMetric(
                "RwImplicitCommits",
                "Total number of commits for implicit read-write transactions.");

        private final LongAdderMetric roExplicitCommits = new LongAdderMetric(
                "RoExplicitCommits",
                "Total number of commits for explicit read-only transactions.");

        private final LongAdderMetric roImplicitCommits = new LongAdderMetric(
                "RoImplicitCommits",
                "Total number of commits for implicit read-only transactions.");

        private final LongAdderMetric rwExplicitRollbacks = new LongAdderMetric(
                "RwExplicitRollbacks",
                "Total number of rolled-back explicit read-write transactions.");

        private final LongAdderMetric rwImplicitRollbacks = new LongAdderMetric(
                "RwImplicitRollbacks",
                "Total number of rolled-back implicit read-write transactions.");

        private final LongAdderMetric roExplicitRollbacks = new LongAdderMetric(
                "RoExplicitRollbacks",
                "Total number of rolled back explicit read-only transactions.");

        private final LongAdderMetric roImplicitRollbacks = new LongAdderMetric(
                "RoImplicitRollbacks",
                "Total number of rolled back implicit read-only transactions.");

        private final DistributionMetric rwDuration = new DistributionMetric(
                "RwDuration",
                ".",
                HISTOGRAM_BUCKETS);

        private final DistributionMetric roDuration = new DistributionMetric(
                "RoDuration",
                ".",
                HISTOGRAM_BUCKETS);

        private final List<Metric> metrics = List.of(
                totalCommits,
                totalRollbacks,
                userRollbacks,
                abnormalRollbacks,
                rwExplicitCommits,
                rwImplicitCommits,
                roExplicitCommits,
                roImplicitCommits,
                rwExplicitRollbacks,
                rwImplicitRollbacks,
                roExplicitRollbacks,
                roImplicitRollbacks,
                rwDuration,
                roDuration);

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
