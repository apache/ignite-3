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

package org.apache.ignite.internal.sql.metrics;

import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.AtomicLongMetric;
import org.apache.ignite.internal.metrics.Metric;

/** Metric source, which provides query execution metrics. */
public class SqlQueryMetricSource extends AbstractMetricSource<SqlQueryMetricSource.Holder> {

    public static final String NAME = "sql.queries";
    public static final String SUCCESSFUL_QUERIES = "Succeeded";
    public static final String FAILED_QUERIES = "Failed";
    public static final String CANCELED_QUERIES = "Canceled";
    public static final String TIMED_OUT_QUERIES = "TimedOut";

    /**
     * Constructor.
     */
    public SqlQueryMetricSource() {
        super(NAME);
    }

    /**
     * Increments the number of successful queries.
     */
    public void success() {
        increment((h) -> h.success.increment());
    }

    /**
     * Increments the number of unsuccessful queries.
     */
    public void failure() {
        increment((h) -> h.failure.increment());
    }

    /**
     * Increments the number of queries that timed out.
     */
    public void timedOut() {
        increment((h) -> h.timedOut.increment());
    }

    /**
     * Increments the number of cancelled queries.
     */
    public void cancel() {
        increment((h) -> h.cancelled.increment());
    }

    private void increment(Consumer<Holder> inc) {
        Holder holder = holder();

        if (holder != null) {
            inc.accept(holder);
        }
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    /** Holder. */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final AtomicLongMetric success = new AtomicLongMetric(SUCCESSFUL_QUERIES, "Successfully completed queries");
        private final AtomicLongMetric failure = new AtomicLongMetric(FAILED_QUERIES, "Failed queries");
        private final AtomicLongMetric cancelled = new AtomicLongMetric(CANCELED_QUERIES, "Cancelled queries");
        private final AtomicLongMetric timedOut = new AtomicLongMetric(TIMED_OUT_QUERIES, "Timed out queries");

        @Override
        public Iterable<Metric> metrics() {
            return List.of(success, failure, cancelled, timedOut);
        }
    }
}
