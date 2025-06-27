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
import java.util.Objects;
import java.util.function.IntSupplier;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.Metric;

/** Source of SQL client metrics. */
public class SqlClientMetricSource extends AbstractMetricSource<SqlClientMetricSource.Holder> {
    public static final String NAME = "sql.client";
    public static final String METRIC_OPEN_CURSORS = "OpenCursors";
    private final IntSupplier numberOfOpenCursorsSupplier;

    /**
     * Constructor.
     *
     * @param numberOfOpenCursorsSupplier Integer supplier provides current number of open cursors.
     */
    public SqlClientMetricSource(IntSupplier numberOfOpenCursorsSupplier) {
        super(NAME);

        this.numberOfOpenCursorsSupplier = Objects.requireNonNull(numberOfOpenCursorsSupplier);
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    /** Holder. */
    protected class Holder implements AbstractMetricSource.Holder<Holder> {
        private final IntGauge numberOfOpenCursors = new IntGauge(
                METRIC_OPEN_CURSORS,
                "Number of currently open cursors",
                numberOfOpenCursorsSupplier
        );

        @Override
        public Iterable<Metric> metrics() {
            return List.of(numberOfOpenCursors);
        }
    }
}
