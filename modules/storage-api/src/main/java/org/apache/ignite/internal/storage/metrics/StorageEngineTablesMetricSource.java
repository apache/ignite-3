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

package org.apache.ignite.internal.storage.metrics;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.table.QualifiedName;
import org.jetbrains.annotations.Nullable;

/**
 * Metric source for storage engine metrics related to a specific table.
 */
public class StorageEngineTablesMetricSource extends AbstractMetricSource<StorageEngineTablesMetricSource.Holder> {
    public static final String METRIC_GROUP = "storage";

    private final Set<Metric> extraMetrics = new HashSet<>();

    /**
     * Creates a new metric source for the given storage engine and table, with a name {@code storage.<engine>.tables.<tableName>} and
     * group {@value METRIC_GROUP}.
     *
     * @param engine Storage engine name.
     * @param tableName Table qualified name.
     */
    public StorageEngineTablesMetricSource(String engine, QualifiedName tableName) {
        super(sourceName(engine, tableName), "\"" + engine + "\" storage engine metrics for the specific table.", METRIC_GROUP);
    }

    /**
     * Creates a new metric source for the given storage engine and table, with a name {@code storage.<engine>.tables.<tableName>} and
     * group {@value METRIC_GROUP}.
     *
     * @param engine Storage engine name.
     * @param tableName Table qualified name.
     * @param copyFrom Optional metric source to copy metrics from.
     */
    public StorageEngineTablesMetricSource(String engine, QualifiedName tableName, @Nullable StorageEngineTablesMetricSource copyFrom) {
        super(sourceName(engine, tableName), "\"" + engine + "\" storage engine metrics for the specific table.", METRIC_GROUP);

        if (copyFrom != null) {
            this.extraMetrics.addAll(copyFrom.extraMetrics);
        }
    }

    /**
     * Returns a metric source name for the given storage engine and table.
     */
    public static String sourceName(String engine, QualifiedName tableName) {
        return METRIC_GROUP + '.' + engine + ".tables." + tableName.toCanonicalForm();
    }

    /**
     * Adds a custom metric to the source.
     *
     * <p>Note: This method cannot be called when the source is enabled.
     *
     * @param metric Metric to add.
     */
    public void addMetric(Metric metric) {
        assert holder() == null : "Cannot add metrics when source is enabled";

        extraMetrics.add(metric);
    }

    @Override
    protected Holder createHolder() {
        return new Holder(extraMetrics);
    }

    /**
     * Holder for the storage engine table metrics.
     */
    public static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final List<Metric> metrics;

        protected Holder(Set<Metric> extraMetrics) {
            metrics = List.copyOf(extraMetrics);
        }

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
