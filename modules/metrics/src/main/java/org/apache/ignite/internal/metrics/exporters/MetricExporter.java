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

package org.apache.ignite.internal.metrics.exporters;

import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricProvider;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.exporters.configuration.ExporterConfiguration;
import org.apache.ignite.internal.metrics.exporters.configuration.ExporterView;
import org.jetbrains.annotations.Nullable;

/**
 * Interface for metric exporters to external recipients.
 * Exporters can be one of the two type: push and pull exporters.
 *
 * <p>Push exporters push metrics to the external endpoint periodically.
 * Push exporters should implement {@link PushMetricExporter} according to its documentation.
 *
 * <p>Pull exporters is the endpoint by itself (HTTP, JMX and etc.), which response with the metric data for request.
 * Pull exporters should extend {@link BasicMetricExporter}.
 */
public interface MetricExporter {
    /**
     * Start metrics exporter. Here all needed listeners, schedulers etc. should be started.
     *
     * @param metricProvider Provider of metric sources.
     * @param configuration Exporter configuration view.
     * @param clusterIdSupplier Cluster ID supplier.
     * @param nodeName Node name.
     */
    void start(MetricProvider metricProvider, @Nullable ExporterView configuration, Supplier<UUID> clusterIdSupplier, String nodeName);

    /**
     * Stop and cleanup work for current exporter must be implemented here.
     */
    void stop();

    /**
     * Returns the name of exporter. Name must be unique and be the same for exporter and its {@link ExporterConfiguration}.
     *
     * @return Name of the exporter.
     */
    String name();

    /**
     * Invokes, when exporter's configuration was updated.
     *
     * <p>Be careful: this method will be invoked from the separate configuration events' thread pool.
     * Appropriate thread-safe logic falls on the shoulders of implementations.
     *
     * @param newValue New configuration view.
     */
    void reconfigure(ExporterView newValue);

    /**
     * {@link MetricManager} invokes this method, when new metric source was enabled.
     *
     * @param metricSet Named metric set.
     */
    default void addMetricSet(MetricSet metricSet) {
        // No-op.
    }

    /**
     * {@link MetricManager} invokes this method, when the metric source was disabled.
     *
     * @param metricSet Named metric set to remove.
     */
    default void removeMetricSet(MetricSet metricSet) {
        // No-op.
    }
}
