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

import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.metrics.MetricProvider;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.exporters.configuration.ExporterConfiguration;
import org.apache.ignite.internal.metrics.exporters.configuration.ExporterView;

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
public interface MetricExporter<CfgT extends ExporterView> {
    /**
     * Start metrics exporter. Here all needed listeners, schedulers etc. should be started.
     *
     * @param metricProvider Provider of metric sources.
     * @param configuration Exporter configuration view.
     */
    void start(MetricProvider metricProvider, CfgT configuration);

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
    void reconfigure(CfgT newValue);

    /**
     * {@link MetricManagerImpl} invokes this method,
     * when new metric source was enabled.
     *
     * @param metricSet Named metric set.
     */
    void addMetricSet(MetricSet metricSet);

    /**
     * {@link MetricManagerImpl} invokes this method,
     * when the metric source was disabled.
     *
     * @param metricSetName Name of metric set to remove.
     */
    void removeMetricSet(String metricSetName);
}
