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

package org.apache.ignite.internal.metrics;

import java.util.Collection;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metrics.exporters.MetricExporter;
import org.jetbrains.annotations.Nullable;

/**
 * The component services of the metrics. It has functions to switch on / off and register them.
 */
public interface MetricManager extends IgniteComponent {
    /**
     * Register metric source. See {@link MetricRegistry#registerSource(MetricSource)}.
     *
     * @param src Metric source.
     */
    void registerSource(MetricSource src);

    /**
     * Disables and unregisters metric source.
     *
     * @param src Metric source.
     * @see #disable(MetricSource) 
     * @see MetricRegistry#unregisterSource(MetricSource)
     */
    void unregisterSource(MetricSource src);

    /**
     * Disables and unregisters metric source by name.
     *
     * @param srcName Metric source name.
     * @see #disable(String)
     * @see MetricRegistry#unregisterSource(String) 
     */
    void unregisterSource(String srcName);

    /**
     * Enable metric source. See {@link MetricRegistry#enable(MetricSource)}.
     *
     * @param src Metric source.
     * @return Metric set, or {@code null} if already enabled.
     */
    @Nullable MetricSet enable(MetricSource src);

    /**
     * Enable metric source by name. See {@link MetricRegistry#enable(String)}.
     *
     * @param srcName Source name.
     * @return Metric set, or {@code null} if already enabled.
     */
    @Nullable MetricSet enable(String srcName);

    /**
     * Disable metric source. See {@link MetricRegistry#disable(MetricSource)}.
     *
     * @param src Metric source.
     */
    void disable(MetricSource src);

    /**
     * Disable metric source by name. See {@link MetricRegistry#disable(String)}.
     *
     * @param srcName Metric source name.
     */
    void disable(String srcName);

    /**
     * Metrics snapshot. This is a snapshot of metric sets with corresponding version, the values of the metrics in the
     * metric sets that are included into the snapshot, are changed dynamically.
     *
     * @return Metrics snapshot.
     */
    MetricSnapshot metricSnapshot();

    /**
     * Gets a collection of metric sources.
     *
     * @return collection of metric sources
     */
    Collection<MetricSource> metricSources();

    /**
     * Returns a collection of currently enabled metric exporters.
     */
    Collection<MetricExporter> enabledExporters();
}
