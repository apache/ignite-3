/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.ignite.internal.manager.IgniteComponent;
import org.jetbrains.annotations.NotNull;

/**
 * Metric manager.
 */
public class MetricManager implements IgniteComponent {
    /**
     * Metric registry.
     */
    private final MetricRegistry registry;

    /**
     * Constructor.
     */
    public MetricManager() {
        this.registry = new MetricRegistry();
    }

    /** {@inheritDoc} */
    @Override public void start() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {
        // No-op.
    }

    /**
     * Register metric source. See {@link MetricRegistry#registerSource(MetricSource)}.
     *
     * @param src Metric source.
     */
    public void registerSource(MetricSource src) {
        registry.registerSource(src);
    }

    /**
     * Unregister metric source. See {@link MetricRegistry#unregisterSource(MetricSource)}.
     *
     * @param src Metric source.
     */
    public void unregisterSource(MetricSource src) {
        registry.unregisterSource(src);
    }

    /**
     * Unregister metric source by name. See {@link MetricRegistry#unregisterSource(String)}.
     *
     * @param srcName Metric source name.
     */
    public void unregisterSource(String srcName) {
        registry.unregisterSource(srcName);
    }

    /**
     * Enable metric source. See {@link MetricRegistry#enable(MetricSource)}.
     *
     * @param src Metric source.
     * @return Metric set, or {@code null} if already enabled.
     */
    public MetricSet enable(MetricSource src) {
        return registry.enable(src);
    }

    /**
     * Enable metric source by name. See {@link MetricRegistry#enable(String)}.
     *
     * @param srcName Source name.
     * @return Metric set, or {@code null} if already enabled.
     */
    public MetricSet enable(final String srcName) {
        return registry.enable(srcName);
    }

    /**
     * Disable metric source. See {@link MetricRegistry#disable(MetricSource)}.
     *
     * @param src Metric source.
     */
    public void disable(MetricSource src) {
        registry.disable(src);
    }

    /**
     * Disable metric source by name. See {@link MetricRegistry#disable(String)}.
     *
     * @param srcName Source name.
     */
    public void disable(final String srcName) {
        registry.disable(srcName);
    }

    /**
     * Metric set schema.
     *
     * @return Metric set schema.
     */
    @NotNull
    public MetricSetSchema metricSetSchema() {
        return registry.metricSetSchema();
    }
}
