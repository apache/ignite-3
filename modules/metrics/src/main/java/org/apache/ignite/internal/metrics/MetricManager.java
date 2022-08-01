/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.metrics;

import org.apache.ignite.internal.manager.IgniteComponent;

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
     * Enable metric source. See {@link MetricRegistry#enable(String)}.
     *
     * @param srcName Source name.
     * @return Metric set, or {@code null} if already enabled.
     */
    public MetricSet enable(final String srcName) {
        return registry.enable(srcName);
    }

    /**
     * Disable metric source. See {@link MetricRegistry#disable(String)}.
     *
     * @param srcName Source name.
     */
    public void disable(final String srcName) {
        registry.disable(srcName);
    }
}
