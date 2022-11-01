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

import org.apache.ignite.internal.metrics.MetricProvider;
import org.apache.ignite.internal.metrics.MetricSet;
import org.jetbrains.annotations.Nullable;

/**
 * Simple test exporter with 1 configuration parameter and "started" flag.
 */
public class TestExporter extends BasicMetricExporter<TestExporterView> {

    private volatile boolean started = false;

    private volatile int port;

    @Override
    public void start(MetricProvider metricsProvider, TestExporterView configuration) {
        super.start(metricsProvider, configuration);

        port = configuration.port();

        started = true;
    }

    @Override
    public void stop() {
        started = false;
    }

    @Override
    public String name() {
        return "test";
    }

    @Override
    public synchronized void reconfigure(@Nullable TestExporterView newValue) {
        super.reconfigure(newValue);

        port = configuration().port();
    }

    @Override
    public void addMetricSet(MetricSet metricSet) {

    }

    @Override
    public void removeMetricSet(String metricSetName) {

    }

    public boolean isStarted() {
        return started;
    }

    public int port() {
        return port;
    }
}
