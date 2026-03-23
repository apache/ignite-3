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
import org.apache.ignite.internal.metrics.MetricProvider;
import org.apache.ignite.internal.metrics.exporters.configuration.ExporterView;
import org.apache.ignite.internal.metrics.exporters.configuration.TestExporterView;

/**
 * Simple test exporter with 1 configuration parameter and "started" flag.
 */
public class TestExporter extends BasicMetricExporter {

    private volatile boolean started = false;

    private volatile int port;

    @Override
    public void start(
            MetricProvider metricsProvider,
            ExporterView configuration,
            Supplier<UUID> clusterIdSupplier,
            String nodeName
    ) {
        super.start(metricsProvider, configuration, clusterIdSupplier, nodeName);

        port = ((TestExporterView) configuration).port();

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
    public void reconfigure(ExporterView newValue) {
        port = ((TestExporterView) newValue).port();
    }

    public boolean isStarted() {
        return started;
    }

    public int port() {
        return port;
    }
}
