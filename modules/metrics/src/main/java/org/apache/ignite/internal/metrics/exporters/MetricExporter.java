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

package org.apache.ignite.internal.metrics.exporters;

import org.apache.ignite.internal.metrics.MetricProvider;

/**
 * Interface for metric exporters to external recipients.
 * Exporters can be one of the two type: push and pull exporters.
 *
 * <p> Push exporters push metrics to the external endpoint periodically. Push exporters should implement {@link PushMetricExporter} according to its documentation.
 * <p> Pull exporters is the endpoint by itself (HTTP, JMX and etc.), which response with the metric data for request. Pull exporters should extend {@link BasicMetricExporter}.
 */
public interface MetricExporter {

    /**
     * Initialize metric exporter with the provider of available metrics.
     *
     * @param metricProvider Metrics provider
     */
    void init(MetricProvider metricProvider);

    /**
     * Start metrics exporter. Here all needed listeners, schedulers etc. should be started.
     */
    void start();

    /**
     * Stop and cleanup work for current exporter must be implemented here.
     */
    void stop();

    /**
     * Returns the name of exporter. Name must be unique.
     *
     * @return Name of the exporter.
     */
    String name();
}
