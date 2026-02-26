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

package org.apache.ignite.internal.hlc;

import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import org.apache.ignite.internal.catalog.configuration.SchemaSynchronizationConfiguration;
import org.apache.ignite.internal.catalog.configuration.SchemaSynchronizationExtensionConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.metrics.sources.ClockServiceMetricSource;

/**
 * A factory for creating instances of {@link ClockServiceImpl}.
 *
 * <p>This factory is responsible for constructing the default implementation of {@link ClockService}
 * using the required components such as a hybrid clock, a clock waiter, configuration registry, and metric source.
 */
@Factory
public class ClockServiceImplFactory {
    /**
     * Creates a new instance of {@link ClockServiceImpl}.
     *
     * @param clock The hybrid clock used to generate and manage timestamps.
     * @param clockWaiter A mechanism to handle waiting for specific timestamps.
     * @param clusterConfigRegistry The configuration registry for distributed cluster settings.
     *                              This is used to retrieve the schema synchronization configuration.
     * @param metricSource The metric source for the clock service, which is notified when the maximum clock skew is exceeded.
     * @return A new instance of {@link ClockServiceImpl} configured with the provided arguments.
     */
    public static ClockServiceImpl create(
            HybridClock clock,
            ClockWaiter clockWaiter,
            @Named("distributed") ConfigurationRegistry clusterConfigRegistry,
            @Named("clockService") ClockServiceMetricSource metricSource
    ) {
        SchemaSynchronizationConfiguration schemaSyncConfig = clusterConfigRegistry
                .getConfiguration(SchemaSynchronizationExtensionConfiguration.KEY).schemaSync();

        return new ClockServiceImpl(
                clock,
                clockWaiter,
                () -> schemaSyncConfig.maxClockSkewMillis().value(),
                metricSource::onMaxClockSkewExceeded
        );
    }
}
