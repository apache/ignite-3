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

package org.apache.ignite.internal.app.di;

import io.micronaut.context.annotation.Factory;
import io.micronaut.core.annotation.Order;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.app.ThreadPoolsManager;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.di.IgniteStartupPhase;
import org.apache.ignite.internal.di.StartupPhase;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricManagerImpl;

/**
 * Micronaut factory for leaf Ignite components that have few or no dependencies on other components.
 */
@Factory
public class CoreComponentsFactory {
    /** Creates the JVM pause detector. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    @Order(100)
    public LongJvmPauseDetector longJvmPauseDetector(NodeSeedParams seedParams) {
        return new LongJvmPauseDetector(seedParams.nodeName());
    }

    /**
     * Creates the metric manager.
     *
     * @param seedParams Node seed parameters.
     */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(900)
    public MetricManagerImpl metricManager(NodeSeedParams seedParams) {
        return new MetricManagerImpl(seedParams.nodeName(), seedParams.clusterIdSupplier());
    }

    /** Creates the thread pools manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    @Order(400)
    public ThreadPoolsManager threadPoolsManager(NodeSeedParams seedParams, MetricManager metricManager) {
        return new ThreadPoolsManager(seedParams.nodeName(), metricManager);
    }

}
