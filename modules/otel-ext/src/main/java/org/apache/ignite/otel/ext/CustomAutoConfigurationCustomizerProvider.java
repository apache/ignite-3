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

package org.apache.ignite.otel.ext;

import com.google.auto.service.AutoService;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizer;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider;
import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.otel.ext.sampler.DynamicRatioSampler;

/**
 * This is one of the main entry points for Instrumentation Agent's customizations. It allows configuring the
 * {@link AutoConfigurationCustomizer}. See the {@link #customize(AutoConfigurationCustomizer)} method below.
 *
 * <p>Also see https://github.com/open-telemetry/opentelemetry-java/issues/2022
 *
 * @see AutoConfigurationCustomizerProvider
 */
@AutoService(AutoConfigurationCustomizerProvider.class)
public class CustomAutoConfigurationCustomizerProvider implements AutoConfigurationCustomizerProvider {
    /** {@inheritDoc} */
    @Override
    public void customize(AutoConfigurationCustomizer autoConfiguration) {
        autoConfiguration
                .addPropertiesCustomizer(CustomAutoConfigurationCustomizerProvider::defaultConfiguration)
                .addSamplerCustomizer(CustomAutoConfigurationCustomizerProvider::customizeSampler);
    }

    private static Map<String, String> defaultConfiguration(ConfigProperties configProperties) {
        String exporter = configProperties.getString("otel.traces.exporter", "zipkin");

        return Map.of(
                "otel.metrics.exporter", "none",
                "otel.traces.exporter", exporter
        );
    }

    private static Sampler customizeSampler(Sampler sampler, ConfigProperties configProperties) {
        return new DynamicRatioSampler();
    }

    private static Map<String, String> customizeIgniteExecutors(ConfigProperties configProperties) {
        return Map.of("otel.instrumentation.executors.include",
                "org.apache.ignite.internal.thread.StripedThreadPoolExecutor,"
                + "org.apache.ignite.internal.thread.StripedScheduledThreadPoolExecutor,"
                + "org.apache.ignite.raft.jraft.util.MetricThreadPoolExecutor,"
                + "org.apache.ignite.raft.jraft.util.MetricScheduledThreadPoolExecutor,"
                + "org.apache.ignite.raft.jraft.util.LogThreadPoolExecutor,"
                + "org.apache.ignite.raft.jraft.util.LogScheduledThreadPoolExecutor,"
                + "org.apache.ignite.raft.jraft.util.concurrent.MpscSingleThreadExecutor"
        );
    }

    // Choose methods to instrument. Can be used as an alternative of @WithSpan or manual span building.
    private static Map<String, String> customizeIncludedMethods(ConfigProperties configProperties) {
        String existed = configProperties.getString("otel.instrumentation.methods.include");

        Set<String> methods = Set.of(
                "org.apache.ignite.internal.replicator.ReplicaService"
                        + "[invoke,sendToReplica]",
                "org.apache.ignite.internal.catalog.CatalogManagerImpl"
                        + "[createTable,dropTable,addColumn,dropColumn,alterColumn,createIndex,dropIndex,createZone,dropZone,alterZone,"
                        + "renameZone,saveUpdate,saveUpdateAndWaitForActivation]",
                "org.apache.ignite.internal.distributionzones.DistributionZoneManager"
                        + "[createZone,alterZone,dropZone,dataNodes,zoneIdAsyncInternal,directZoneIdInternal]"
        );
        String joined = String.join(";", methods);
        Map<String, String> properties = new HashMap<>();
        properties.put("otel.instrumentation.methods.include", existed == null ? joined : existed + ";" + joined);

        properties.put("otel.instrumentation.methods.include", existed == null ? joined : existed + ";" + joined);
        return properties;
    }
}
