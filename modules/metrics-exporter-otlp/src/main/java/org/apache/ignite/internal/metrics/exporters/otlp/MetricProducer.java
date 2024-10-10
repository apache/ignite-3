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

package org.apache.ignite.internal.metrics.exporters.otlp;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.Clock;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.CollectionRegistration;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableDoublePointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableLongPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.resources.Resource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.metrics.DoubleMetric;
import org.apache.ignite.internal.metrics.IntMetric;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricProvider;
import org.apache.ignite.internal.metrics.MetricSet;
import org.jetbrains.annotations.Nullable;

/**
 * Metrics producer which collect metrics in OpenTelemetry format.
 */
class MetricProducer implements CollectionRegistration {
    private final MetricProvider metricsProvider;

    private final Supplier<UUID> clusterIdSupplier;

    private final String nodeName;

    MetricProducer(MetricProvider metricsProvider, Supplier<UUID> clusterIdSupplier, String nodeName) {
        this.metricsProvider = metricsProvider;
        this.clusterIdSupplier = clusterIdSupplier;
        this.nodeName = nodeName;
    }

    @Override
    public Collection<MetricData> collectAllMetrics() {
        IgniteBiTuple<Map<String, MetricSet>, Long> snapshot = metricsProvider.metrics();
        @Nullable Long metricCount = snapshot.getValue();

        if (metricCount == null || Objects.equals(metricCount, 0L)) {
            return emptyList();
        }

        long epochNanos = Clock.getDefault().now();

        Resource resource = Resource.builder()
                .put("service.name", clusterIdSupplier.get().toString())
                .put("service.instance.id", nodeName)
                .build();

        Collection<MetricData> result = new ArrayList<>(Math.toIntExact(metricCount));

        for (MetricSet metricSet : snapshot.getKey().values())  {
            InstrumentationScopeInfo scope = InstrumentationScopeInfo.builder(metricSet.name())
                    .build();

            for (Metric metric : metricSet) {
                if (metric instanceof IntMetric) {
                    IntMetric intMetric = (IntMetric) metric;

                    result.add(ImmutableMetricData.createLongGauge(
                            resource,
                            scope,
                            metric.name(),
                            metric.description(),
                            "",
                            ImmutableGaugeData.create(
                                    singleton(ImmutableLongPointData.create(epochNanos, epochNanos, Attributes.empty(), intMetric.value()))
                            )
                    ));
                } else if (metric instanceof LongMetric) {
                    LongMetric longMetric = (LongMetric) metric;

                    result.add(ImmutableMetricData.createLongGauge(
                            resource,
                            scope,
                            metric.name(),
                            metric.description(),
                            "",
                            ImmutableGaugeData.create(
                                    singleton(ImmutableLongPointData.create(epochNanos, epochNanos, Attributes.empty(), longMetric.value()))
                            )
                    ));
                } else if (metric instanceof DoubleMetric) {
                    DoubleMetric metric0 = (DoubleMetric) metric;

                    result.add(ImmutableMetricData.createDoubleGauge(
                            resource,
                            scope,
                            metric.name(),
                            metric.description(),
                            "",
                            ImmutableGaugeData.create(
                                    singleton(ImmutableDoublePointData.create(epochNanos, epochNanos, Attributes.empty(), metric0.value()))
                            )
                    ));
                }
            }
        }

        return result;
    }
}
