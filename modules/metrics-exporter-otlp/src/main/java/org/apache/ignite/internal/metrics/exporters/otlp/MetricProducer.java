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

import static io.opentelemetry.sdk.metrics.data.AggregationTemporality.CUMULATIVE;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNullElse;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.Clock;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.CollectionRegistration;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableDoublePointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableHistogramData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableHistogramPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableLongPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.resources.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.DistributionMetric;
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
    private static final IgniteLogger LOG = Loggers.forClass(MetricProducer.class);

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

        for (MetricSet metricSet : snapshot.getKey().values()) {
            InstrumentationScopeInfo scope = InstrumentationScopeInfo.builder(metricSet.name())
                    .build();

            for (Metric metric : metricSet) {
                if (metric instanceof IntMetric) {
                    IntMetric intMetric = (IntMetric) metric;

                    result.add(ImmutableMetricData.createLongGauge(
                            resource,
                            scope,
                            metric.name(),
                            requireNonNullElse(metric.description(), ""),
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
                            requireNonNullElse(metric.description(), ""),
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
                            requireNonNullElse(metric.description(), ""),
                            "",
                            ImmutableGaugeData.create(
                                    singleton(ImmutableDoublePointData.create(epochNanos, epochNanos, Attributes.empty(), metric0.value()))
                            )
                    ));
                } else if (metric instanceof DistributionMetric) {
                    DistributionMetric metric0 = (DistributionMetric) metric;

                    result.add(ImmutableMetricData.createDoubleHistogram(
                            resource,
                            scope,
                            metric.name(),
                            requireNonNullElse(metric.description(), ""),
                            "",
                            ImmutableHistogramData.create(
                                    CUMULATIVE,
                                    List.of(ImmutableHistogramPointData.create(
                                            epochNanos, epochNanos, Attributes.empty(), Double.NaN, false, Double.NaN, false, Double.NaN,
                                            asDoubleList(boxed(metric0.bounds())),
                                            Arrays.asList(boxed(metric0.value()))
                                    ))
                            )
                    ));
                }

                LOG.debug("Unknown metric class for export " + metric.getClass());
            }
        }

        return result;
    }

    private static List<Double> asDoubleList(Long[] array) {
        ArrayList<Double> result = new ArrayList<>(array.length);

        for (Long el : array) {
            result.add(Double.valueOf(el));
        }

        return result;
    }

    private static Long[] boxed(long[] array) {
        Long[] result = new Long[array.length];
        int i = 0;

        for (long temp : array) {
            result[i++] = temp;
        }

        return result;
    }
}
