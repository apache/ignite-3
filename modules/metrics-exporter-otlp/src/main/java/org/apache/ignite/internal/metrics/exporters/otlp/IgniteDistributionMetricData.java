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

import static io.opentelemetry.sdk.metrics.data.MetricDataType.HISTOGRAM;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.Data;
import io.opentelemetry.sdk.metrics.data.DoubleExemplarData;
import io.opentelemetry.sdk.metrics.data.HistogramData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.resources.Resource;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.util.Lazy;

/**
 * Metric data that holds distribution metric.
 */
class IgniteDistributionMetricData extends IgniteMetricData<DistributionMetric> {
    private final HistogramData data;

    IgniteDistributionMetricData(Lazy<Resource> resource, InstrumentationScopeInfo scope, DistributionMetric metric) {
        super(resource, scope, metric);

        data = new IgniteHistogramData(new IgniteDistributionPointData(metric));
    }

    @Override
    public MetricDataType getType() {
        return HISTOGRAM;
    }

    @Override
    public Data<?> getData() {
        return data;
    }

    static class IgniteHistogramData implements HistogramData {
        private final Collection<HistogramPointData> points;

        IgniteHistogramData(HistogramPointData data) {
            points = singletonList(data);
        }

        @Override
        public AggregationTemporality getAggregationTemporality() {
            return AggregationTemporality.CUMULATIVE;
        }

        @Override
        public Collection<HistogramPointData> getPoints() {
            return points;
        }
    }

    static class IgniteDistributionPointData extends IgnitePointData implements HistogramPointData {
        private final DistributionMetric metric;

        private final List<Double> boundaries;

        IgniteDistributionPointData(DistributionMetric metric) {
            this.metric = metric;

            boundaries = asDoubleList(metric.bounds());
        }

        @Override
        public double getSum() {
            return Double.NaN;
        }

        @Override
        public long getCount() {
            long totalCount = 0;

            for (long c : metric.value()) {
                totalCount += c;
            }

            return totalCount;
        }

        @Override
        public boolean hasMin() {
            return false;
        }

        @Override
        public double getMin() {
            return Double.NaN;
        }

        @Override
        public boolean hasMax() {
            return false;
        }

        @Override
        public double getMax() {
            return Double.NaN;
        }

        @Override
        public List<Double> getBoundaries() {
            return boundaries;
        }

        @Override
        public List<Long> getCounts() {
            return LongArrayList.wrap(metric.value());
        }

        @Override
        public List<DoubleExemplarData> getExemplars() {
            return emptyList();
        }

        private static List<Double> asDoubleList(long[] array) {
            ArrayList<Double> result = new ArrayList<>(array.length);

            for (long el : array) {
                result.add((double) el);
            }

            return result;
        }
    }
}
