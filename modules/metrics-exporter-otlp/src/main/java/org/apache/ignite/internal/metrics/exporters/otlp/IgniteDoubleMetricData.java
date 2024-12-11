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

import static io.opentelemetry.sdk.metrics.data.MetricDataType.DOUBLE_GAUGE;
import static java.util.Collections.emptyList;

import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.Data;
import io.opentelemetry.sdk.metrics.data.DoubleExemplarData;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.resources.Resource;
import java.util.List;
import org.apache.ignite.internal.metrics.DoubleMetric;
import org.apache.ignite.internal.util.Lazy;

/**
 * Metric data that holds double metric.
 */
class IgniteDoubleMetricData extends IgniteMetricData<DoubleMetric> {
    private final Data<IgniteDoublePointData> data;

    IgniteDoubleMetricData(Lazy<Resource> resource, InstrumentationScopeInfo scope, DoubleMetric metric) {
        super(resource, scope, metric);

        data = new IgniteGaugeData<>(new IgniteDoublePointData(metric));
    }

    @Override
    public MetricDataType getType() {
        return DOUBLE_GAUGE;
    }

    @Override
    public Data<?> getData() {
        return data;
    }

    private static class IgniteDoublePointData extends IgnitePointData implements DoublePointData {
        private final DoubleMetric metric;

        IgniteDoublePointData(DoubleMetric metric) {
            this.metric = metric;
        }

        @Override
        public double getValue() {
            return metric.value();
        }

        @Override
        public List<DoubleExemplarData> getExemplars() {
            return emptyList();
        }
    }
}
