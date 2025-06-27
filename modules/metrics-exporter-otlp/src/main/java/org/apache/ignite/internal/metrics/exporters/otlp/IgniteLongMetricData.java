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

import static io.opentelemetry.sdk.metrics.data.MetricDataType.LONG_GAUGE;
import static java.util.Collections.emptyList;

import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.Data;
import io.opentelemetry.sdk.metrics.data.LongExemplarData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.resources.Resource;
import java.util.List;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.util.Lazy;

/**
 * Metric data that holds long metric.
 */
class IgniteLongMetricData extends IgniteMetricData<LongMetric> {
    private final Data<IgniteLongPointData> data;

    IgniteLongMetricData(Lazy<Resource> resource, InstrumentationScopeInfo scope, LongMetric metric) {
        super(resource, scope, metric);

        this.data = new IgniteGaugeData<>(new IgniteLongPointData(metric));
    }

    @Override
    public MetricDataType getType() {
        return LONG_GAUGE;
    }

    @Override
    public Data<?> getData() {
        return data;
    }

    private static class IgniteLongPointData extends IgnitePointData implements LongPointData {
        private final LongMetric metric;

        IgniteLongPointData(LongMetric metric) {
            this.metric = metric;
        }

        @Override
        public long getValue() {
            return metric.value();
        }

        @Override
        public List<LongExemplarData> getExemplars() {
            return emptyList();
        }
    }
}
