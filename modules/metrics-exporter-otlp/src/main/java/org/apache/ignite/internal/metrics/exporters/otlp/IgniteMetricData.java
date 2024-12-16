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

import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.resources.Resource;
import java.util.Objects;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.util.Lazy;
import org.jetbrains.annotations.Nullable;

/**
 * Metric data represents the aggregated measurements of an instrument.
 *
 * @param <T> A type of the metric.
 */
abstract class IgniteMetricData<T extends Metric> implements MetricData {
    private final Lazy<Resource> resource;
    private final InstrumentationScopeInfo scope;
    private final T metric;

    IgniteMetricData(Lazy<Resource> resource, InstrumentationScopeInfo scope, T metric) {
        this.resource = resource;
        this.scope = scope;
        this.metric = metric;
    }

    @Override
    public Resource getResource() {
        @Nullable Resource resource0 = resource.get();

        assert resource0 != null;

        return resource0;
    }

    @Override
    public InstrumentationScopeInfo getInstrumentationScopeInfo() {
        return scope;
    }

    @Override
    public String getName() {
        return metric.name();
    }

    @Override
    public String getDescription() {
        // Can't be null.
        return Objects.requireNonNull(metric.description(), "");
    }

    @Override
    public String getUnit() {
        // Can't be null.
        return "";
    }
}
