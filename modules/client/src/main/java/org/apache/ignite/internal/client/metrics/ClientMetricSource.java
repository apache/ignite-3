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

package org.apache.ignite.internal.client.metrics;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.AtomicLongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSetBuilder;

/**
 * Client-side metrics.
 */
public class ClientMetricSource extends AbstractMetricSource<ClientMetricSource.Holder> {
    /**
     * Constructor.
     */
    public ClientMetricSource() {
        super("client");
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    @Override
    protected void init(MetricSetBuilder bldr, Holder holder) {
        holder.register(bldr);
    }

    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final AtomicLongMetric connectionsInitiated =
                new AtomicLongMetric("ConnectionsInitiated", "Total initiated connections");

        final List<Metric> metrics = Arrays.asList(
                connectionsInitiated
        );

        void register(MetricSetBuilder bldr) {
            for (var metric : metrics) {
                bldr.register(metric);
            }
        }
    }
}
