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

package org.apache.ignite.internal.metrics;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.manager.ComponentContext;

/** Test implementation without exporters. */
public class TestMetricManager extends AbstractMetricManager {
    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    /** Returns the metric for the arguments. */
    public <T extends Metric> T metric(String sourceName, String metricName) {
        MetricSet metrics = metricSnapshot().metrics().get(sourceName);

        if (metrics == null) {
            throw new IllegalArgumentException("Metric source not found: " + sourceName);
        }

        T metric = metrics.get(metricName);

        if (metric == null) {
            throw new IllegalArgumentException("Metric not found: " + metricName);
        }

        return metric;
    }
}
