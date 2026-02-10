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

package org.apache.ignite.internal.rest.metrics;

import static java.util.stream.Collectors.toList;

import io.micronaut.http.annotation.Controller;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.metric.Metric;
import org.apache.ignite.internal.rest.api.metric.MetricSet;
import org.apache.ignite.internal.rest.api.metric.MetricSource;
import org.apache.ignite.internal.rest.api.metric.NodeMetricApi;
import org.apache.ignite.internal.rest.metrics.exception.MetricNotFoundException;

/** Node metric controller. */
@Controller("/management/v1/metric/node")
public class NodeMetricController implements NodeMetricApi, ResourceHolder {
    private MetricManager metricManager;

    public NodeMetricController(MetricManager metricManager) {
        this.metricManager = metricManager;
    }

    @Override
    public void enable(String srcName) {
        try {
            metricManager.enable(srcName);
        } catch (IllegalStateException e) {
            throw new MetricNotFoundException(e);
        }
    }

    @Override
    public void disable(String srcName) {
        try {
            metricManager.disable(srcName);
        } catch (IllegalStateException e) {
            throw new MetricNotFoundException(e);
        }
    }

    @Override
    public Collection<MetricSource> listMetricSources() {
        return metricManager.metricSources().stream()
                .map(source -> new MetricSource(source.name(), source.enabled()))
                .sorted(Comparator.comparing(MetricSource::name))
                .collect(toList());
    }

    @Override
    public Collection<MetricSet> listMetricSets() {
        return metricManager.metricSnapshot().metrics().values().stream()
                .map(metricSet -> {
                    List<Metric> metricDtos = StreamSupport.stream(metricSet.spliterator(), false)
                            .map(metric -> new Metric(metric.name(), metric.description()))
                            .sorted(Comparator.comparing(Metric::name))
                            .collect(toList());

                    return new MetricSet(metricSet.name(), metricDtos);
                })
                .sorted(Comparator.comparing(MetricSet::name))
                .collect(toList());
    }

    @Override
    public void cleanResources() {
        metricManager = null;
    }
}
