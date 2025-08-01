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
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;

import io.micronaut.http.annotation.Controller;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.metrics.message.MetricSourceDto;
import org.apache.ignite.internal.metrics.messaging.MetricMessaging;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.metric.ClusterMetricApi;
import org.apache.ignite.internal.rest.api.metric.MetricSource;
import org.apache.ignite.internal.rest.api.metric.NodeMetricSources;
import org.apache.ignite.internal.rest.metrics.exception.MetricNotFoundException;
import org.apache.ignite.internal.util.ExceptionUtils;

/** Cluster metric controller. */
@Controller("/management/v1/metric/cluster")
public class ClusterMetricController implements ClusterMetricApi, ResourceHolder {
    private MetricMessaging messaging;

    public ClusterMetricController(MetricMessaging messaging) {
        this.messaging = messaging;
    }

    @Override
    public CompletableFuture<Void> enable(String srcName) {
        return messaging.broadcastMetricEnableAsync(srcName).exceptionally(ClusterMetricController::mapException);
    }

    @Override
    public CompletableFuture<Void> disable(String srcName) {
        return messaging.broadcastMetricDisableAsync(srcName).exceptionally(ClusterMetricController::mapException);
    }

    @Override
    public CompletableFuture<Collection<NodeMetricSources>> listMetricSources() {
        return messaging.broadcastMetricSourcesAsync()
                .exceptionally(ClusterMetricController::mapException)
                .thenApply(ClusterMetricController::fromDto);
    }

    private static List<NodeMetricSources> fromDto(Map<String, Collection<MetricSourceDto>> sources) {
        return sources.entrySet().stream()
                .map(ClusterMetricController::fromDto)
                .sorted(Comparator.comparing(NodeMetricSources::node))
                .collect(toList());
    }

    private static NodeMetricSources fromDto(Entry<String, Collection<MetricSourceDto>> entry) {
        List<MetricSource> sources = entry.getValue().stream()
                .map(ClusterMetricController::fromDto)
                .sorted(Comparator.comparing(MetricSource::name))
                .collect(toList());

        return new NodeMetricSources(entry.getKey(), sources);
    }

    private static MetricSource fromDto(MetricSourceDto source) {
        return new MetricSource(source.name(), source.enabled());
    }

    private static <T> T mapException(Throwable throwable) {
        Throwable cause = ExceptionUtils.unwrapCause(throwable);
        if (cause instanceof IllegalStateException) {
            throw new MetricNotFoundException(cause);
        }
        throw sneakyThrow(cause);
    }

    @Override
    public void cleanResources() {
        messaging = null;
    }
}
