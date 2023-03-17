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

package org.apache.ignite.internal.cli.core.repl.registry.impl;

import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.cli.call.node.metric.NodeMetricSourceListCall;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.internal.cli.core.repl.AsyncSessionEventListener;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.repl.registry.MetricRegistry;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.thread.NamedThreadFactory;

/** Implementation of {@link MetricRegistry}. */
@Singleton
public class MetricRegistryImpl implements MetricRegistry, AsyncSessionEventListener {

    private static final IgniteLogger LOG = CliLoggers.forClass(MetricRegistryImpl.class);

    @Inject
    private NodeMetricSourceListCall metricSourceListCall;

    private ScheduledExecutorService executor;

    private final AtomicReference<String> lastKnownUrl = new AtomicReference<>(null);

    private final Set<String> metricSources = ConcurrentHashMap.newKeySet();

    @Override
    public Set<String> metricSources() {
        return metricSources;
    }

    @Override
    public void refresh() {
        updateMetrics(lastKnownUrl.get());
    }

    /**
     * Start pulling updates from a node.
     *
     * @param sessionInfo sessionInfo.
     */
    @Override
    public void onConnect(SessionInfo sessionInfo) {
        if (executor == null) {
            executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("NodeNameRegistry", LOG));
            executor.scheduleWithFixedDelay(() ->
                    updateMetrics(sessionInfo.nodeUrl()), 0, 5, TimeUnit.SECONDS);
        }
    }

    /**
     * Stops pulling updates.
     */
    @Override
    public void onDisconnect() {
        if (executor != null) {
            shutdownAndAwaitTermination(executor, 3, TimeUnit.SECONDS);
            executor = null;
        }
        clearMetrics();
    }

    private void updateMetrics(String nodeUrl) {
        lastKnownUrl.set(nodeUrl);
        clearMetrics();

        metricSourceListCall.execute(new UrlCallInput(nodeUrl))
                .body()
                .forEach(source -> metricSources.add(source.getName()));
    }

    private void clearMetrics() {
        metricSources.clear();
    }
}
