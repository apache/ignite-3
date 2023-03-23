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

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.cli.call.node.metric.NodeMetricSourceListCall;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.internal.cli.core.repl.AsyncSessionEventListener;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.repl.registry.MetricRegistry;

/** Implementation of {@link MetricRegistry}. */
@Singleton
public class MetricRegistryImpl implements MetricRegistry, AsyncSessionEventListener {

    @Inject
    private NodeMetricSourceListCall metricSourceListCall;

    private final Set<String> metricSources = ConcurrentHashMap.newKeySet();

    @Override
    public Set<String> metricSources() {
        return metricSources;
    }

    /**
     * Gets list of metric sources from the node.
     *
     * @param sessionInfo sessionInfo.
     */
    @Override
    public void onConnect(SessionInfo sessionInfo) {
        CompletableFuture.runAsync(() -> {
            try {
                //TODO https://issues.apache.org/jira/browse/IGNITE-17416
                metricSourceListCall.execute(new UrlCallInput(sessionInfo.nodeUrl()))
                        .body()
                        .forEach(source -> metricSources.add(source.getName()));
            } catch (Exception ignored) {
                // no-op
            }
        });
    }

    /**
     * Clears metric sources list.
     */
    @Override
    public void onDisconnect() {
        metricSources.clear();
    }
}
