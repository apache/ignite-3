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

import jakarta.inject.Singleton;
import java.net.URL;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.core.JdbcUrlFactory;
import org.apache.ignite.internal.cli.core.repl.AsyncSessionEventListener;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.repl.registry.JdbcUrlRegistry;
import org.apache.ignite.internal.cli.core.repl.registry.NodeNameRegistry;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.invoker.ApiException;

/** Implementation of {@link JdbcUrlRegistry}. */
@Singleton
public class JdbcUrlRegistryImpl implements JdbcUrlRegistry, AsyncSessionEventListener {

    private static final IgniteLogger log = CliLoggers.forClass(JdbcUrlRegistryImpl.class);

    private final NodeNameRegistry nodeNameRegistry;

    private final ApiClientFactory clientFactory;

    private final JdbcUrlFactory jdbcUrlFactory;

    private volatile Set<String> jdbcUrls = Set.of();

    private ScheduledExecutorService executor;

    /** Constructor. */
    public JdbcUrlRegistryImpl(NodeNameRegistry nodeNameRegistry, ApiClientFactory clientFactory, JdbcUrlFactory jdbcUrlFactory) {
        this.nodeNameRegistry = nodeNameRegistry;
        this.clientFactory = clientFactory;
        this.jdbcUrlFactory = jdbcUrlFactory;
    }

    private void fetchJdbcUrls() {
        jdbcUrls = nodeNameRegistry.urls()
                .stream()
                .map(URL::toString)
                .map(this::fetchJdbcUrl)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    /** {@inheritDoc} */
    @Override
    public Set<String> jdbcUrls() {
        return Set.copyOf(jdbcUrls);
    }

    private String fetchJdbcUrl(String nodeUrl) {
        try {
            return jdbcUrlFactory.constructJdbcUrl(fetchNodeConfiguration(nodeUrl), nodeUrl);
        } catch (ApiException e) {
            log.warn("Couldn't fetch jdbc url of " + nodeUrl + " node: ", e);
            return null;
        }
    }

    private String fetchNodeConfiguration(String nodeUrl) throws ApiException {
        return new NodeConfigurationApi(clientFactory.getClient(nodeUrl)).getNodeConfiguration();
    }

    @Override
    public synchronized void onConnect(SessionInfo sessionInfo) {
        if (executor == null) {
            executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("JdbcUrlRegistry", log));
            executor.scheduleWithFixedDelay(this::fetchJdbcUrls, 0, 5, TimeUnit.SECONDS);
        }
    }

    @Override
    public synchronized void onDisconnect() {
        if (executor != null) {
            shutdownAndAwaitTermination(executor, 3, TimeUnit.SECONDS);
            executor = null;
        }
    }
}
