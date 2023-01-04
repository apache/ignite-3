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

import com.google.gson.Gson;
import jakarta.inject.Singleton;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.core.JdbcUrl;
import org.apache.ignite.internal.cli.core.repl.AsyncSessionEventListener;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.repl.config.RootConfig;
import org.apache.ignite.internal.cli.core.repl.registry.JdbcUrlRegistry;
import org.apache.ignite.internal.cli.core.repl.registry.NodeNameRegistry;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.invoker.Configuration;

/** Implementation of {@link JdbcUrlRegistry}. */
@Singleton
public class JdbcUrlRegistryImpl implements JdbcUrlRegistry, AsyncSessionEventListener {

    private static final IgniteLogger log = CliLoggers.forClass(JdbcUrlRegistryImpl.class);

    private final NodeNameRegistry nodeNameRegistry;

    private volatile Set<JdbcUrl> jdbcUrls = Set.of();

    private ScheduledExecutorService executor;

    public JdbcUrlRegistryImpl(NodeNameRegistry nodeNameRegistry) {
        this.nodeNameRegistry = nodeNameRegistry;
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
        return jdbcUrls.stream()
                .map(JdbcUrl::toString)
                .collect(Collectors.toSet());
    }

    private JdbcUrl fetchJdbcUrl(String nodeUrl) {
        try {
            return constructJdbcUrl(fetchNodeConfiguration(nodeUrl), nodeUrl);
        } catch (Exception e) {
            log.warn("Couldn't fetch jdbc url of " + nodeUrl + " node: ", e);
            return null;
        }
    }

    private String fetchNodeConfiguration(String nodeUrl) throws ApiException {
        return new NodeConfigurationApi(Configuration.getDefaultApiClient().setBasePath(nodeUrl)).getNodeConfiguration();
    }

    private JdbcUrl constructJdbcUrl(String configuration, String nodeUrl) {
        try {
            int port = new Gson().fromJson(configuration, RootConfig.class).clientConnector.port;
            return JdbcUrl.of(nodeUrl, port);
        } catch (MalformedURLException ignored) {
            // Shouldn't happen ever since we are now connected to this URL
            return null;
        }
    }

    @Override
    public void onConnect(SessionInfo sessionInfo) {
        if (executor == null) {
            executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("JdbcUrlRegistry", log));
            executor.scheduleWithFixedDelay(() -> fetchJdbcUrls(), 0, 5, TimeUnit.SECONDS);
        }
    }

    @Override
    public void onDisconnect() {

    }
}
