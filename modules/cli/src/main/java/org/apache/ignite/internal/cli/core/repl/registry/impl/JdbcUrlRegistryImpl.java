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

import jakarta.inject.Singleton;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.call.cluster.topology.PhysicalTopologyCall;
import org.apache.ignite.internal.cli.core.JdbcUrlFactory;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.internal.cli.core.repl.PeriodicSessionTask;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.repl.registry.JdbcUrlRegistry;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.rest.client.api.NodeManagementApi;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.ClusterNode;
import org.apache.ignite.rest.client.model.NodeInfo;
import org.jetbrains.annotations.Nullable;

/** Implementation of {@link JdbcUrlRegistry}. */
@Singleton
public class JdbcUrlRegistryImpl implements JdbcUrlRegistry, PeriodicSessionTask {

    private static final IgniteLogger LOG = CliLoggers.forClass(JdbcUrlRegistryImpl.class);

    private final PhysicalTopologyCall physicalTopologyCall;

    private final ApiClientFactory clientFactory;

    private final JdbcUrlFactory jdbcUrlFactory;

    private volatile Set<String> jdbcUrls = Set.of();

    /** Constructor. */
    public JdbcUrlRegistryImpl(PhysicalTopologyCall physicalTopologyCall, ApiClientFactory clientFactory, JdbcUrlFactory jdbcUrlFactory) {
        this.physicalTopologyCall = physicalTopologyCall;
        this.clientFactory = clientFactory;
        this.jdbcUrlFactory = jdbcUrlFactory;
    }

    @Override
    public void update(SessionInfo sessionInfo) {
        jdbcUrls = physicalTopologyCall.execute(new UrlCallInput(sessionInfo.nodeUrl())).body()
                .stream()
                .map(this::fetchJdbcUrl)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    @Override
    public void onDisconnect() {
        jdbcUrls = Set.of();
    }

    /** {@inheritDoc} */
    @Override
    public Set<String> jdbcUrls() {
        return Set.copyOf(jdbcUrls);
    }

    @Nullable
    private String fetchJdbcUrl(ClusterNode node) {
        try {
            String nodeUrl = NodeNameRegistryImpl.urlFromClusterNode(node.getMetadata());
            if (nodeUrl != null) {
                NodeInfo nodeInfo = new NodeManagementApi(clientFactory.getClient(nodeUrl)).nodeInfo();
                return jdbcUrlFactory.constructJdbcUrl(nodeUrl, nodeInfo.getJdbcPort());
            }
        } catch (ApiException e) {
            LOG.warn("Couldn't fetch jdbc url of node {}", e, node.getName());
        }
        return null;
    }
}
