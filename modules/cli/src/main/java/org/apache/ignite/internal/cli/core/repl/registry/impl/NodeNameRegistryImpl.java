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
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.call.cluster.topology.PhysicalTopologyCall;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.internal.cli.core.repl.PeriodicSessionTask;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.repl.registry.NodeNameRegistry;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.rest.client.model.ClusterNode;
import org.apache.ignite.rest.client.model.NodeMetadata;
import org.jetbrains.annotations.Nullable;

/** Implementation of {@link NodeNameRegistry}. */
@Singleton
public class NodeNameRegistryImpl implements NodeNameRegistry, PeriodicSessionTask {

    private static final IgniteLogger LOG = CliLoggers.forClass(NodeNameRegistryImpl.class);

    private final PhysicalTopologyCall physicalTopologyCall;

    private volatile Map<String, String> nodeNameToNodeUrl = Map.of();

    public NodeNameRegistryImpl(PhysicalTopologyCall physicalTopologyCall) {
        this.physicalTopologyCall = physicalTopologyCall;
    }

    /** {@inheritDoc} */
    @Override
    public Optional<String> nodeUrlByName(String nodeName) {
        return Optional.ofNullable(nodeNameToNodeUrl.get(nodeName));
    }

    /** {@inheritDoc} */
    @Override
    public Set<String> names() {
        return nodeNameToNodeUrl.keySet();
    }

    /** {@inheritDoc} */
    @Override
    public Set<String> urls() {
        return new HashSet<>(nodeNameToNodeUrl.values());
    }

    @Override
    public void update(SessionInfo sessionInfo) {
        nodeNameToNodeUrl = physicalTopologyCall.execute(new UrlCallInput(sessionInfo.nodeUrl()))
                .body()
                .stream()
                .map(NodeNameRegistryImpl::toNodeNameAndUrlPair)
                .filter(it -> it.url != null)
                .collect(Collectors.toUnmodifiableMap(it -> it.name, it -> it.url));
    }

    @Override
    public void onDisconnect() {
        nodeNameToNodeUrl = Map.of();
    }

    private static NodeNameAndUrlPair toNodeNameAndUrlPair(ClusterNode node) {
        return new NodeNameAndUrlPair(node.getName(), urlFromClusterNode(node.getMetadata()));
    }

    @Nullable
    static String urlFromClusterNode(@Nullable NodeMetadata metadata) {
        if (metadata == null) {
            return null;
        }
        try {
            Integer httpsPort = metadata.getHttpsPort();
            if (httpsPort != -1) {
                return new URL("https://" + metadata.getRestHost() + ":" + httpsPort).toString();
            }
            return new URL("http://" + metadata.getRestHost() + ":" + metadata.getHttpPort()).toString();
        } catch (Exception e) {
            LOG.warn("Couldn't create URL: {}", e);
            return null;
        }
    }

    private static class NodeNameAndUrlPair {
        private final String name;
        private final String url;

        private NodeNameAndUrlPair(String name, @Nullable String url) {
            this.name = name;
            this.url = url;
        }
    }
}
