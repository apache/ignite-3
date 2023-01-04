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
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.call.cluster.topology.PhysicalTopologyCall;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.internal.cli.core.repl.AsyncSessionEventListener;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.repl.registry.NodeNameRegistry;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.rest.client.model.ClusterNode;
import org.apache.ignite.rest.client.model.NodeMetadata;
import org.jetbrains.annotations.Nullable;

/** Implementation of {@link NodeNameRegistry}. */
@Singleton
public class NodeNameRegistryImpl implements NodeNameRegistry, AsyncSessionEventListener {

    private static final IgniteLogger log = CliLoggers.forClass(NodeNameRegistryImpl.class);

    private final PhysicalTopologyCall physicalTopologyCall;

    private volatile Map<String, URL> nodeNameToNodeUrl = Map.of();

    private ScheduledExecutorService executor;

    public NodeNameRegistryImpl(PhysicalTopologyCall physicalTopologyCall) {
        this.physicalTopologyCall = physicalTopologyCall;
    }

    /** {@inheritDoc} */
    @Override
    public Optional<URL> nodeUrlByName(String nodeName) {
        return Optional.ofNullable(nodeNameToNodeUrl.get(nodeName));
    }

    /** {@inheritDoc} */
    @Override
    public Set<String> names() {
        return nodeNameToNodeUrl.keySet();
    }

    /** {@inheritDoc} */
    @Override
    public Set<URL> urls() {
        return new HashSet<>(nodeNameToNodeUrl.values());
    }

    private void updateNodeNames(String nodeUrl) {
        nodeNameToNodeUrl = physicalTopologyCall.execute(new UrlCallInput(nodeUrl))
                .body()
                .stream()
                .map(this::toNodeNameAndUrlPair)
                .filter(it -> it.url != null)
                .collect(Collectors.toUnmodifiableMap(it -> it.name, it -> it.url));
    }

    private NodeNameAndUrlPair toNodeNameAndUrlPair(ClusterNode node) {
        return new NodeNameAndUrlPair(node.getName(), urlFromClusterNode(node.getMetadata()));
    }

    @Nullable
    private URL urlFromClusterNode(NodeMetadata metadata) {
        if (metadata == null) {
            return null;
        }
        try {
            return new URL("http://" + metadata.getRestHost() + ":" + metadata.getRestPort());
        } catch (Exception e) {
            log.warn("Couldn't create URL: {}", e);
            return null;
        }
    }

    /**
     * Start pulling updates from a node.
     *
     * @param sessionInfo sessionInfo.
     */

    @Override
    public void onConnect(SessionInfo sessionInfo) {
        if (executor == null) {
            executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("NodeNameRegistry", log));
            executor.scheduleWithFixedDelay(() ->
                    updateNodeNames(sessionInfo.nodeUrl()), 0, 5, TimeUnit.SECONDS);
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
    }

    private static class NodeNameAndUrlPair {
        private final String name;
        private final URL url;

        private NodeNameAndUrlPair(String name, @Nullable URL url) {
            this.name = name;
            this.url = url;
        }
    }
}
