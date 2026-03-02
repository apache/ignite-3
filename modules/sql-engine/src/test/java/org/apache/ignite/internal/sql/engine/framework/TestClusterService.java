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

package org.apache.ignite.internal.sql.engine.framework;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.sql.engine.framework.ClusterServiceFactory.LocalMessagingService;
import org.apache.ignite.internal.sql.engine.framework.ClusterServiceFactory.LocalTopologyService;
import org.apache.ignite.network.NodeMetadata;

class TestClusterService implements ClusterService {
    private final String nodeName;
    private final ClusterServiceFactory factory;
    private final Map<String, LocalTopologyService> topologyServicesByNode;
    private final Map<String, LocalMessagingService> messagingServicesByNode;

    TestClusterService(
            String nodeName,
            ClusterServiceFactory factory,
            Map<String, LocalTopologyService> topologyServicesByNode,
            Map<String, LocalMessagingService> messagingServicesByNode
    ) {
        this.nodeName = nodeName;
        this.factory = factory;
        this.topologyServicesByNode = topologyServicesByNode;
        this.messagingServicesByNode = messagingServicesByNode;
    }

    @Override
    public String nodeName() {
        throw new AssertionError("Should not be called");
    }

    /** {@inheritDoc} */
    @Override
    public TopologyService topologyService() {
        return topologyServicesByNode.computeIfAbsent(nodeName, factory::createTopologyService);
    }

    /** {@inheritDoc} */
    @Override
    public MessagingService messagingService() {
        return messagingServicesByNode.computeIfAbsent(nodeName, key -> factory.createMessagingService(nodeName));
    }

    /** {@inheritDoc} */
    @Override
    public void updateMetadata(NodeMetadata metadata) {
        throw new AssertionError("Should not be called");
    }

    @Override
    public MessageSerializationRegistry serializationRegistry() {
        throw new AssertionError("Should not be called");
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        factory.stopNode(nodeName);

        return nullCompletedFuture();
    }

    void disconnect(String nodeName) {
        factory.disconnectNode(nodeName);
    }
}
