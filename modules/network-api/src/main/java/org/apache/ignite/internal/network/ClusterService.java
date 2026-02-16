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

package org.apache.ignite.internal.network;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.network.NodeMetadata;

/**
 * Class, that represents the network-related resources of a node and provides entry points for working with the network members of a
 * cluster.
 */
public interface ClusterService extends IgniteComponent {
    /**
     * Returns the network alias of the node.
     */
    String nodeName();

    /**
     * Returns the {@link TopologyService} for working with the cluster topology.
     *
     * @return Topology Service.
     */
    TopologyService topologyService();

    /**
     * Returns the {@link MessagingService} for sending messages to the cluster members.
     *
     * @return Messaging Service.
     */
    MessagingService messagingService();

    /**
     * Returns the message serialization registry.
     */
    MessageSerializationRegistry serializationRegistry();

    /** {@inheritDoc} */
    @Override
    default CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    /**
     * Updates metadata of this cluster node and sends update events to other members.
     *
     * @param metadata new metadata.
     */
    void updateMetadata(NodeMetadata metadata);
}
