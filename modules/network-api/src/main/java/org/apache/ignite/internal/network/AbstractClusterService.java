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

import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;

/**
 * Default implementation of a {@link ClusterService}.
 *
 * <p>Extending classes should use {@link #start()} and {@link #stop()} to allocate and free any network-related resources.
 */
public abstract class AbstractClusterService implements ClusterService {
    /** Node name. */
    private final String nodeName;

    /** Topology service. */
    private final TopologyService topologyService;

    /** Messaging service. */
    private final MessagingService messagingService;

    /** Serialization registry. */
    private final MessageSerializationRegistry serializationRegistry;

    /**
     * Constructor.
     *
     * @param topologyService  Topology service.
     * @param messagingService Messaging service.
     */
    public AbstractClusterService(
            String nodeName,
            TopologyService topologyService,
            MessagingService messagingService,
            MessageSerializationRegistry serializationRegistry
    ) {
        this.nodeName = nodeName;
        this.serializationRegistry = serializationRegistry;
        this.topologyService = topologyService;
        this.messagingService = messagingService;
    }

    @Override
    public String nodeName() {
        return nodeName;
    }

    /** {@inheritDoc} */
    @Override
    public final TopologyService topologyService() {
        return topologyService;
    }

    /** {@inheritDoc} */
    @Override
    public final MessagingService messagingService() {
        return messagingService;
    }

    @Override
    public MessageSerializationRegistry serializationRegistry() {
        return serializationRegistry;
    }
}
