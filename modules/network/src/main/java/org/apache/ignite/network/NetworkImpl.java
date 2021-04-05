/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.network;

/**
 * Default implementation of a {@link Network}.
 * <p>
 * Implementing classes should use {@link #start()} and {@link #shutdown()} to allocate and free any network-related
 * resources.
 */
public abstract class NetworkImpl implements Network {
    /** Context. */
    private final NetworkContext context;

    /** Topology service. */
    private final TopologyService topologyService;

    /** Messaging service. */
    private final MessagingService messagingService;

    /** */
    public NetworkImpl(
        NetworkContext context,
        TopologyService topologyService,
        MessagingService messagingService
    ) {
        this.context = context;
        this.topologyService = topologyService;
        this.messagingService = messagingService;
    }

    /** {@inheritDoc} */
    @Override public final NetworkContext getContext() {
        return context;
    }

    /** {@inheritDoc} */
    @Override public final TopologyService getTopologyService() {
        return topologyService;
    }

    /** {@inheritDoc} */
    @Override public final MessagingService getMessagingService() {
        return messagingService;
    }
}
