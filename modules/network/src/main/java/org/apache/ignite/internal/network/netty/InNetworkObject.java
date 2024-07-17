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

package org.apache.ignite.internal.network.netty;

import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Marshallable;
import org.apache.ignite.internal.network.serialization.DescriptorRegistry;
import org.apache.ignite.network.ClusterNode;

/**
 * Wrapper for the received network message.
 */
public class InNetworkObject {
    /** Message. */
    private final NetworkMessage message;

    private final ClusterNode sender;

    private final short connectionIndex;

    /** DescriptorRegistry that will be used for the deserialization of the message's {@link Marshallable} fields. */
    private final DescriptorRegistry registry;

    /** Constructor. */
    public InNetworkObject(NetworkMessage message, ClusterNode sender, short connectionIndex, DescriptorRegistry registry) {
        this.message = message;
        this.sender = sender;
        this.connectionIndex = connectionIndex;
        this.registry = registry;
    }

    /**
     * Returns message.
     *
     * @return Message.
     */
    public NetworkMessage message() {
        return message;
    }

    /**
     * Returns a {@link ClusterNode} representing the sender.
     */
    public ClusterNode sender() {
        return sender;
    }

    /**
     * Returns node ID of the sender that does not survive node restart (aka launch ID).
     */
    public String launchId() {
        return sender.id();
    }

    /**
     * Returns consistent id.
     *
     * @return Consistent id.
     */
    public String consistentId() {
        return sender.name();
    }

    /**
     * Returns connection index.
     */
    public short connectionIndex() {
        return connectionIndex;
    }

    /**
     * Returns descriptor registry.
     *
     * @return Descriptor registry.
     */
    public DescriptorRegistry registry() {
        return registry;
    }
}
