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

package org.apache.ignite.internal.network.message;

import static org.apache.ignite.internal.network.NetworkMessageTypes.CLUSTER_NODE_MESSAGE;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.network.NetworkAddress;

/**
 * {@link InternalClusterNode} as a network message class.
 */
@Transferable(CLUSTER_NODE_MESSAGE)
public interface ClusterNodeMessage extends NetworkMessage, Serializable {
    /** Node ID. */
    UUID id();

    /** Node name, aka consistent ID. */
    String name();

    /** Host (part of the {@link NetworkAddress} of the node. */
    String host();

    /** Port (part of the {@link NetworkAddress} of the node. */
    int port();

    /** Converts this message to the corresponding {@link InternalClusterNode} instance. */
    default InternalClusterNode asClusterNode() {
        return new ClusterNodeImpl(id(), name(), new NetworkAddress(host(), port()));
    }
}
