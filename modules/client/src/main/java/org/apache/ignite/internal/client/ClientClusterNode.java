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

package org.apache.ignite.internal.client;

import java.util.Objects;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Client representation of a node in a cluster.
 */
public class ClientClusterNode implements ClusterNode {
    private static final long serialVersionUID = 3574984937527678507L;

    /** Local ID assigned to the node instance. The ID changes between restarts. */
    private final String id;

    /** Unique name of a cluster member, {@code null} if the node has not been added to the topology. Consistent between restarts. */
    private final @Nullable String name;

    /** Network address of the node. */
    private final NetworkAddress address;

    /** Metadata of this node. */
    private final @Nullable NodeMetadata nodeMetadata;

    /**
     * Constructor.
     *
     * @param id Local id that changes between restarts.
     * @param name Unique name of a member in a cluster, {@code null} if the node has not been added to the topology.
     * @param address Node address.
     * @param nodeMetadata Node metadata.
     */
    public ClientClusterNode(String id, @Nullable String name, NetworkAddress address, @Nullable NodeMetadata nodeMetadata) {
        this.id = id;
        this.name = name;
        this.address = address;
        this.nodeMetadata = nodeMetadata;
    }

    /**
     * Constructor.
     *
     * @param id Local ID that changes between restarts.
     * @param name Unique name of a cluster member, {@code null} if the node has not been added to the topology.
     * @param address Node address.
     */
    public ClientClusterNode(String id, @Nullable String name, NetworkAddress address) {
        this(id, name, address, null);
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public @Nullable String name() {
        return name;
    }

    @Override
    public NetworkAddress address() {
        return address;
    }

    @Override
    public @Nullable NodeMetadata nodeMetadata() {
        return nodeMetadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClientClusterNode that = (ClientClusterNode) o;
        return Objects.equals(name, that.name) && address.equals(that.address);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(name);
        result = 31 * result + address.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return S.toString(ClientClusterNode.class, this);
    }
}
