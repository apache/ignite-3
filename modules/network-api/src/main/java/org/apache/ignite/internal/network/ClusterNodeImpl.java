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

import java.util.UUID;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Representation of a node in a cluster.
 */
public class ClusterNodeImpl implements InternalClusterNode {
    /** Local ID assigned to the node instance. The ID changes between restarts. */
    private final UUID id;

    /** Unique name of a cluster member. Consistent between restarts. */
    private final String name;

    /** Network address of the node. */
    private final NetworkAddress address;

    /** Version of this node. */
    private final String version;

    /** Metadata of this node. */
    @Nullable
    private final NodeMetadata nodeMetadata;

    public static ClusterNodeImpl fromPublicClusterNode(ClusterNode node) {
        // ToDo decide do we need to store version in public ClusterNode
        return new ClusterNodeImpl(node.id(), node.name(), node.address(), null, node.nodeMetadata());
    }

    /**
     * Constructor.
     *
     * @param id      Local id that changes between restarts.
     * @param name    Unique name of a member in a cluster.
     * @param address Node address.
     * @param version Node version.
     * @param nodeMetadata Node metadata.
     */
    public ClusterNodeImpl(UUID id, String name, NetworkAddress address, String version, @Nullable NodeMetadata nodeMetadata) {
        this.id = id;
        this.name = name;
        this.address = address;
        this.version = version;
        this.nodeMetadata = nodeMetadata;
    }

    /**
     * Constructor.
     *
     * @param id      Local ID that changes between restarts.
     * @param name    Unique name of a cluster member.
     * @param address Node address.
     * @param version Node version.
     */
    public ClusterNodeImpl(UUID id, String name, NetworkAddress address, String version) {
        this(id, name, address, version, null);
    }

    @Override
    public UUID id() {
        return id;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public NetworkAddress address() {
        return address;
    }

    @Override
    @Nullable
    public String version() {
        return version;
    }

    @Override
    @Nullable
    public NodeMetadata nodeMetadata() {
        return nodeMetadata;
    }

    @Override
    public ClusterNode toPublicNode() {
        return new PublicClusterNodeImpl(id, name, address, nodeMetadata);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClusterNodeImpl that = (ClusterNodeImpl) o;
        if (version != null ? !version.equals(that.version) : that.version != null) {
            return false;
        }
        return name.equals(that.name) && address.equals(that.address) ;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        if (version != null) {
            result = 31 * result + version.hashCode();
        }
        return result;
    }

    @Override
    public String toString() {
        return String.format("{id=%s, name=%s, address=%s, version=%s}", id, name, address, version);
    }
}
