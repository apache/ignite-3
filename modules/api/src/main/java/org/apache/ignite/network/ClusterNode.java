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

package org.apache.ignite.network;

import java.io.Serializable;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Representation of a node in a cluster.
 */
public class ClusterNode implements Serializable {
    /** Local ID assigned to the node instance. The ID changes between restarts. */
    private final String id;

    /** Unique name of a cluster member. Consistent between restarts. */
    private final String name;

    /** Network address of the node. */
    private final NetworkAddress address;

    /** Metadata of this node. */
    @Nullable
    private final NodeMetadata nodeMetadata;

    /**
     * Constructor.
     *
     * @param id      Local id that changes between restarts.
     * @param name    Unique name of a member in a cluster.
     * @param address Node address.
     * @param nodeMetadata Node metadata.
     */
    public ClusterNode(String id, String name, NetworkAddress address, @Nullable NodeMetadata nodeMetadata) {
        this.id = id;
        this.name = name;
        this.address = address;
        this.nodeMetadata = nodeMetadata;
    }

    /**
     * Constructor.
     *
     * @param id      Local ID that changes between restarts.
     * @param name    Unique name of a cluster member.
     * @param address Node address.
     */
    public ClusterNode(String id, String name, NetworkAddress address) {
        this.id = id;
        this.name = name;
        this.address = address;
    }

    /**
     * Returns the node's local ID.
     *
     * @return Node's local ID.
     */
    public String id() {
        return id;
    }

    /**
     * Returns the unique name (consistent ID) of the node in the cluster. Does not change between restarts.
     *
     * @return Unique name of a cluster member.
     */
    public String name() {
        return name;
    }

    /**
     * Returns the network address of the node.
     *
     * @return Network address of the node.
     */
    public NetworkAddress address() {
        return address;
    }

    @Nullable
    public NodeMetadata nodeMetadata() {
        return nodeMetadata;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClusterNode that = (ClusterNode) o;
        return name.equals(that.name) && address.equals(that.address);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + address.hashCode();
        return result;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(ClusterNode.class, this);
    }
}
