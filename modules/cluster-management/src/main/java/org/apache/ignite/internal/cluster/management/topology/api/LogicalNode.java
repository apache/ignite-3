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

package org.apache.ignite.internal.cluster.management.topology.api;

import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.NetworkAddress;

/**
 * Representation of a logical node in a cluster.
 */
public class LogicalNode extends ClusterNodeImpl {
    /**
     * Node's attributes.
     *
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/distribution-zones/tech-notes/filters.md">Filter
     *         documentation</a>
     */
    @IgniteToStringInclude
    private final Map<String, String> nodeAttributes;

    /**
     * Constructor.
     *
     * @param id      Local id that changes between restarts.
     * @param name    Unique name of a member in a cluster.
     * @param address Node address.
     */
    public LogicalNode(
            String id,
            String name,
            NetworkAddress address
    ) {
        super(id, name, address);

        this.nodeAttributes = Collections.emptyMap();
    }

    /**
     * Constructor.
     *
     * @param clusterNode    Represents a node in a cluster.
     * @param nodeAttributes Node attributes.
     */
    public LogicalNode(ClusterNode clusterNode, Map<String, String> nodeAttributes) {
        super(clusterNode.id(), clusterNode.name(), clusterNode.address(), clusterNode.nodeMetadata());

        this.nodeAttributes = nodeAttributes;
    }

    /**
     * Constructor.
     *
     * @param clusterNode    Represents a node in a cluster.
     */
    public LogicalNode(ClusterNode clusterNode) {
        super(clusterNode.id(), clusterNode.name(), clusterNode.address(), clusterNode.nodeMetadata());

        this.nodeAttributes = Collections.emptyMap();
    }

    /**
     * Returns node's attributes.
     *
     * @return Node's attributes.
     */
    public Map<String, String> nodeAttributes() {
        return nodeAttributes;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(LogicalNode.class, this, super.toString());
    }
}
