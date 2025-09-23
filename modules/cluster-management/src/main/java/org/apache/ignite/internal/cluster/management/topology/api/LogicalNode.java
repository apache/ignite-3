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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeMetadata;
import org.jetbrains.annotations.Nullable;

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
    private final Map<String, String> userAttributes;

    private final Map<String, String> systemAttributes;

    /**
     * List of storage profiles, which the node supports.
     */
    private final List<String> storageProfiles;

    /**
     * Constructor.
     *
     * @param id      Local id that changes between restarts.
     * @param name    Unique name of a member in a cluster.
     * @param address Node address.
     */
    public LogicalNode(UUID id, String name, NetworkAddress address) {
        this(id, name, address, null, emptyMap(), emptyMap(), emptyList());
    }

    /**
     * Constructor.
     *
     * @param clusterNode Represents a node in a cluster.
     * @param userAttributes  Node attributes defined in configuration.
     */
    public LogicalNode(InternalClusterNode clusterNode, Map<String, String> userAttributes) {
        this(clusterNode, userAttributes, emptyMap(), emptyList());
    }

    /**
     * Constructor.
     *
     * @param node Represents a node in a cluster.
     * @param userAttributes Node attributes defined in configuration.
     * @param systemAttributes Internal node attributes provided by system components at startup.
     * @param storageProfiles List of storage profiles, which the node supports.
     */
    public LogicalNode(
            InternalClusterNode node,
            Map<String, String> userAttributes,
            Map<String, String> systemAttributes,
            List<String> storageProfiles
    ) {
        this(node.id(), node.name(), node.address(), node.nodeMetadata(), userAttributes, systemAttributes, storageProfiles);
    }

    /**
     * Constructor.
     *
     * @param clusterNode    Represents a node in a cluster.
     */
    public LogicalNode(InternalClusterNode clusterNode) {
        this(clusterNode, emptyMap(), emptyMap(), emptyList());
    }

    LogicalNode(
            UUID id,
            String name,
            NetworkAddress address,
            @Nullable NodeMetadata nodeMetadata,
            Map<String, String> userAttributes,
            Map<String, String> systemAttributes,
            List<String> storageProfiles
    ) {
        super(id, name, address, nodeMetadata);

        this.userAttributes = Map.copyOf(userAttributes);
        this.systemAttributes = Map.copyOf(systemAttributes);
        this.storageProfiles = List.copyOf(storageProfiles);
    }

    /**
     * Returns node's attributes provided by user using node's configuration.
     *
     * @return Node's attributes provided by user using node's configuration.
     */
    public Map<String, String> userAttributes() {
        return userAttributes;
    }

    /**
     * Returns Internal node attributes provided by system components at startup.
     *
     * @return Internal node attributes provided by system components at startup.
     */
    public Map<String, String> systemAttributes() {
        return systemAttributes;
    }

    /**
     * Returns the list of storage profiles, which the node supports.
     *
     * @return List of storage profiles, which the node supports.
     */
    public List<String> storageProfiles() {
        return storageProfiles;
    }
}
