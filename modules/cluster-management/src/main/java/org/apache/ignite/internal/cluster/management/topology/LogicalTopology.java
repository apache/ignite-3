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

package org.apache.ignite.internal.cluster.management.topology;

import java.util.Collection;
import java.util.Set;
import org.apache.ignite.network.ClusterNode;

/**
 * Used to manage logical topology information available locally on the current node.
 * It is fed by internal CMG-related components with information about logical topology changes.
 */
public interface LogicalTopology {
    /**
     * Retrieves the current logical topology.
     */
    Collection<ClusterNode> getLogicalTopology();

    /**
     * Puts a given node as a part of the logical topology.
     *
     * @param node Node to put.
     */
    void putLogicalTopologyNode(ClusterNode node);

    /**
     * Removes given nodes from the logical topology.
     *
     * @param nodes Nodes to remove.
     */
    void removeLogicalTopologyNodes(Set<ClusterNode> nodes);

    /**
     * Returns {@code true} if a given node is present in the logical topology or {@code false} otherwise.
     */
    boolean isNodeInLogicalTopology(ClusterNode node);
}
