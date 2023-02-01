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

import java.util.Collection;
import org.jetbrains.annotations.Nullable;

/**
 * Entry point for obtaining information about a cluster's physical topology.
 *
 * <p>Physical topology (PT) is the set of nodes that are thought to be reachable (from the point of view of this node)
 * for message sending. More precisely, a node appears in the PT when it gets discovered by an underlying
 * discovery mechanism (like SWIM) and disappears from the PT when it disappears from the Logical Topology (LT).
 * This means that a node might still be in the PT when it is not actually reachable anymore (i.e. due to a network partition).
 *
 * <p>A node that was removed from the PT due to being removed from the LT cannot reappear in the PT with same node ID
 * (which means that it will probably have to restart to enter the PT again).
 */
// TODO: allow removing event handlers, see https://issues.apache.org/jira/browse/IGNITE-14519
public interface TopologyService {
    /**
     * Returns information of the current node.
     *
     * @return Information about the local network member.
     */
    ClusterNode localMember();

    /**
     * Returns a list of all cluster members that are considered reachable for messaging, including the local member itself.
     *
     * @return List of all cluster members that are considered reachable for messaging.
     */
    Collection<ClusterNode> allMembers();

    /**
     * Registers a handler for physical topology change events.
     *
     * @param handler Physical topology events handler.
     */
    void addEventHandler(TopologyEventHandler handler);

    /**
     * Returns a cluster node (present in the physical topology) by its network address.
     *
     * @param addr The address.
     * @return The node or {@code null} if the node has not yet been discovered or is offline.
     */
    @Nullable ClusterNode getByAddress(NetworkAddress addr);

    /**
     * Returns a cluster node (present in the physical topology) by its consistent id.
     *
     * @param consistentId Consistent id.
     * @return The node or {@code null} if the node has not yet been discovered or is offline.
     */
    @Nullable ClusterNode getByConsistentId(String consistentId);
}
