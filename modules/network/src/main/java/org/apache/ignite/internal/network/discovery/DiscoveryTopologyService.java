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

package org.apache.ignite.internal.network.discovery;

import java.util.Collection;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Service to work with the Discovery Topology. Discovery Topology (DT) is the set of nodes
 * that were discovered by a discovery mechanism (like a SWIM protocol implementation) and which did not leave (from
 * the point of view of the discovery mechanism) yet.
 */
// TODO: allow removing event handlers, see https://issues.apache.org/jira/browse/IGNITE-14519
public interface DiscoveryTopologyService {
    /**
     * Registers a listener for discovery topology change events.
     *
     * @param listener Discovery topology events listener.
     */
    void addDiscoveryEventListener(DiscoveryTopologyEventListener listener);

    /**
     * Returns a list of all discovered cluster members, including the local member itself.
     *
     * @return List of all discovered cluster members.
     */
    Collection<ClusterNode> allDiscoveredMembers();

    /**
     * Returns a cluster node (discovered by a discovery mechanism) by its consistent id.
     *
     * @param consistentId Consistent id.
     * @return The node or {@code null} if the node has not yet been discovered or is offline.
     */
    @Nullable ClusterNode discoveredMemberByConsistentId(String consistentId);

    /**
     * Removes a member from the physical topology.
     *
     * @param memberToRemove Member to remove.
     */
    void removeFromPhysicalTopology(ClusterNode memberToRemove);
}
