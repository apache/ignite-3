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

import java.util.Collection;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;

/**
 * Entry point for obtaining physical cluster topology information.
 */
// TODO: allow removing event handlers, see https://issues.apache.org/jira/browse/IGNITE-14519
public interface TopologyService extends ClusterNodeResolver, JoinedNodes {
    /**
     * Returns information about the current node.
     *
     * @return Information about the local network member.
     */
    InternalClusterNode localMember();

    /**
     * Returns a list of all discovered cluster members, including the local member itself.
     *
     * @return List of the discovered cluster members.
     */
    Collection<InternalClusterNode> allMembers();

    /**
     * Returns a list of all logical topology members, including the local member.
     *
     * @return List of logical topology members.
     */
    Collection<InternalClusterNode> logicalTopologyMembers();

    /**
     * Registers a handler for physical topology change events.
     *
     * @param handler Physical topology event handler.
     */
    void addEventHandler(TopologyEventHandler handler);

    /**
     * Unregisters a handler for physical topology change events.
     *
     * @param handler Physical topology event handler.
     */
    void removeEventHandler(TopologyEventHandler handler);

    /**
     * Returns a cluster node specified by its network address in the 'host:port' format.
     *
     * @param addr The network address.
     * @return The node object; {@code null} if the node has not been discovered or is offline.
     */
    @Nullable InternalClusterNode getByAddress(NetworkAddress addr);
}
