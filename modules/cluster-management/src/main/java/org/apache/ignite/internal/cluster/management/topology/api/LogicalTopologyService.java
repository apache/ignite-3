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

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.network.ClusterNode;

/**
 * Used for getting information about the cluster's Logical Topology.
 *
 * <p>There are 2 kinds of 'topologies': physical (see {@link TopologyService} and logical.
 * <ul>
 *     <li>Physical topology consists of nodes mutually discovered by a membership protocol (like SWIM)</li>
 *     <li>
 *         Logical topology is a subset of a physical topology and only contains nodes that have successfully
 *         <a href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-77%3A+Node+Join+Protocol+and+Initialization+for+Ignite+3">
 *         joined the cluster</a>
 *     </li>
 * </ul>
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-77%3A+Node+Join+Protocol+and+Initialization+for+Ignite+3">
 *     IEP-77: Node Join Protocol and Initialization for Ignite 3</a>
 * @see TopologyService
 */
public interface LogicalTopologyService {
    /**
     * Adds a listener for logical topology events.
     *
     * @param listener Listener to add.
     * @see LogicalTopologyEventListener
     */
    void addEventListener(LogicalTopologyEventListener listener);

    /**
     * Removes a listener for logical topology events.
     *
     * @param listener Listener to remove.
     */
    void removeEventListener(LogicalTopologyEventListener listener);

    /**
     * Returns a future that, when complete, resolves into a logical topology snapshot as the CMG leader sees it.
     *
     * @return Future that, when complete, resolves into a logical topology snapshot from the point of view of the CMG leader.
     */
    CompletableFuture<LogicalTopologySnapshot> logicalTopologyOnLeader();

    /**
     * Retrieves the current logical topology snapshot stored in the local storage.
     */
    LogicalTopologySnapshot localLogicalTopology();

    /**
     * Returns a future that, when complete, resolves into a list of validated nodes (including ones present in the Logical Topology).
     *
     * @return Future that, when complete, resolves into a list of validated nodes (including ones present in the Logical Topology).
     */
    CompletableFuture<Set<ClusterNode>> validatedNodesOnLeader();
}
