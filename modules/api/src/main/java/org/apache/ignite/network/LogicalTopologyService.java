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

import java.util.concurrent.CompletableFuture;

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
     * <p>Event listeners are not guaranteed to see events they receive being consistent with the state acquired from
     * {@link #logicalTopologyOnLeader()} (or local logical topology state obtained in other means). This means that,
     * if you get an event with topology version N, event listener might see version M less or greater than N if it
     * tries to get current logical topology in other means.
     *
     * <p>Event listener methods must return as quickly as possible. If some heavy processing, blocking I/O or waiting
     * for a future has to be done, this should be offloaded to another thread.
     *
     * <p>While an event listener is registered, it is guaranteed to get all logical topology events, in the correct order.
     * While an event listener is NOT registereed (for instance, when its node is restarting), events are skipped. This
     * means that, if an Ignite node is restarting, it might miss some logical topology events. {@link #logicalTopologyOnLeader()}
     * should be used to catch up.
     *
     * @param listener Listener to add.
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
}
