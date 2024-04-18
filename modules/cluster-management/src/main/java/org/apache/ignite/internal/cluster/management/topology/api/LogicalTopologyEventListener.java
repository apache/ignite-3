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

/**
 * Listens to events related to logical topology changes.
 *
 * <p>Event listeners are not guaranteed to see events they receive being consistent with the state acquired from
 * {@link LogicalTopologyService#logicalTopologyOnLeader()} (or local logical topology state obtained in other means). This means that,
 * if you get an event with topology version N, event listener might see version M greater than N if it
 * tries to get current logical topology from the CMG leader, or it might see version M less or greater than N if it
 * tries to get current local topology from the local storage.
 *
 * <p>Event listener methods must return as quickly as possible. If some heavy processing, blocking I/O or waiting
 * for a future has to be done, this should be offloaded to another thread.
 *
 * <p>While an event listener is registered, it is guaranteed to get all logical topology events, in the correct order.
 * While an event listener is NOT registered (for instance, when its node is restarting), events are skipped. This
 * means that, if an Ignite node is restarting, it might miss some logical topology events.
 * {@link LogicalTopologyService#logicalTopologyOnLeader()} should be used to catch up.
 */
public interface LogicalTopologyEventListener {
    /**
     * Called after a node has been validated, but not yet joined the Logical Topology.
     *
     * @param validatedNode The validated node.
     */
    default void onNodeValidated(LogicalNode validatedNode) {
        // No-op.
    }

    /**
     * Called if a node has passed validation but left before joining the cluster.
     *
     * @param invalidatedNode Node that left the cluster.
     */
    default void onNodeInvalidated(LogicalNode invalidatedNode) {
        // No-op.
    }

    /**
     * Called when a new member has joined a cluster's logical topology.
     *
     * @param joinedNode Appeared cluster member.
     * @param newTopology Topology in which the member has joined.
     */
    default void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {
        // No-op.
    }

    /**
     * Called when a member has left a cluster's logical topology.
     *
     * @param leftNode Disappeared cluster member.
     * @param newTopology Topology in which the member has disappeared.
     */
    default void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
        // No-op.
    }

    /**
     * Called when a topology changed in a way that cannot be interpreted as a sequence of 'appeared'/'disappeared' events,
     * so the change happens as a leap forward. This is mutually exclusive with {@link #onNodeJoined(LogicalNode, LogicalTopologySnapshot)}
     * and {@link #onNodeLeft(LogicalNode, LogicalTopologySnapshot)}.
     *
     * <p>This happens rarely (in a well-configured system this should never happen), for instance, when a Cluster Management
     * RAFT Group sends its update by installing a RAFT snapshot instead of normal AppendEntries.
     *
     * @param newTopology The new logical topology state.
     */
    default void onTopologyLeap(LogicalTopologySnapshot newTopology) {
        // No-op.
    }
}
