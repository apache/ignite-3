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

/**
 * Listens to events related to logical topology changes.
 */
public interface LogicalTopologyEventListener {
    /**
     * Called when a new member has joined a cluster's logical topology.
     *
     * @param appearedNode Appeared cluster member.
     * @param newTopology Topology in which the member has joined.
     */
    default void onAppeared(ClusterNode appearedNode, LogicalTopologySnapshot newTopology) {
        // No-op.
    }

    /**
     * Called when a member has left a cluster's logical topology.
     *
     * @param disappearedNode Disappeared cluster member.
     * @param newTopology Topology in which the member has disappeared.
     */
    default void onDisappeared(ClusterNode disappearedNode, LogicalTopologySnapshot newTopology) {
        // No-op.
    }

    /**
     * Called when a topology changed in a way that cannot be interpreted as a sequence of 'appeared'/'disappeared' events,
     * so the change happens as a leap forward. This is mutually exclusive with {@link #onAppeared(ClusterNode, LogicalTopologySnapshot)}
     * and {@link #onDisappeared(ClusterNode, LogicalTopologySnapshot)}.
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
