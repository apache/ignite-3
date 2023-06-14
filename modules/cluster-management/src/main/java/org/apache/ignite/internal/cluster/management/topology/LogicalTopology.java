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

import java.util.Set;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;

/**
 * Used to manage logical topology information available locally on the current node.
 * It is fed by internal CMG-related components with information about logical topology changes.
 */
public interface LogicalTopology {
    /**
     * Retrieves the current logical topology snapshot stored in the local storage.
     */
    LogicalTopologySnapshot getLogicalTopology();

    /**
     * Callback that gets called after a node has been validated, but not yet joined the Logical Topology.
     *
     * @param node The validated node.
     */
    void onNodeValidated(LogicalNode node);

    /**
     * Callback that gets called if a node has passed validation but left before joining the cluster.
     *
     * @param node Node that left the cluster.
     */
    void onNodeInvalidated(LogicalNode node);

    /**
     * Puts a given node as a part of the logical topology.
     *
     * @param node Node to put.
     */
    void putNode(LogicalNode node);

    /**
     * Removes given nodes from the logical topology.
     *
     * @param nodes Nodes to remove.
     */
    void removeNodes(Set<LogicalNode> nodes);

    /**
     * Returns {@code true} if a given node is present in the logical topology or {@code false} otherwise.
     */
    boolean isNodeInLogicalTopology(LogicalNode node);

    /**
     * Adds a listener for logical topology events.
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
     * Causes {@link LogicalTopologyEventListener#onTopologyLeap(LogicalTopologySnapshot)} to be fired with the topology snapshot
     * currently stored in an underlying storage. Invoked after the storage has been restored from a snapshot.
     */
    void fireTopologyLeap();
}
