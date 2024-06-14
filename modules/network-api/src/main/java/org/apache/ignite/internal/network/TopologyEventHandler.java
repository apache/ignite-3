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

import org.apache.ignite.network.ClusterNode;

/**
 * Interface for handling the topology change events.
 */
public interface TopologyEventHandler {
    /**
     * Called when a new cluster member has been detected.
     *
     * @param member New cluster member.
     */
    default void onAppeared(ClusterNode member) {
        // no-op
    }

    /**
     * Indicates that a member has left a cluster. Called only when a member leaves permanently (i.e., it is not possible to
     * re-establish a connection to it).
     *
     * @param member Member that has left the cluster.
     */
    default void onDisappeared(ClusterNode member) {
        // no-op
    }
}
