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

package org.apache.ignite.compute;

import java.util.Objects;
import java.util.Set;
import org.apache.ignite.network.ClusterNode;

/**
 * All nodes broadcast execution target. Indicates that the job will be executed on all nodes from the provided set.
 */
public class AllNodesBroadcastJobTarget implements BroadcastJobTarget {
    private final Set<ClusterNode> nodes;

    AllNodesBroadcastJobTarget(Set<ClusterNode> nodes) {
        Objects.requireNonNull(nodes);

        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("Nodes collection must not be empty.");
        }

        this.nodes = nodes;
    }

    /**
     * Returns a set of nodes to execute the jobs on.
     *
     * @return A set of nodes to execute the jobs on.
     */
    public Set<ClusterNode> nodes() {
        return nodes;
    }
}
