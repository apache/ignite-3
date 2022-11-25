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

import static java.util.Collections.emptySet;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.network.ClusterNode;

/**
 * A snapshot of a logical topology as seen locally. Includes nodes participating in the logical topology and the version
 * of the topology (that gets incremented on each update to the topology).
 */
public class LogicalTopologySnapshot implements Serializable {
    private static final long serialVersionUID = 0L;

    /** Initial 'topology' for an empty cluster (before any node joined). */
    static final LogicalTopologySnapshot INITIAL = new LogicalTopologySnapshot(0, emptySet());

    private final long version;

    @IgniteToStringInclude
    private final Set<ClusterNode> nodes;

    private LogicalTopologySnapshot(long version, Collection<ClusterNode> nodes) {
        this.version = version;
        this.nodes = Set.copyOf(nodes);
    }

    /**
     * Returns the version of this logical topology (which is incremented on each change to the topology).
     */
    public long version() {
        return version;
    }

    /**
     * Returns the nodes that comprise the logical topology.
     */
    public Set<ClusterNode> nodes() {
        return nodes;
    }

    LogicalTopologySnapshot addNode(ClusterNode nodeToAdd) {
        Map<String, ClusterNode> map = nodes.stream().collect(toMap(ClusterNode::name, identity()));

        ClusterNode oldNode = map.put(nodeToAdd.name(), nodeToAdd);
        if (oldNode != null && oldNode.id().equals(nodeToAdd.id())) {
            // We already have this node, nothing needs to be changed.
            return this;
        }

        return new LogicalTopologySnapshot(version + 1, map.values());
    }

    LogicalTopologySnapshot removeNodesByIds(Set<ClusterNode> nodesToRemove) {
        Map<String, ClusterNode> mapById = nodes.stream().collect(toMap(ClusterNode::id, identity()));

        int originalSize = mapById.size();

        for (ClusterNode nodeToRemove : nodesToRemove) {
            mapById.remove(nodeToRemove.id());
        }

        if (mapById.size() == originalSize) {
            // Nothing was actually removed.
            return this;
        }

        return new LogicalTopologySnapshot(version + 1, mapById.values());
    }

    boolean containsNodeById(ClusterNode needle) {
        return nodes.stream().anyMatch(node -> node.id().equals(needle.id()));
    }

    @Override
    public String toString() {
        return S.toString(LogicalTopologySnapshot.class, this);
    }
}
