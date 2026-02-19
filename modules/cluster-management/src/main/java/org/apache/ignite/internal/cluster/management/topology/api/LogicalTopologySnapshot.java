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

import static java.util.Collections.emptySet;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.TestOnly;

/**
 * A snapshot of a logical topology as seen locally. Includes nodes participating in the logical topology and the version
 * of the topology (that gets incremented on each update to the topology).
 *
 * <p>Instances of this class are immutable.
 */
public class LogicalTopologySnapshot {
    /** Version that first topology snapshot in history will have. */
    public static final long FIRST_VERSION = 1;

    /** Initial 'topology' for an empty cluster (before any node has joined). */
    public static final LogicalTopologySnapshot INITIAL = new LogicalTopologySnapshot(FIRST_VERSION - 1, emptySet(), new UUID(0, 0));

    private final long version;

    @IgniteToStringInclude
    private final Set<LogicalNode> nodes;

    private final UUID clusterId;

    /** Constructor. */
    public LogicalTopologySnapshot(long version, Collection<LogicalNode> nodes, UUID clusterId) {
        this.version = version;
        this.nodes = Set.copyOf(nodes);
        this.clusterId = clusterId;
    }

    /**
     * Creates a snapshot with a random cluster ID (only suitable for tests that don't care about clusterIds).
     */
    @TestOnly
    public LogicalTopologySnapshot(long version, Collection<LogicalNode> nodes) {
        this(version, nodes, UUID.randomUUID());
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
    public Set<LogicalNode> nodes() {
        return nodes;
    }

    /**
     * Returns {@code true} if this topology snapshot contains a node with the given {@code id}, and {@code false} otherwise.
     * Take into account that this method has O(n) complexity.
     *
     * @param nodeId Node id.
     * @return {@code true} if this topology snapshot contains a node with the given {@code id}, and {@code false} otherwise.
     */
    public boolean hasNode(UUID nodeId) {
        return nodes.stream().anyMatch(node -> nodeId.equals(node.id()));
    }

    /**
     * Returns {@code true} if this topology snapshot contains a node with the given {@code nodeName}, and {@code false} otherwise.
     * Take into account that this method has O(n) complexity.
     *
     * @param nodeName Node name, aka consistent id.
     * @return {@code true} if this topology snapshot contains a node with the given {@code nodeName}, and {@code false} otherwise.
     */
    public boolean hasNode(String nodeName) {
        return nodes.stream().anyMatch(node -> nodeName.equals(node.name()));
    }

    /**
     * Returns a node with the given {@code nodeId} in this topology snapshot.
     * Take into account that this method has O(n) complexity.
     *
     * @param nodeId Node id to find.
     * @return Node with the given {@code nodeId} in this topology snapshot.
     */
    public Optional<LogicalNode> node(UUID nodeId) {
        return nodes.stream().filter(node -> nodeId.equals(node.id())).findAny();
    }

    /**
     * Returns a node with the given {@code nodeId} in this topology snapshot.
     * Take into account that this method has O(n) complexity.
     *
     * @param nodeName Node name to find.
     * @return Node with the given {@code nodeName} in this topology snapshot.
     */
    public Optional<LogicalNode> node(String nodeName) {
        return nodes.stream().filter(node -> nodeName.equals(node.name())).findAny();
    }

    /**
     * Returns ID of the cluster in which this topology snapshot was created.
     */
    public UUID clusterId() {
        return clusterId;
    }

    /**
     * Returns the number of nodes in this snapshot.
     *
     * @return Number of nodes in this snapshot.
     */
    public int size() {
        return nodes.size();
    }

    @Override
    public String toString() {
        return S.toString(LogicalTopologySnapshot.class, this);
    }
}
