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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.sql.engine.exec.NodeWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.PartitionProvider;
import org.apache.ignite.internal.sql.engine.exec.PartitionWithConsistencyToken;
import org.jetbrains.annotations.Nullable;

/**
 * A group of a sources which shares common set of nodes and assignments to be executed.
 *
 * <p>Although source is essentially a leaf node of the fragment represented by scan operator or receiver,
 * there might be intermediate relations that require to be mapped to the certain topology, thus having
 * its own "source" identifier, and, as a result, being part of the colocation group.
 */
public class ColocationGroup implements Serializable {
    private static final long serialVersionUID = 1370403193139083025L;

    private final LongList sourceIds;

    private final List<String> nodeNames;

    private final Int2ObjectMap<NodeWithConsistencyToken> assignments;

    private final Map<String, List<PartitionWithConsistencyToken>> partitionsPerNode;

    /** Constructor. */
    public ColocationGroup(LongList sourceIds, List<String> nodeNames, Int2ObjectMap<NodeWithConsistencyToken> assignments) {
        this.sourceIds = Objects.requireNonNull(sourceIds, "sourceIds");
        this.nodeNames = Objects.requireNonNull(nodeNames, "nodeNames");
        this.assignments = Objects.requireNonNull(assignments, "assignments");
        this.partitionsPerNode = null;
    }

    /** Constructor for colocation group with applied partition pruning. */
    public ColocationGroup(
            LongList sourceIds,
            List<String> nodeNames,
            Int2ObjectMap<NodeWithConsistencyToken> assignments,
            Map<String, List<PartitionWithConsistencyToken>> partitionsPerNode
    ) {
        this.sourceIds = Objects.requireNonNull(sourceIds, "sourceIds");
        this.nodeNames = Objects.requireNonNull(nodeNames, "nodeNames");
        this.assignments = Objects.requireNonNull(assignments, "assignments");
        this.partitionsPerNode = Objects.requireNonNull(partitionsPerNode, "partitionsPerNode");
    }

    private ColocationGroup(
            List<String> nodeNames,
            LongList sourceIds,
            Int2ObjectMap<NodeWithConsistencyToken> assignments,
            @Nullable Map<String, List<PartitionWithConsistencyToken>> partitionsPerNode
    ) {
        this.sourceIds = sourceIds;
        this.nodeNames = nodeNames;
        this.assignments = assignments;
        this.partitionsPerNode = partitionsPerNode;
    }

    /**
     * Get lists of colocation group sources.
     */
    public LongList sourceIds() {
        return sourceIds;
    }

    /**
     * Get lists of nodes the query fragment should be executed on.
     */
    public List<String> nodeNames() {
        return nodeNames;
    }

    /**
     * Get map of partitions per and nodes having an appropriate partition in OWNING state, calculated for
     * distributed tables, involved in query execution.
     */
    public Int2ObjectMap<NodeWithConsistencyToken> assignments() {
        return assignments;
    }

    /**
     * Returns list of pairs containing the partition number to scan on the given node with the corresponding enlistment consistency token.
     *
     * @param nodeName Cluster node consistent ID.
     * @return List of pairs containing the partition number to scan on the given node with the corresponding enlistment consistency token.
     */
    public List<PartitionWithConsistencyToken> partitionsWithConsistencyTokens(String nodeName) {
        // If partitionsPerNode field is set, then we should use pre computed partitions applied by partition pruning.
        // Otherwise use use assignments to extract partitions for the given node.
        if (partitionsPerNode != null) {
            return partitionsPerNode.getOrDefault(nodeName, Collections.emptyList());
        } else {
            return PartitionProvider.partitionsForNode(assignments, nodeName);
        }
    }

    /** Creates a copy of this colocation group that does not have the specified source ids. */
    public ColocationGroup removeSources(LongSet ids) {
        LongList list = new LongArrayList(sourceIds);
        list.removeAll(ids);
        return new ColocationGroup(nodeNames, list, assignments, partitionsPerNode);
    }
}
