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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.sql.engine.exec.NodeWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.PartitionWithConsistencyToken;

/**
 * A group of a sources which shares common set of nodes and assignments to be executed.
 *
 * <p>Although source is essentially a leaf node of the fragment represented by scan operator or receiver,
 * there might be intermediate relations that require to be mapped to the certain topology, thus having
 * its own "source" identifier, and, as a result, being part of the colocation group.
 */
public class ColocationGroup implements Serializable {
    private static final long serialVersionUID = 1370403193139083025L;

    private final List<Long> sourceIds;

    private final List<String> nodeNames;

    private final List<NodeWithConsistencyToken> assignments;

    /** Constructor. */
    public ColocationGroup(List<Long> sourceIds, List<String> nodeNames, List<NodeWithConsistencyToken> assignments) {
        this.sourceIds = Objects.requireNonNull(sourceIds, "sourceIds");
        this.nodeNames = Objects.requireNonNull(nodeNames, "nodeNames");
        this.assignments = Objects.requireNonNull(assignments, "assignments");
    }

    /**
     * Get lists of colocation group sources.
     */
    public List<Long> sourceIds() {
        return sourceIds;
    }

    /**
     * Get lists of nodes the query fragment should be executed on.
     */
    public List<String> nodeNames() {
        return nodeNames;
    }

    /**
     * Get list of partitions (index) and nodes (items) having an appropriate partition in OWNING state, calculated for
     * distributed tables, involved in query execution.
     */
    public List<NodeWithConsistencyToken> assignments() {
        return assignments;
    }

    /**
     * Returns list of pairs containing the partition number to scan on the given node with the corresponding enlistment consistency token.
     *
     * @param nodeName Cluster node consistent ID.
     * @return List of pairs containing the partition number to scan on the given node with the corresponding enlistment consistency token.
     */
    public List<PartitionWithConsistencyToken> partitionsWithConsistencyTokens(String nodeName) {
        List<PartitionWithConsistencyToken> partsWithConsistencyTokens = new ArrayList<>();

        for (int p = 0; p < assignments.size(); p++) {
            NodeWithConsistencyToken nodeWithConsistencyToken = assignments.get(p);

            if (Objects.equals(nodeName, nodeWithConsistencyToken.name())) {
                partsWithConsistencyTokens.add(new PartitionWithConsistencyToken(p, nodeWithConsistencyToken.enlistmentConsistencyToken()));
            }
        }

        return partsWithConsistencyTokens;
    }
}
