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

package org.apache.ignite.internal.sql.engine.exec.mapping.largecluster;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.BitSet;
import java.util.List;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.sql.engine.exec.NodeWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTarget;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTargetFactory;

/**
 * A factory that able to create targets for cluster with up to 64 nodes.
 */
public class LargeClusterFactory implements ExecutionTargetFactory {
    private final List<String> nodes;
    private final Object2IntMap<String> nodeNameToId;

    /** Constructor. */
    public LargeClusterFactory(List<String> nodes) {
        this.nodes = nodes;

        nodeNameToId = new Object2IntOpenHashMap<>(nodes.size());
        nodeNameToId.defaultReturnValue(-1);

        int idx = 0;
        for (String name : nodes) {
            int ret = nodeNameToId.putIfAbsent(name, idx++);
            assert ret == -1 : "invalid node";
        }
    }

    @Override
    public ExecutionTarget allOf(List<String> nodes) {
        BitSet nodesSet = new BitSet(nodeNameToId.size());

        for (String name : nodes) {
            int id = nodeNameToId.getOrDefault(name, -1);
            assert id >= 0 : "invalid node";

            nodesSet.set(id);
        }

        return new AllOfTarget(nodesSet);
    }

    @Override
    public ExecutionTarget oneOf(List<String> nodes) {
        return new OneOfTarget(nodeListToMap(nodes));
    }

    @Override
    public ExecutionTarget someOf(List<String> nodes) {
        return new SomeOfTarget(nodeListToMap(nodes));
    }

    @Override
    public ExecutionTarget partitioned(List<TokenizedAssignments> assignments) {
        BitSet[] partitionNodes = new BitSet[assignments.size()];
        long[] enlistmentConsistencyTokens = new long[assignments.size()];

        int idx = 0;
        boolean finalised = true;
        for (TokenizedAssignments assignment : assignments) {
            BitSet nodes = new BitSet(nodeNameToId.size());
            for (Assignment a : assignment.nodes()) {
                int node = nodeNameToId.getOrDefault(a.consistentId(), -1);

                // TODO Ignore unknown node until IGNITE-22969
                if (node != -1) {
                    nodes.set(node);
                }
            }

            assert !nodes.isEmpty() : "No partition node found";

            finalised = finalised && nodes.cardinality() < 2;

            partitionNodes[idx] = nodes;
            enlistmentConsistencyTokens[idx] = assignment.token();
            idx++;
        }

        return new PartitionedTarget(finalised, partitionNodes, enlistmentConsistencyTokens);
    }

    @Override
    public List<String> resolveNodes(ExecutionTarget target) {
        assert target instanceof AbstractTarget : target == null ? "<null>" : target.getClass().getCanonicalName();

        target = ((AbstractTarget) target).finalise();

        return ((AbstractTarget) target).nodes(nodes);
    }

    @Override
    public Int2ObjectMap<NodeWithConsistencyToken> resolveAssignments(ExecutionTarget target) {
        assert target instanceof AbstractTarget : target == null ? "<null>" : target.getClass().getCanonicalName();

        target = ((AbstractTarget) target).finalise();

        return ((AbstractTarget) target).assignments(nodes);
    }

    private BitSet nodeListToMap(List<String> nodes) {
        BitSet nodesSet = new BitSet(nodeNameToId.size());

        for (String name : nodes) {
            int id = nodeNameToId.getOrDefault(name, -1);

            // TODO Ignore unknown node until IGNITE-22969
            if (id != -1) {
                nodesSet.set(id);
            }
        }

        return nodesSet;
    }
}
