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
import org.apache.calcite.util.BitSets;
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

        int idx = 0;
        for (String name : nodes) {
            nodeNameToId.putIfAbsent(name, idx++);
        }
    }

    @Override
    public ExecutionTarget allOf(List<String> nodes) {
        return new AllOfTarget(nodeListToMap(nodes));
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
    public ExecutionTarget partitioned(List<NodeWithConsistencyToken> nodes) {
        BitSet[] partitionNodes = new BitSet[nodes.size()];
        long[] enlistmentConsistencyTokens = new long[nodes.size()];

        int idx = 0;
        for (NodeWithConsistencyToken e : nodes) {
            int node = nodeNameToId.getOrDefault(e.name(), -1);
            partitionNodes[idx] = node == -1 ? BitSets.of() : BitSets.of(node);
            enlistmentConsistencyTokens[idx++] = e.enlistmentConsistencyToken();
        }

        return new PartitionedTarget(true, partitionNodes, enlistmentConsistencyTokens);
    }

    @Override
    public List<String> resolveNodes(ExecutionTarget target) {
        target = target.finalise();

        assert target instanceof AbstractTarget : target == null ? "<null>" : target.getClass().getCanonicalName();

        return ((AbstractTarget) target).nodes(nodes);
    }

    @Override
    public Int2ObjectMap<NodeWithConsistencyToken> resolveAssignments(ExecutionTarget target) {
        target = target.finalise();

        assert target instanceof AbstractTarget : target == null ? "<null>" : target.getClass().getCanonicalName();

        return ((AbstractTarget) target).assignments(nodes);
    }

    private BitSet nodeListToMap(List<String> nodes) {
        BitSet nodesMap = new BitSet(nodes.size());

        for (String name : nodes) {
            int id = nodeNameToId.getOrDefault(name, -1);
            if (id != -1) {
                nodesMap.set(id);
            }
        }

        return nodesMap;
    }
}
