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

package org.apache.ignite.internal.sql.engine.exec.mapping.smallcluster;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import java.util.List;
import org.apache.ignite.internal.sql.engine.exec.NodeWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTarget;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTargetFactory;

/**
 * A factory that able to create targets for cluster with up to 64 nodes.
 */
public class SmallClusterFactory implements ExecutionTargetFactory {
    private final List<String> nodes;
    private final Object2LongMap<String> nodeNameToId;

    /** Constructor. */
    public SmallClusterFactory(List<String> nodes) {
        if (nodes.size() > 64) {
            throw new IllegalArgumentException("Supported up to 64 nodes, but was " + nodes.size());
        }

        this.nodes = nodes;

        nodeNameToId = new Object2LongOpenHashMap<>(nodes.size());

        int idx = 0;
        for (String name : nodes) {
            nodeNameToId.putIfAbsent(name, 1L << idx++);
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
        long[] partitionNodes = new long[nodes.size()];
        long[] enlistmentConsistencyTokens = new long[nodes.size()];

        int idx = 0;
        for (NodeWithConsistencyToken e : nodes) {
            partitionNodes[idx] = nodeNameToId.getOrDefault(e.name(), 0);
            enlistmentConsistencyTokens[idx++] = e.enlistmentConsistencyToken();
        }

        return new PartitionedTarget(true, partitionNodes, enlistmentConsistencyTokens);
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

    private long nodeListToMap(List<String> nodes) {
        long nodesMap = 0;

        for (String name : nodes) {
            nodesMap |= nodeNameToId.getOrDefault(name, 0);
        }

        return nodesMap;
    }
}
