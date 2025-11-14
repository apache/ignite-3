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

import static org.apache.ignite.internal.util.IgniteUtils.isPow2;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.sql.engine.exec.NodeWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationMappingException;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTarget;

/**
 * Abstract nodes that implements colocation strategies for every pair of targets.
 *
 * <p>Children of this class are primary used for dispatching an execution to a corresponding
 * colocation method.
 */
abstract class AbstractTarget implements ExecutionTarget {
    final long nodes;

    AbstractTarget(long nodes) {
        if (nodes <= 0) {
            throw new IllegalArgumentException("Empty target is not allowed [nodes=" + nodes + ']');
        }

        this.nodes = nodes;
    }

    List<String> nodes(List<String> nodeNames) {
        if (isPow2(nodes)) {
            int idx = Long.numberOfTrailingZeros(nodes);

            return List.of(nodeNames.get(idx));
        }

        int count = Long.bitCount(nodes);
        List<String> result = new ArrayList<>(count);

        for (int bit = 1, idx = 0; bit <= nodes; bit <<= 1, idx++) {
            if ((nodes & bit) != 0) {
                result.add(nodeNames.get(idx));
            }
        }

        return result;
    }

    Int2ObjectMap<NodeWithConsistencyToken> assignments(List<String> nodeNames) {
        if (!(this instanceof PartitionedTarget)) {
            return Int2ObjectMaps.emptyMap();
        }

        PartitionedTarget partitionedTarget = (PartitionedTarget) this;

        Int2ObjectMap<NodeWithConsistencyToken> result = new Int2ObjectOpenHashMap<>(partitionedTarget.partitionsNodes.length);

        for (int partNo = 0; partNo < partitionedTarget.partitionsNodes.length; partNo++) {
            long partitionNodes = partitionedTarget.partitionsNodes[partNo];

            assert isPow2(partitionNodes);

            int idx = Long.numberOfTrailingZeros(partitionNodes);

            result.put(partNo, new NodeWithConsistencyToken(
                    nodeNames.get(idx),
                    partitionedTarget.enlistmentConsistencyTokens[partNo]
            ));
        }

        return result;
    }

    /**
     * Finalises target by choosing exactly one node for targets with multiple options.
     *
     * <p>Some targets may have several options, so we have to pick one in order to get
     * correct results. Call to this methods resolves this ambiguity by truncating all
     * but one option. Which exactly option will be left is implementation defined.
     *
     * @return Finalised target.
     */
    abstract ExecutionTarget finalise();

    abstract ExecutionTarget colocate(AllOfTarget other) throws ColocationMappingException;

    abstract ExecutionTarget colocate(OneOfTarget other) throws ColocationMappingException;

    abstract ExecutionTarget colocate(PartitionedTarget other) throws ColocationMappingException;

    abstract ExecutionTarget colocate(SomeOfTarget other) throws ColocationMappingException;

    static ExecutionTarget colocate(AllOfTarget allOf, AllOfTarget otherAllOf) throws ColocationMappingException {
        if (allOf.nodes != otherAllOf.nodes) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        return allOf;
    }

    static ExecutionTarget colocate(AllOfTarget allOf, OneOfTarget oneOf) throws ColocationMappingException {
        if ((allOf.nodes & oneOf.nodes) == 0) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        if (!isPow2(allOf.nodes)) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        return allOf;
    }

    static ExecutionTarget colocate(AllOfTarget allOf, PartitionedTarget partitioned) throws ColocationMappingException {
        throw new ColocationMappingException("AllOf target and Partitioned can't be colocated");
    }

    static ExecutionTarget colocate(AllOfTarget allOf, SomeOfTarget someOf) throws ColocationMappingException {
        long newNodes = allOf.nodes & someOf.nodes;

        if (allOf.nodes != newNodes) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        return allOf;
    }

    static ExecutionTarget colocate(OneOfTarget oneOf, OneOfTarget anotherOneOf) throws ColocationMappingException {
        long newNodes = oneOf.nodes & anotherOneOf.nodes;

        if (newNodes == 0) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        return new OneOfTarget(newNodes);
    }

    static ExecutionTarget colocate(OneOfTarget oneOf, PartitionedTarget partitioned) throws ColocationMappingException {
        if ((oneOf.nodes & partitioned.nodes) == 0) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        if (isPow2((partitioned.nodes))) {
            return partitioned; // All partitions on single node.
        }

        long colocatedNodes = oneOf.nodes;
        for (int partNo = 0; partNo < partitioned.partitionsNodes.length; partNo++) {
            colocatedNodes &= partitioned.partitionsNodes[partNo];

            if (colocatedNodes == 0) {
                throw new ColocationMappingException("Targets are not colocated");
            }
        }

        boolean finalised = isPow2(colocatedNodes);
        long[] newNodes = new long[partitioned.partitionsNodes.length];
        Arrays.fill(newNodes, colocatedNodes);

        return new PartitionedTarget(finalised, newNodes, partitioned.enlistmentConsistencyTokens);
    }

    static ExecutionTarget colocate(OneOfTarget oneOf, SomeOfTarget someOf) throws ColocationMappingException {
        long newNodes = oneOf.nodes & someOf.nodes;

        if (newNodes == 0) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        return new OneOfTarget(newNodes);
    }

    static ExecutionTarget colocate(PartitionedTarget partitioned, PartitionedTarget otherPartitioned) throws ColocationMappingException {
        if (partitioned.partitionsNodes.length != otherPartitioned.partitionsNodes.length) {
            throw new ColocationMappingException("Partitioned targets with not matching numbers of partitions are not colocated");
        }

        boolean finalised = true;
        long[] newPartitionsNodes = new long[partitioned.partitionsNodes.length];
        for (int partNo = 0; partNo < partitioned.partitionsNodes.length; partNo++) {
            long newNodes = partitioned.partitionsNodes[partNo] & otherPartitioned.partitionsNodes[partNo];

            if (newNodes == 0) {
                throw new ColocationMappingException("Targets are not colocated");
            }

            newPartitionsNodes[partNo] = newNodes;
            finalised = finalised && isPow2(newNodes);
        }

        if (!Arrays.equals(partitioned.enlistmentConsistencyTokens, otherPartitioned.enlistmentConsistencyTokens)) {
            throw new ColocationMappingException("Partitioned targets have different terms");
        }

        return new PartitionedTarget(finalised, newPartitionsNodes, partitioned.enlistmentConsistencyTokens);
    }

    static ExecutionTarget colocate(PartitionedTarget partitioned, SomeOfTarget someOf) throws ColocationMappingException {
        boolean finalised = true;
        long[] newPartitionsNodes = new long[partitioned.partitionsNodes.length];
        for (int partNo = 0; partNo < partitioned.partitionsNodes.length; partNo++) {
            long newNodes = partitioned.partitionsNodes[partNo] & someOf.nodes;

            if (newNodes == 0) {
                throw new ColocationMappingException("Targets are not colocated");
            }

            newPartitionsNodes[partNo] = newNodes;
            finalised = finalised && isPow2(newNodes);
        }

        return new PartitionedTarget(finalised, newPartitionsNodes, partitioned.enlistmentConsistencyTokens);
    }

    static ExecutionTarget colocate(SomeOfTarget someOf, SomeOfTarget otherSomeOf) throws ColocationMappingException {
        long newNodes = someOf.nodes & otherSomeOf.nodes;

        if (newNodes == 0) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        return new SomeOfTarget(newNodes);
    }

    static long pickOne(long nodes) {
        return Long.lowestOneBit(nodes);
    }
}
