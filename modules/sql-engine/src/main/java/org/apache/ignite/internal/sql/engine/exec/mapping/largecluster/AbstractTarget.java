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
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import org.apache.calcite.util.BitSets;
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
    final BitSet nodes;

    AbstractTarget(BitSet nodes) {
        assert !nodes.isEmpty();
        
        this.nodes = nodes;
    }

    List<String> nodes(List<String> nodeNames) {
        int cardinality = nodes.cardinality();

        if (cardinality == 1) {
            int idx = nodes.nextSetBit(0);

            return List.of(nodeNames.get(idx));
        }

        List<String> result = new ArrayList<>(cardinality);

        for (int idx = nodes.nextSetBit(0); idx >= 0; idx = nodes.nextSetBit(idx + 1)) {
            result.add(nodeNames.get(idx));

            if (idx == Integer.MAX_VALUE) {
                break;  // or (i+1) would overflow
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
            BitSet partitionNodes = partitionedTarget.partitionsNodes[partNo];

            assert partitionNodes.cardinality() == 1;

            int idx = partitionNodes.nextSetBit(0);

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
        if (!allOf.nodes.equals(otherAllOf.nodes) || otherAllOf.nodes.cardinality() == 0) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        return allOf;
    }

    static ExecutionTarget colocate(AllOfTarget allOf, OneOfTarget oneOf) throws ColocationMappingException {
        int target = allOf.nodes.nextSetBit(0);

        // When colocated, AllOfTarget must contains a single node that matches one of OneOfTarget nodes.
        if (target == -1 || allOf.nodes.nextSetBit(target + 1) != -1 || !oneOf.nodes.get(target)) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        return allOf;
    }

    static ExecutionTarget colocate(AllOfTarget allOf, PartitionedTarget partitioned) throws ColocationMappingException {
        throw new ColocationMappingException("AllOf target and Partitioned can't be colocated");
    }

    static ExecutionTarget colocate(AllOfTarget allOf, SomeOfTarget someOf) throws ColocationMappingException {
        if (!BitSets.contains(someOf.nodes, allOf.nodes) || allOf.nodes.isEmpty()) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        return allOf;
    }

    static ExecutionTarget colocate(OneOfTarget oneOf, OneOfTarget anotherOneOf) throws ColocationMappingException {
        BitSet newNodes = (BitSet) oneOf.nodes.clone();
        newNodes.and(anotherOneOf.nodes);

        if (newNodes.isEmpty()) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        return new OneOfTarget(newNodes);
    }

    static ExecutionTarget colocate(OneOfTarget oneOf, PartitionedTarget partitioned) throws ColocationMappingException {
        int target = partitioned.nodes.nextSetBit(0);

        if (target == -1 || partitioned.nodes.nextSetBit(target + 1) != -1) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        if (!oneOf.nodes.get(target)) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        // When colocated, PartitionedTarget must contains a single node that matches one of OneOfTarget nodes.
        assert partitioned.nodes.cardinality() == 1;
        assert partitioned.nodes.intersects(oneOf.nodes);

        return partitioned;
    }

    static ExecutionTarget colocate(OneOfTarget oneOf, SomeOfTarget someOf) throws ColocationMappingException {
        if (!oneOf.nodes.intersects(someOf.nodes)) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        BitSet newNodes = (BitSet) oneOf.nodes.clone();
        newNodes.and(someOf.nodes);

        return new OneOfTarget(newNodes);
    }

    static ExecutionTarget colocate(PartitionedTarget partitioned, PartitionedTarget otherPartitioned) throws ColocationMappingException {
        if (partitioned.partitionsNodes.length != otherPartitioned.partitionsNodes.length) {
            throw new ColocationMappingException("Partitioned targets with mot matching numbers of partitioned are not colocated");
        }

        boolean finalised = true;
        BitSet[] newPartitionsNodes = new BitSet[partitioned.partitionsNodes.length];
        for (int partNo = 0; partNo < partitioned.partitionsNodes.length; partNo++) {
            if (!partitioned.partitionsNodes[partNo].intersects(otherPartitioned.partitionsNodes[partNo])) {
                throw new ColocationMappingException("Targets are not colocated");
            }

            if (partitioned.enlistmentConsistencyTokens[partNo] != otherPartitioned.enlistmentConsistencyTokens[partNo]) {
                throw new ColocationMappingException("Partitioned targets have different terms");
            }

            BitSet newNodes = BitSet.valueOf(partitioned.partitionsNodes[partNo].toLongArray());
            newNodes.and(otherPartitioned.nodes);

            newPartitionsNodes[partNo] = newNodes;
            finalised = finalised && newNodes.cardinality() == 1;
        }

        return new PartitionedTarget(finalised, newPartitionsNodes, partitioned.enlistmentConsistencyTokens);
    }

    static ExecutionTarget colocate(PartitionedTarget partitioned, SomeOfTarget someOf) throws ColocationMappingException {
        boolean finalised = true;
        BitSet[] newPartitionsNodes = new BitSet[partitioned.partitionsNodes.length];
        for (int partNo = 0; partNo < partitioned.partitionsNodes.length; partNo++) {
            if (!partitioned.partitionsNodes[partNo].intersects(someOf.nodes)) {
                throw new ColocationMappingException("Targets are not colocated");
            }

            BitSet newNodes = BitSet.valueOf(partitioned.partitionsNodes[partNo].toLongArray());
            newNodes.and(someOf.nodes);

            newPartitionsNodes[partNo] = newNodes;
            finalised = finalised && newNodes.cardinality() == 1;
        }

        return new PartitionedTarget(finalised, newPartitionsNodes, partitioned.enlistmentConsistencyTokens);
    }

    static ExecutionTarget colocate(SomeOfTarget someOf, SomeOfTarget otherSomeOf) throws ColocationMappingException {
        if (!someOf.nodes.intersects(otherSomeOf.nodes)) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        BitSet newNodes = BitSet.valueOf(someOf.nodes.toLongArray());
        newNodes.and(otherSomeOf.nodes);

        return new SomeOfTarget(newNodes);
    }

    static BitSet pickOne(BitSet nodes) {
        int node = nodes.nextSetBit(0);

        return node == -1 ? BitSets.of() : BitSets.of(node);
    }
}
