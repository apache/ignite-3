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

package org.apache.ignite.internal.sql.engine.exec.mapping.bigcluster;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
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
    /** Participating nodes. */
    final BitSet nodes;

    AbstractTarget(BitSet nodes) {
        this.nodes = nodes;

        assert !nodes.isEmpty();
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
                break;
            }
        }

        return result;
    }

    Int2ObjectMap<NodeWithConsistencyToken> assignments(List<String> nodeNames) {
        if (!(this instanceof PartitionedTarget)) {
            return Int2ObjectMaps.emptyMap();
        }

        PartitionedTarget partitionedTarget = (PartitionedTarget) this;

        int partitionsNodes = partitionedTarget.partitionsNodes.length;

        Int2ObjectMap<NodeWithConsistencyToken> result = new Int2ObjectOpenHashMap<>(partitionsNodes);

        for (int partNo = 0; partNo < partitionsNodes; partNo++) {
            int partitionNode = partitionedTarget.partitionsNodes[partNo];

            result.put(partNo, new NodeWithConsistencyToken(
                    nodeNames.get(partitionNode),
                    partitionedTarget.enlistmentConsistencyTokens[partNo]
            ));
        }

        return result;
    }

    abstract ExecutionTarget colocate(AllOfTarget other) throws ColocationMappingException;

    abstract ExecutionTarget colocate(OneOfTarget other) throws ColocationMappingException;

    abstract ExecutionTarget colocate(PartitionedTarget other) throws ColocationMappingException;

    abstract ExecutionTarget colocate(SomeOfTarget other) throws ColocationMappingException;

    static ExecutionTarget colocate(AllOfTarget allOf, AllOfTarget otherAllOf) throws ColocationMappingException {
        if (!allOf.nodes.equals(otherAllOf.nodes)) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        return allOf;
    }

    static ExecutionTarget colocate(AllOfTarget allOf, OneOfTarget oneOf) throws ColocationMappingException {
        if (allOf.nodes.cardinality() == 1 && oneOf.nodes.get(allOf.nodes.nextSetBit(0))) {
            return allOf;
        }

        throw new ColocationMappingException("Targets are not colocated");
    }

    static ExecutionTarget colocate(AllOfTarget allOf, PartitionedTarget partitioned) throws ColocationMappingException {
        throw new ColocationMappingException("AllOf target and Partitioned can't be colocated");
    }

    static ExecutionTarget colocate(AllOfTarget allOf, SomeOfTarget someOf) throws ColocationMappingException {
        BitSet newNodes = (BitSet) allOf.nodes.clone();
        newNodes.and(someOf.nodes);

        if (newNodes.cardinality() != allOf.nodes.cardinality()) {
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
        if (!oneOf.nodes.intersects(partitioned.nodes) || partitioned.nodes.cardinality() != 1) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        return partitioned;
    }

    static ExecutionTarget colocate(OneOfTarget oneOf, SomeOfTarget someOf) throws ColocationMappingException {
        BitSet newNodes = (BitSet) oneOf.nodes.clone();
        newNodes.and(someOf.nodes);

        if (newNodes.isEmpty()) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        return new OneOfTarget(newNodes);
    }

    static ExecutionTarget colocate(PartitionedTarget partitioned, PartitionedTarget otherPartitioned) throws ColocationMappingException {
        if (partitioned.partitionsNodes.length != otherPartitioned.partitionsNodes.length) {
            throw new ColocationMappingException("Partitioned targets with mot matching numbers of partitioned are not colocated");
        }

        if (!Arrays.equals(partitioned.partitionsNodes, otherPartitioned.partitionsNodes)) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        return partitioned;
    }

    static ExecutionTarget colocate(PartitionedTarget partitioned, SomeOfTarget someOf) throws ColocationMappingException {
        for (int partNo = 0; partNo < partitioned.partitionsNodes.length; partNo++) {
            boolean contain = someOf.nodes.get(partitioned.partitionsNodes[partNo]);

            if (!contain) {
                throw new ColocationMappingException("Targets are not colocated");
            }
        }

        return partitioned;
    }

    static ExecutionTarget colocate(SomeOfTarget someOf, SomeOfTarget otherSomeOf) throws ColocationMappingException {
        BitSet newNodes = (BitSet) someOf.nodes.clone();
        newNodes.and(otherSomeOf.nodes);

        if (newNodes.isEmpty()) {
            throw new ColocationMappingException("Targets are not colocated");
        }

        return new SomeOfTarget(newNodes);
    }
}
