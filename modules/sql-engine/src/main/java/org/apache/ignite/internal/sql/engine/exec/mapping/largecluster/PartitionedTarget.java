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

import it.unimi.dsi.fastutil.ints.Int2ObjectFunction;
import java.util.BitSet;
import java.util.List;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationMappingException;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTarget;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTargetFactory;

/**
 * Represents a target that consists of a number of primary partitions.
 *
 * <p>See javadoc of {@link ExecutionTargetFactory#partitioned(List)} for details.
 */
class PartitionedTarget extends AbstractTarget {
    private final boolean finalised;
    final BitSet[] partitionsNodes;
    final long[] enlistmentConsistencyTokens;

    PartitionedTarget(boolean finalised, BitSet[] partitionsNodes, long[] enlistmentConsistencyTokens) {
        super(computeNodes(partitionsNodes));

        this.finalised = finalised;
        this.partitionsNodes = partitionsNodes;
        this.enlistmentConsistencyTokens = enlistmentConsistencyTokens;
    }

    @Override
    public ExecutionTarget finalise() {
        if (finalised) {
            return this;
        }

        BitSet[] newPartitionsNodes = new BitSet[partitionsNodes.length];

        for (int partNo = 0; partNo < partitionsNodes.length; partNo++) {
            newPartitionsNodes[partNo] = pickOne(partitionsNodes[partNo]);
        }

        return new PartitionedTarget(true, newPartitionsNodes, enlistmentConsistencyTokens);
    }

    @Override
    public ExecutionTarget colocateWith(ExecutionTarget other) throws ColocationMappingException {
        assert other instanceof AbstractTarget : other == null ? "<null>" : other.getClass().getCanonicalName();

        return ((AbstractTarget) other).colocate(this);
    }

    @Override
    public ExecutionTarget trimTo(ExecutionTarget other) {
        assert other instanceof AbstractTarget : other == null ? "<null>" : other.getClass().getCanonicalName();

        if (finalised) {
            return this;
        }

        BitSet[] newPartitionsNodes = new BitSet[partitionsNodes.length];
        boolean changed = false;
        Int2ObjectFunction<BitSet> partitionNodesResolver = partitionNodeResolver(other);

        for (int i = 0; i < partitionsNodes.length; i++) {
            BitSet newNodes = partitionsNodes[i];
            BitSet otherNodes = partitionNodesResolver.get(i);

            if (!newNodes.equals(otherNodes) && newNodes.intersects(otherNodes)) {
                newNodes = (BitSet) newNodes.clone();
                newNodes.and(otherNodes);

                changed = true;
            }

            newPartitionsNodes[i] = newNodes;
        }

        if (changed) {
            return new PartitionedTarget(false, newPartitionsNodes, enlistmentConsistencyTokens);
        }

        return this;
    }

    private Int2ObjectFunction<BitSet> partitionNodeResolver(ExecutionTarget other) {
        Int2ObjectFunction<BitSet> partitionNodesResolver;

        if (other instanceof PartitionedTarget
                && ((PartitionedTarget) other).partitionsNodes.length == partitionsNodes.length) {
            PartitionedTarget otherPartitioned = (PartitionedTarget) other;

            partitionNodesResolver = partId -> otherPartitioned.partitionsNodes[partId];
        } else {
            BitSet otherNodes = ((AbstractTarget) other).nodes;

            partitionNodesResolver = partId -> otherNodes;
        }

        return partitionNodesResolver;
    }

    @Override
    ExecutionTarget colocate(AllOfTarget other) throws ColocationMappingException {
        return colocate(other, this);
    }

    @Override
    ExecutionTarget colocate(OneOfTarget other) throws ColocationMappingException {
        return colocate(other, this);
    }

    @Override
    ExecutionTarget colocate(PartitionedTarget other) throws ColocationMappingException {
        return colocate(this, other);
    }

    @Override
    ExecutionTarget colocate(SomeOfTarget other) throws ColocationMappingException {
        return colocate(this, other);
    }

    private static BitSet computeNodes(BitSet[] partitionsNodes) {
        BitSet nodes = new BitSet();
        for (BitSet nodesOfPartition : partitionsNodes) {
            nodes.or(nodesOfPartition);
        }

        return nodes;
    }
}
