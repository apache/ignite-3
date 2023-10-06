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
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.sql.engine.exec.NodeWithTerm;
import org.apache.ignite.internal.sql.engine.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteFilter;
import org.apache.ignite.internal.sql.engine.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteLimit;
import org.apache.ignite.internal.sql.engine.rel.IgniteMergeJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteRelVisitor;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.apache.ignite.internal.sql.engine.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.sql.engine.rel.IgniteSystemViewScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableSpool;
import org.apache.ignite.internal.sql.engine.rel.IgniteTrimExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteUnionAll;
import org.apache.ignite.internal.sql.engine.rel.IgniteValues;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteSetOp;
import org.apache.ignite.internal.sql.engine.schema.IgniteDataSource;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;

/**
 * A mapper that traverse a fragment tree and calculates a mapping.
 */
class FragmentMapper {

    private final RelMetadataQuery mq;

    private final MappingContext context;

    private final Int2ObjectMap<ExecutionTarget> targets;

    FragmentMapper(RelMetadataQuery mq, MappingContext context, Int2ObjectMap<ExecutionTarget> targets) {
        assert mq instanceof RelMetadataQueryEx;

        this.mq = mq;
        this.context = context;
        this.targets = targets;
    }

    /**
     * Computes mapping for the given fragment.
     *
     * @param fragment A fragment to compute mapping for.
     * @return Fragment meta information.
     */
    public FragmentMapping map(Fragment fragment) throws FragmentMappingException {
        Mapping mapping = fragment.root().accept(new MapperVisitor());

        if (fragment.single()) {
            // if this fragment is supposed to be mapped to a single node, then let's try
            // to map it to a local node. That way simple queries like SELECT x FROM TABLE(system_range(1, 5))
            // will be executed locally. Also, root fragment must be colocated with local node, because
            // it's where cursor should be opened
            Mapping localNodeMapping = newMapping(-1, context.targetFactory().oneOf(List.of(context.localNode())));

            try {
                mapping = mapping.colocate(localNodeMapping);
            } catch (ColocationMappingException e1) {
                if (fragment.rootFragment()) {
                    throw new FragmentMappingException(e1.getMessage(), fragment.root(), e1);
                }

                // if we were unable to map it to local node, then let's try to map it to any available
                // node from current topology
                Mapping anyNodeMapping = newMapping(-1, context.targetFactory().oneOf(context.nodes()));

                try {
                    mapping = mapping.colocate(anyNodeMapping);
                } catch (ColocationMappingException e2) {
                    // we've got problem
                    throw new FragmentMappingException(e2.getMessage(), fragment.root(), e2);
                }
            }
        }

        return new FragmentMapping(mapping.createColocationGroups());
    }

    private class MapperVisitor implements IgniteRelVisitor<Mapping> {
        @Override
        public Mapping visit(IgniteSender rel) {
            return mapSingleRel(rel);
        }

        @Override
        public Mapping visit(IgniteFilter rel) {
            return mapSingleRel(rel);
        }

        @Override
        public Mapping visit(IgniteTrimExchange rel) {
            return mapTrimExchange(rel);
        }

        @Override
        public Mapping visit(IgniteProject rel) {
            return mapSingleRel(rel);
        }

        @Override
        public Mapping visit(IgniteNestedLoopJoin rel) {
            return mapBiRel(rel);
        }

        @Override
        public Mapping visit(IgniteCorrelatedNestedLoopJoin rel) {
            return mapBiRel(rel);
        }

        @Override
        public Mapping visit(IgniteMergeJoin rel) {
            return mapBiRel(rel);
        }

        @Override
        public Mapping visit(IgniteIndexScan rel) {
            return mapTableScan(rel.sourceId(), rel);
        }

        @Override
        public Mapping visit(IgniteTableScan rel) {
            return mapTableScan(rel.sourceId(), rel);
        }

        @Override
        public Mapping visit(IgniteSystemViewScan rel) {
            return mapTableScan(rel.sourceId(), rel);
        }

        @Override
        public Mapping visit(IgniteReceiver rel) {
            return mapComputableSource(rel.exchangeId());
        }

        @Override
        public Mapping visit(IgniteExchange rel) {
            throw new AssertionError("Unexpected call: " + rel);
        }

        @Override
        public Mapping visit(IgniteColocatedHashAggregate rel) {
            return mapSingleRel(rel);
        }

        @Override
        public Mapping visit(IgniteMapHashAggregate rel) {
            return mapSingleRel(rel);
        }

        @Override
        public Mapping visit(IgniteReduceHashAggregate rel) {
            return mapSingleRel(rel);
        }

        @Override
        public Mapping visit(IgniteColocatedSortAggregate rel) {
            return mapSingleRel(rel);
        }

        @Override
        public Mapping visit(IgniteMapSortAggregate rel) {
            return mapSingleRel(rel);
        }

        @Override
        public Mapping visit(IgniteReduceSortAggregate rel) {
            return mapSingleRel(rel);
        }

        @Override
        public Mapping visit(IgniteTableModify rel) {
            return mapTableModify(rel);
        }

        @Override
        public Mapping visit(IgniteValues rel) {
            return mapComputableSource(rel.sourceId());
        }

        @Override
        public Mapping visit(IgniteUnionAll rel) {
            return mapSetOp(rel);
        }

        @Override
        public Mapping visit(IgniteSort rel) {
            return mapSingleRel(rel);
        }

        @Override
        public Mapping visit(IgniteTableSpool rel) {
            return mapSingleRel(rel);
        }

        @Override
        public Mapping visit(IgniteSortedIndexSpool rel) {
            return mapSingleRel(rel);
        }

        @Override
        public Mapping visit(IgniteLimit rel) {
            return mapSingleRel(rel);
        }

        @Override
        public Mapping visit(IgniteHashIndexSpool rel) {
            return mapSingleRel(rel);
        }

        @Override
        public Mapping visit(IgniteSetOp rel) {
            assert rel instanceof SetOp;

            SetOp rel0 = (SetOp) rel;
            return mapSetOp(rel0);
        }

        @Override
        public Mapping visit(IgniteTableFunctionScan rel) {
            return mapComputableSource(rel.sourceId());
        }

        @Override
        public Mapping visit(IgniteRel rel) {
            throw new AssertionError("Unexpected call: " + rel);
        }

        private Mapping computeMapping(RelNode relNode) {
            IgniteRel igniteRel = (IgniteRel) relNode;

            return igniteRel.accept(this);
        }

        /**
         * Requests meta information about nodes capable to execute a query over particular partitions.
         *
         * @param rel Relational node.
         * @return Nodes mapping, representing a list of nodes capable to execute a query over particular partitions.
         */
        private Mapping mapSingleRel(SingleRel rel) {
            return computeMapping(rel.getInput());
        }

        /**
         * {@link ColocationMappingException} may be thrown on two children nodes locations merge. This means that the fragment (which part
         * the parent node is) cannot be executed on any node and additional exchange is needed. This case we throw
         * {@link FragmentMappingException} with an edge, where we need the additional exchange. After the exchange is put into the fragment
         * and the fragment is split into two ones, fragment meta information will be recalculated for all fragments.
         */
        private Mapping mapBiRel(BiRel rel) {
            RelNode left = rel.getLeft();
            RelNode right = rel.getRight();

            Mapping lhsMapping = computeMapping(left);

            if (lhsMapping.failed()) {
                // just bubble up failed mapping, we will deal with it on top of the tree
                return lhsMapping;
            }

            Mapping rhsMapping = computeMapping(right);

            try {
                return lhsMapping.colocate(rhsMapping);
            } catch (ColocationMappingException e) {
                IgniteExchange leftExch = new IgniteExchange(rel.getCluster(), left.getTraitSet(), left, TraitUtils.distribution(left));
                IgniteExchange rightExch = new IgniteExchange(rel.getCluster(), right.getTraitSet(), right, TraitUtils.distribution(right));

                RelNode leftVar = rel.copy(rel.getTraitSet(), List.of(leftExch, right));
                RelNode rightVar = rel.copy(rel.getTraitSet(), List.of(left, rightExch));

                RelOptCost leftVarCost = mq.getCumulativeCost(leftVar);
                RelOptCost rightVarCost = mq.getCumulativeCost(rightVar);

                if (leftVarCost.isLt(rightVarCost)) {
                    return new FailedMapping(new FragmentMappingException(e.getMessage(), left, e));
                } else {
                    return new FailedMapping(new FragmentMappingException(e.getMessage(), right, e));
                }
            }
        }

        /**
         * See {@link MapperVisitor#mapSingleRel(SingleRel)}
         *
         * <p>{@link ColocationMappingException} may be thrown on two children nodes locations merge. This means that the
         * fragment (which part the parent node is) cannot be executed on any node and additional exchange is needed.
         * This case we throw {@link FragmentMappingException} with an edge, where we need the additional exchange.
         * After the exchange is put into the fragment and the fragment is split into two ones, fragment meta information
         * will be recalculated for all fragments.
         */
        private Mapping mapSetOp(SetOp rel) {
            if (TraitUtils.distribution(rel) == IgniteDistributions.random()) {
                List<Mapping> mappings = new ArrayList<>(rel.getInputs().size());
                for (RelNode input : rel.getInputs()) {
                    Mapping mapping = computeMapping(input);

                    if (mapping.failed()) {
                        // just bubble up failed mapping, we will deal with it on top of the tree
                        return mapping;
                    }

                    mappings.add(mapping);
                }

                return combineMappings(mappings);
            } else {
                Mapping res = null;

                for (RelNode input : rel.getInputs()) {
                    try {
                        res = res == null ? computeMapping(input) : res.colocate(computeMapping(input));

                        if (res.failed()) {
                            // just bubble up failed mapping, we will deal with it on top of the tree
                            return res;
                        }
                    } catch (ColocationMappingException e) {
                        return new FailedMapping(new FragmentMappingException(e.getMessage(), input, e));
                    }
                }

                return res;
            }
        }

        private Mapping mapTrimExchange(IgniteTrimExchange rel) {
            RelNode input = rel.getInput();

            Mapping mapping = computeMapping(input);

            try {
                return mapComputableSource(rel.sourceId()).colocate(mapping);
            } catch (ColocationMappingException e) {
                return new FailedMapping(new FragmentMappingException(e.getMessage(), input, e));
            }
        }

        private Mapping mapTableModify(IgniteTableModify rel) {
            RelNode input = rel.getInput();
            Mapping mapping = computeMapping(input);

            if (mapping.failed()) {
                // just bubble up failed mapping, we will deal with it on top of the tree
                return mapping;
            }

            IgniteTable igniteTable = rel.getTable().unwrapOrThrow(IgniteTable.class);

            ExecutionTarget target = targets.get(igniteTable.id());
            assert target != null : "No colocation group for " + igniteTable.id();

            try {
                return newMapping(-1, target).colocate(mapping);
            } catch (ColocationMappingException e) {
                return new FailedMapping(new FragmentMappingException(e.getMessage(), input, e));
            }
        }

        private Mapping mapComputableSource(long sourceId) {
            ExecutionTarget target = context.targetFactory().someOf(context.nodes());

            return newMapping(sourceId, target);
        }

        private Mapping mapTableScan(long sourceId, ProjectableFilterableTableScan rel) {
            IgniteDataSource igniteDataSource = rel.getTable().unwrapOrThrow(IgniteDataSource.class);

            ExecutionTarget target = targets.get(igniteDataSource.id());
            assert target != null : "No colocation group for " + igniteDataSource.id();

            return newMapping(sourceId, target);
        }
    }

    private Mapping newMapping(long sourceId, ExecutionTarget target) {
        return new ColocatedMapping(LongSets.singleton(sourceId), target);
    }

    private static Mapping combineMappings(List<Mapping> mappings) {
        return new CombinedMapping(mappings);
    }

    private interface Mapping {
        boolean failed();

        boolean colocated();

        Mapping colocate(Mapping other) throws ColocationMappingException;

        List<ColocationGroup> createColocationGroups() throws FragmentMappingException;
    }

    private static class FailedMapping implements Mapping {
        private final FragmentMappingException exception;

        FailedMapping(FragmentMappingException exception) {
            this.exception = exception;
        }

        @Override
        public boolean failed() {
            return true;
        }

        @Override
        public boolean colocated() {
            return true;
        }

        @Override
        public Mapping colocate(Mapping other) {
            return this;
        }

        @Override
        public List<ColocationGroup> createColocationGroups() throws FragmentMappingException {
            throw exception;
        }
    }

    private static class CombinedMapping implements Mapping {
        private final List<Mapping> mappings;

        CombinedMapping(List<Mapping> mappings) {
            this.mappings = mappings;
        }

        @Override
        public boolean failed() {
            return false;
        }

        @Override
        public boolean colocated() {
            return false;
        }

        @Override
        public Mapping colocate(Mapping other) throws ColocationMappingException {
            throw new ColocationMappingException("Combined mapping can't be colocated");
        }

        @Override
        public List<ColocationGroup> createColocationGroups() throws FragmentMappingException {
            List<ColocationGroup> groups = new ArrayList<>();

            for (Mapping mapping : mappings) {
                groups.addAll(mapping.createColocationGroups());
            }

            return groups;
        }
    }

    private class ColocatedMapping implements Mapping {
        private final LongSet sourceIds;
        private final ExecutionTarget target;

        ColocatedMapping(LongSet sourceIds, ExecutionTarget target) {
            this.sourceIds = sourceIds;
            this.target = target;
        }

        @Override
        public boolean failed() {
            return false;
        }

        @Override
        public boolean colocated() {
            return true;
        }

        @Override
        public Mapping colocate(Mapping other) throws ColocationMappingException {
            if (!other.colocated()) {
                throw new ColocationMappingException("Non colocated mapping can't be colocated");
            }

            assert other instanceof ColocatedMapping : other.getClass().getCanonicalName();

            ColocatedMapping colocatedMapping = (ColocatedMapping) other;

            ExecutionTarget colocatedTarget = target.colocateWith(colocatedMapping.target);

            LongSet sourceIds = new LongOpenHashSet(this.sourceIds);
            sourceIds.addAll(colocatedMapping.sourceIds);

            return new ColocatedMapping(LongSets.unmodifiable(sourceIds), colocatedTarget);
        }

        @Override
        public List<ColocationGroup> createColocationGroups() {
            ExecutionTarget finalised = target.finalise();

            List<String> nodes = context.targetFactory().resolveNodes(finalised);
            List<NodeWithTerm> assignments = context.targetFactory().resolveAssignments(finalised);

            return List.of(
                    new ColocationGroup(
                            List.copyOf(sourceIds),
                            nodes,
                            assignments
                    )
            );
        }
    }
}
