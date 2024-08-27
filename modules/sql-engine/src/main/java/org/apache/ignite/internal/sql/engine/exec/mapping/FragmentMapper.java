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

import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.sql.engine.exec.NodeWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteFilter;
import org.apache.ignite.internal.sql.engine.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.sql.engine.rel.IgniteHashJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueGet;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteLimit;
import org.apache.ignite.internal.sql.engine.rel.IgniteMergeJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteRelVisitor;
import org.apache.ignite.internal.sql.engine.rel.IgniteSelectCount;
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
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.jetbrains.annotations.Nullable;

/**
 * A mapper that traverse a fragment tree and calculates a mapping.
 */
class FragmentMapper {
    private static final int MAPPING_ATTEMPTS = 3;

    private final RelMetadataQuery mq;

    private final MappingContext context;

    private final Int2ObjectMap<ExecutionTarget> targets;

    FragmentMapper(RelMetadataQuery mq, MappingContext context, Int2ObjectMap<ExecutionTarget> targets) {
        assert mq instanceof RelMetadataQueryEx;

        this.mq = mq;
        this.context = context;
        this.targets = targets;
    }

    private Mapping map(Fragment fragment) throws FragmentMappingException {
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

        mapping.validate();

        return mapping;
    }

    /**
     * Computes mapping for the given list of fragments.
     *
     * @param fragments Fragments to compute mapping for.
     * @param idGenerator Identity generator is used to get id for a new fragments which were split from original one in case mapper
     *         consider the fragment impossible to colocate.
     * @return Fragment meta information.
     */
    public List<FragmentMapping> map(List<Fragment> fragments, IdGenerator idGenerator) {
        Exception ex = null;
        boolean lastAttemptSucceed = false;
        Long2ObjectMap<Pair<Fragment, Mapping>> mappingByFragmentId = new Long2ObjectOpenHashMap<>();
        for (int attempt = 0; attempt < MAPPING_ATTEMPTS && !lastAttemptSucceed; attempt++) {
            Fragment currentFragment = null;
            try {
                for (Fragment fragment : fragments) {
                    currentFragment = fragment;

                    if (mappingByFragmentId.containsKey(fragment.fragmentId())) {
                        continue;
                    }

                    mappingByFragmentId.put(fragment.fragmentId(), new Pair<>(fragment, map(fragment)));
                }

                lastAttemptSucceed = true;
            } catch (FragmentMappingException mappingException) {
                if (ex == null) {
                    ex = mappingException;
                } else {
                    ex.addSuppressed(mappingException);
                }

                fragments = replace(
                        fragments,
                        currentFragment,
                        new FragmentSplitter(idGenerator, mappingException.node()).go(currentFragment)
                );
            }
        }

        if (!lastAttemptSucceed) {
            throw new IgniteInternalException(Sql.MAPPING_ERR, "Unable to map query: " + ex.getMessage(), ex);
        }

        return adjustMapping(mappingByFragmentId).stream()
                .map(pair -> new FragmentMapping(pair.getFirst(), pair.getSecond().createColocationGroups()))
                .collect(Collectors.toList());
    }

    /**
     * Attempt to colocate different fragments with each other.
     *
     * <p>Current implementation assumes that intermediate results becomes smaller on every step, thus it prioritizes colocation of children
     * over colocation of parents. This assumption is not always correct, but it will be addressed later by more sophisticated algorithm.
     *
     * @param mappingByFragmentId Fragments to map.
     * @return List of pairs of fragments to theirs mapping metadata.
     */
    private static List<Pair<Fragment, Mapping>> adjustMapping(
            Long2ObjectMap<Pair<Fragment, Mapping>> mappingByFragmentId
    ) {
        LongList fragmentIds = collectFragmentIds(mappingByFragmentId);

        /*
         Current implementation adjusts colocation in two passes. First pass is bottom-up, here we
         start from leaf nodes and adjust execution targets pairwise up to the root. Second pass is
         top-down, here we try to improve colocation of an entire plan with root fragment.
         */

        // first pass is bottom-up
        // the very first fragment is root, which don't have any parents, so let's just skip it
        for (int i = fragmentIds.size() - 1; i > 0; i--) {
            Pair<Fragment, Mapping> currentPair = mappingByFragmentId.get(fragmentIds.getLong(i));

            Mapping currentMapping = currentPair.getSecond();

            Long targetFragmentId = currentPair.getFirst().targetFragmentId();

            assert targetFragmentId != null;

            Pair<Fragment, Mapping> parentPair = mappingByFragmentId.get((long) targetFragmentId);

            Mapping parentMapping = parentPair.getSecond();

            { // to reduce visibility scope of `newCurrentMapping`
                Mapping newCurrentMapping = currentMapping.bestEffortColocate(parentMapping);
                if (newCurrentMapping != null) {
                    Fragment currentFragment = currentPair.getFirst();

                    mappingByFragmentId.put(currentFragment.fragmentId(), new Pair<>(currentFragment, newCurrentMapping));
                    currentMapping = newCurrentMapping;
                }
            }

            { // to reduce visibility scope of `newParentMapping`
                Mapping newParentMapping = parentMapping.bestEffortColocate(currentMapping);
                if (newParentMapping != null) {
                    Fragment parentFragment = parentPair.getFirst();

                    mappingByFragmentId.put(parentFragment.fragmentId(), new Pair<>(parentFragment, newParentMapping));
                }
            }
        }

        // second pass is top-down
        // root fragment has been already processed on previous pass, thus let's skip it
        for (int i = 1; i < fragmentIds.size(); i++) {
            Pair<Fragment, Mapping> currentPair = mappingByFragmentId.get(fragmentIds.getLong(i));

            Mapping currentMapping = currentPair.getSecond();

            for (IgniteReceiver receiver : currentPair.getFirst().remotes()) {
                Pair<Fragment, Mapping> childPair = mappingByFragmentId.get(receiver.sourceFragmentId());

                Mapping childMapping = childPair.getSecond();

                { // to reduce visibility scope of `newCurrentMapping`
                    Mapping newCurrentMapping = currentMapping.bestEffortColocate(childMapping);
                    if (newCurrentMapping != null) {
                        Fragment currentFragment = currentPair.getFirst();

                        mappingByFragmentId.put(currentFragment.fragmentId(), new Pair<>(currentFragment, newCurrentMapping));
                        currentMapping = newCurrentMapping;
                    }
                }

                { // to reduce visibility scope of `newChildMapping`
                    Mapping newChildMapping = childMapping.bestEffortColocate(currentMapping);
                    if (newChildMapping != null) {
                        Fragment childFragment = childPair.getFirst();

                        mappingByFragmentId.put(childFragment.fragmentId(), new Pair<>(childFragment, newChildMapping));
                    }
                }
            }
        }

        return fragmentIds.longStream()
                .mapToObj(mappingByFragmentId::get)
                .collect(Collectors.toList());
    }

    private static LongList collectFragmentIds(Long2ObjectMap<Pair<Fragment, Mapping>> mappingByFragmentId) {
        LongList fragmentIds = new LongArrayList();
        Queue<Fragment> toProcess = new LinkedList<>();

        Fragment root = findRootFragment(mappingByFragmentId.values());

        toProcess.add(root);

        while (!toProcess.isEmpty()) {
            Fragment current = toProcess.poll();

            fragmentIds.add(current.fragmentId());

            for (IgniteReceiver receiver : current.remotes()) {
                toProcess.add(mappingByFragmentId.get(receiver.sourceFragmentId()).getFirst());
            }
        }
        return fragmentIds;
    }

    private static Fragment findRootFragment(Collection<Pair<Fragment, Mapping>> fragments) {
        Fragment root = null;
        for (Pair<Fragment, Mapping> pair : fragments) {
            if (pair.getFirst().rootFragment()) {
                assert root == null;

                root = pair.getFirst();
            }
        }

        assert root != null;

        return root;
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
        public Mapping visit(IgniteHashJoin rel) {
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
            throw new AssertionError(rel.getClass());
        }

        @Override
        public Mapping visit(IgniteKeyValueGet rel) {
            throw new AssertionError(rel.getClass());
        }

        @Override
        public Mapping visit(IgniteKeyValueModify rel) {
            throw new AssertionError(rel.getClass());
        }

        @Override
        public Mapping visit(IgniteSelectCount rel) {
            throw new AssertionError(rel.getClass());
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

                assert leftVarCost != null;
                assert rightVarCost != null;

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
         * fragment (which part the parent node is) cannot be executed on any node and additional exchange is needed. This case we throw
         * {@link FragmentMappingException} with an edge, where we need the additional exchange. After the exchange is put into the fragment
         * and the fragment is split into two ones, fragment meta information will be recalculated for all fragments.
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

                assert res != null : "SetOp without inputs";

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

            IgniteDataSource igniteDataSource = rel.getTable().unwrapOrThrow(IgniteDataSource.class);

            ExecutionTarget target = targets.get(igniteDataSource.id());
            assert target != null : "No colocation group for " + igniteDataSource.id();

            try {
                return newMapping(rel.sourceId(), target).colocate(mapping);
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

        @Nullable Mapping bestEffortColocate(Mapping other);

        void validate() throws FragmentMappingException;

        List<ColocationGroup> createColocationGroups();
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
        public @Nullable Mapping bestEffortColocate(Mapping other) {
            return null;
        }

        @Override
        public void validate() throws FragmentMappingException {
            throw exception;
        }

        @Override
        public List<ColocationGroup> createColocationGroups() {
            throw new AssertionError("Should not be called");
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
        public @Nullable Mapping bestEffortColocate(Mapping other) {
            return null;
        }

        @Override
        public void validate() throws FragmentMappingException {
            for (Mapping mapping : mappings) {
                mapping.validate();
            }
        }

        @Override
        public List<ColocationGroup> createColocationGroups() {
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
        public @Nullable Mapping bestEffortColocate(Mapping other) {
            if (!other.colocated()) {
                return null;
            }

            assert other instanceof ColocatedMapping : other.getClass().getCanonicalName();

            ColocatedMapping colocatedMapping = (ColocatedMapping) other;

            ExecutionTarget newTarget = target.trimTo(colocatedMapping.target);

            if (newTarget == target) {
                return null;
            }

            return new ColocatedMapping(this.sourceIds, newTarget);
        }

        @Override
        public void validate() {
            // colocated mapping always valid
        }

        @Override
        public List<ColocationGroup> createColocationGroups() {
            List<String> nodes = context.targetFactory().resolveNodes(target);
            Int2ObjectMap<NodeWithConsistencyToken> assignments = context.targetFactory().resolveAssignments(target);

            return List.of(
                    new ColocationGroup(
                            List.copyOf(sourceIds),
                            nodes,
                            assignments
                    )
            );
        }
    }

    private static List<Fragment> replace(
            List<Fragment> originalFragments,
            Fragment fragmentToReplace,
            List<Fragment> replacement
    ) {
        assert !nullOrEmpty(replacement);

        Long2LongOpenHashMap newTargets = new Long2LongOpenHashMap();
        for (Fragment fragment0 : replacement) {
            for (IgniteReceiver remote : fragment0.remotes()) {
                newTargets.put(remote.exchangeId(), fragment0.fragmentId());
            }
        }

        List<Fragment> newFragments = new ArrayList<>(originalFragments.size() + replacement.size() - 1);
        for (Fragment fragment : originalFragments) {
            if (fragment == fragmentToReplace) {
                //noinspection AssignmentToForLoopParameter
                fragment = first(replacement);
            } else if (!fragment.rootFragment()) {
                IgniteSender sender = (IgniteSender) fragment.root();

                long newTargetId = newTargets.getOrDefault(sender.exchangeId(), Long.MIN_VALUE);

                if (newTargetId != Long.MIN_VALUE) {
                    sender = new IgniteSender(sender.getCluster(), sender.getTraitSet(),
                            sender.getInput(), sender.exchangeId(), newTargetId, sender.distribution());

                    //noinspection AssignmentToForLoopParameter
                    fragment = new Fragment(fragment.fragmentId(), fragment.correlated(), sender,
                            fragment.remotes(), fragment.tables(), fragment.systemViews());
                }
            }

            newFragments.add(fragment);
        }

        newFragments.addAll(replacement.subList(1, replacement.size()));

        return newFragments;
    }
}
