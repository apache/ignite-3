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

package org.apache.ignite.internal.sql.engine.metadata;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.sql.engine.prepare.MappingQueryContext;
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
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.util.CollectionUtils;

/**
 * Fragment mapping calculation.
 */
public class IgniteFragmentMapping implements IgniteRelVisitor<FragmentMapping> {

    private final RelMetadataQuery mq;

    private final MappingQueryContext ctx;

    private final Map<Integer, ColocationGroup> colocationGroups;

    /**
     * Fragment info calculation entry point.
     *
     * @param mq  Metadata query instance. Used to request appropriate metadata from node children.
     * @param ctx context.
     * @param colocationGroups resolved colocation groups.
     */
    public IgniteFragmentMapping(RelMetadataQuery mq, MappingQueryContext ctx, Map<Integer, ColocationGroup> colocationGroups) {
        assert mq instanceof RelMetadataQueryEx;

        this.mq = mq;
        this.ctx = ctx;
        this.colocationGroups = colocationGroups;
    }

    /**
     * Computes fragment mapping for the given rel node.
     *
     * @param rel Root node of a calculated fragment.
     *
     * @return Fragment meta information.
     */
    public FragmentMapping computeMapping(IgniteRel rel) {
        return rel.accept(this);
    }

    @Override
    public FragmentMapping visit(IgniteSender rel) {
        return mapSingleRel(rel);
    }

    @Override
    public FragmentMapping visit(IgniteFilter rel) {
        return mapFilter(rel);
    }

    @Override
    public FragmentMapping visit(IgniteTrimExchange rel) {
        return mapTrimExchange(rel);
    }

    @Override
    public FragmentMapping visit(IgniteProject rel) {
        return mapSingleRel(rel);
    }

    @Override
    public FragmentMapping visit(IgniteNestedLoopJoin rel) {
        return mapBiRel(rel);
    }

    @Override
    public FragmentMapping visit(IgniteCorrelatedNestedLoopJoin rel) {
        return mapBiRel(rel);
    }

    @Override
    public FragmentMapping visit(IgniteMergeJoin rel) {
        return mapBiRel(rel);
    }

    @Override
    public FragmentMapping visit(IgniteIndexScan rel) {
        return mapIndexScan(rel);
    }

    @Override
    public FragmentMapping visit(IgniteTableScan rel) {
        return mapTableScan(rel);
    }

    @Override
    public FragmentMapping visit(IgniteSystemViewScan rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FragmentMapping visit(IgniteReceiver rel) {
        return mapReceiver(rel);
    }

    @Override
    public FragmentMapping visit(IgniteExchange rel) {
        throw new AssertionError("Unexpected call: " + rel);
    }

    @Override
    public FragmentMapping visit(IgniteColocatedHashAggregate rel) {
        return mapSingleRel(rel);
    }

    @Override
    public FragmentMapping visit(IgniteMapHashAggregate rel) {
        return mapSingleRel(rel);
    }

    @Override
    public FragmentMapping visit(IgniteReduceHashAggregate rel) {
        return mapSingleRel(rel);
    }

    @Override
    public FragmentMapping visit(IgniteColocatedSortAggregate rel) {
        return mapSingleRel(rel);
    }

    @Override
    public FragmentMapping visit(IgniteMapSortAggregate rel) {
        return mapSingleRel(rel);
    }

    @Override
    public FragmentMapping visit(IgniteReduceSortAggregate rel) {
        return mapSingleRel(rel);
    }

    @Override
    public FragmentMapping visit(IgniteTableModify rel) {
        return mapTableModify(rel);
    }

    @Override
    public FragmentMapping visit(IgniteValues rel) {
        return mapValues(rel, ctx);
    }

    @Override
    public FragmentMapping visit(IgniteUnionAll rel) {
        return mapSetOp(rel);
    }

    @Override
    public FragmentMapping visit(IgniteSort rel) {
        return mapSingleRel(rel);
    }

    @Override
    public FragmentMapping visit(IgniteTableSpool rel) {
        return mapSingleRel(rel);
    }

    @Override
    public FragmentMapping visit(IgniteSortedIndexSpool rel) {
        return mapSingleRel(rel);
    }

    @Override
    public FragmentMapping visit(IgniteLimit rel) {
        return mapSingleRel(rel);
    }

    @Override
    public FragmentMapping visit(IgniteHashIndexSpool rel) {
        return mapSingleRel(rel);
    }

    @Override
    public FragmentMapping visit(IgniteSetOp rel) {
        assert rel instanceof SetOp;
        SetOp rel0 = (SetOp) rel;
        return mapSetOp(rel0);
    }

    @Override
    public FragmentMapping visit(IgniteTableFunctionScan rel) {
        return mapTableFunction(rel);
    }

    @Override
    public FragmentMapping visit(IgniteRel rel) {
        throw new AssertionError("Unexpected call: " + rel);
    }

    private FragmentMapping doComputeMapping(RelNode relNode) {
        IgniteRel igniteRel = (IgniteRel) relNode;
        return igniteRel.accept(this);
    }

    /**
     * Requests meta information about nodes capable to execute a query over particular partitions.
     *
     * @param rel Relational node.
     * @return Nodes mapping, representing a list of nodes capable to execute a query over particular partitions.
     */
    private FragmentMapping mapSingleRel(SingleRel rel) {
        return doComputeMapping(rel.getInput());
    }

    /**
     * See {@link IgniteFragmentMapping#mapSingleRel(SingleRel)}.
     *
     * <p>{@link ColocationMappingException} may be thrown on two children nodes locations merge. This means that the fragment
     * (which part the parent node is) cannot be executed on any node and additional exchange is needed. This case we throw {@link
     * NodeMappingException} with an edge, where we need the additional exchange. After the exchange is put into the fragment and the
     * fragment is split into two ones, fragment meta information will be recalculated for all fragments.
     */
    private FragmentMapping mapBiRel(BiRel rel) {
        RelNode left = rel.getLeft();
        RelNode right = rel.getRight();

        FragmentMapping frgLeft = doComputeMapping(left);
        FragmentMapping frgRight = doComputeMapping(right);

        try {
            return frgLeft.colocate(frgRight);
        } catch (ColocationMappingException e) {
            IgniteExchange leftExch = new IgniteExchange(rel.getCluster(), left.getTraitSet(), left, TraitUtils.distribution(left));
            IgniteExchange rightExch = new IgniteExchange(rel.getCluster(), right.getTraitSet(), right, TraitUtils.distribution(right));

            RelNode leftVar = rel.copy(rel.getTraitSet(), List.of(leftExch, right));
            RelNode rightVar = rel.copy(rel.getTraitSet(), List.of(left, rightExch));

            RelOptCost leftVarCost = mq.getCumulativeCost(leftVar);
            RelOptCost rightVarCost = mq.getCumulativeCost(rightVar);

            if (leftVarCost.isLt(rightVarCost)) {
                throw new NodeMappingException("Failed to calculate physical distribution", left, e);
            } else {
                throw new NodeMappingException("Failed to calculate physical distribution", right, e);
            }
        }
    }

    /**
     * See {@link IgniteFragmentMapping#mapSingleRel(SingleRel)}
     *
     * <p>{@link ColocationMappingException} may be thrown on two children nodes locations merge. This means that the
     * fragment (which part the parent node is) cannot be executed on any node and additional exchange is needed.
     * This case we throw {@link NodeMappingException} with an edge, where we need the additional exchange.
     * After the exchange is put into the fragment and the fragment is split into two ones, fragment meta information
     * will be recalculated for all fragments.
     */
    private FragmentMapping mapSetOp(SetOp rel) {
        FragmentMapping res = null;

        if (TraitUtils.distribution(rel) == IgniteDistributions.random()) {
            for (RelNode input : rel.getInputs()) {
                res = res == null ? doComputeMapping(input) : res.combine(
                        doComputeMapping(input));
            }
        } else {
            for (RelNode input : rel.getInputs()) {
                try {
                    res = res == null ? doComputeMapping(input) : res.colocate(
                            doComputeMapping(input));
                } catch (ColocationMappingException e) {
                    throw new NodeMappingException("Failed to calculate physical distribution", input, e);
                }
            }
        }

        return res;
    }

    /**
     * See {@link IgniteFragmentMapping#mapSingleRel(SingleRel)}.
     *
     * <p>Prunes involved partitions (hence nodes, involved in query execution) if possible.
     */
    private FragmentMapping mapFilter(IgniteFilter rel) {
        return doComputeMapping(rel.getInput()).prune(rel);
    }

    /**
     * See {@link IgniteFragmentMapping#mapSingleRel(SingleRel)}.
     *
     * <p>Prunes involved partitions (hence nodes, involved in query execution) if possible.
     */
    private FragmentMapping mapTrimExchange(IgniteTrimExchange rel) {
        try {
            FragmentMapping mapping = doComputeMapping(rel.getInput());

            return FragmentMapping.create(rel.sourceId()).colocate(mapping);
        } catch (ColocationMappingException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * See {@link IgniteFragmentMapping#mapSingleRel(SingleRel)}.
     */
    private FragmentMapping mapReceiver(IgniteReceiver rel) {
        return FragmentMapping.create(rel.exchangeId());
    }

    /**
     * See {@link IgniteFragmentMapping#mapSingleRel(SingleRel)}.
     */
    private FragmentMapping mapIndexScan(IgniteIndexScan rel) {
        return mapScan(rel.sourceId(), rel);
    }

    /**
     * See {@link IgniteFragmentMapping#mapSingleRel(SingleRel)}.
     */
    private FragmentMapping mapTableScan(IgniteTableScan rel) {
        return mapScan(rel.sourceId(), rel);
    }

    /**
     * See {@link IgniteFragmentMapping#mapSingleRel(SingleRel)}.
     */
    private FragmentMapping mapValues(IgniteValues rel, MappingQueryContext ctx) {
        ColocationGroup group = ColocationGroup.forNodes(ctx.mappingService().executionNodes(false, null));

        return FragmentMapping.create(rel.sourceId(), group);
    }

    /**
     * See {@link IgniteFragmentMapping#mapSingleRel(SingleRel)}.
     */
    private FragmentMapping mapTableModify(IgniteTableModify rel) {
        RelNode input = rel.getInput();
        FragmentMapping mapping = doComputeMapping(input);
        IgniteTable igniteTable = rel.getTable().unwrapOrThrow(IgniteTable.class);

        ColocationGroup tableColocationGroup = colocationGroups.get(igniteTable.id());
        assert tableColocationGroup != null : "No colocation group for " + igniteTable.id();

        List<NodeWithTerm> assignments = tableColocationGroup.assignments().stream()
                .map(CollectionUtils::first)
                .collect(Collectors.toList());

        FragmentMapping tableMapping = FragmentMapping.create(-1, tableColocationGroup);
        try {
            mapping = tableMapping.colocate(mapping);
        } catch (ColocationMappingException e) {
            throw new NodeMappingException("Failed to calculate physical distribution", input, e);
        }

        mapping = mapping.updatingTableAssignments(assignments);

        return mapping;
    }

    /**
     * See {@link IgniteFragmentMapping#mapSingleRel(SingleRel)}.
     */
    private FragmentMapping mapTableFunction(IgniteTableFunctionScan rel) {
        ColocationGroup group = ColocationGroup.forNodes(ctx.mappingService().executionNodes(false, null));

        return FragmentMapping.create(rel.sourceId(), group);
    }

    private FragmentMapping mapScan(long sourceId, ProjectableFilterableTableScan rel) {
        IgniteTable igniteTable = rel.getTable().unwrapOrThrow(IgniteTable.class);

        ColocationGroup group = colocationGroups.get(igniteTable.id());
        assert group != null : "No colocation group for " + igniteTable.id();

        return FragmentMapping.create(sourceId, group);
    }
}
