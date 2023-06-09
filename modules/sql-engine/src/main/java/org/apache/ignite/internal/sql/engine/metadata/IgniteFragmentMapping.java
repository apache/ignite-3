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
public class IgniteFragmentMapping {
    /**
     * Fragment info calculation entry point.
     *
     * @param rel Root node of a calculated fragment.
     * @param mq  Metadata query instance.
     * @return Fragment meta information.
     */
    public static FragmentMapping fragmentMappingForMetadataQuery(RelNode rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        assert mq instanceof RelMetadataQueryEx;

        IgniteRelVisitor<FragmentMapping> visitor = new IgniteRelVisitor<>() {
            @Override
            public FragmentMapping visit(IgniteSender rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteFilter rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteTrimExchange rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteProject rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteNestedLoopJoin rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteCorrelatedNestedLoopJoin rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteMergeJoin rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteIndexScan rel) {
                return fragmentMapping(rel, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteTableScan rel) {
                return fragmentMapping(rel, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteReceiver rel) {
                return fragmentMapping(rel);
            }

            @Override
            public FragmentMapping visit(IgniteExchange rel) {
                throw new AssertionError("Unexpected call: " + rel);
            }

            @Override
            public FragmentMapping visit(IgniteColocatedHashAggregate rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteMapHashAggregate rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteReduceHashAggregate rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteColocatedSortAggregate rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteMapSortAggregate rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteReduceSortAggregate rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteTableModify rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteValues rel) {
                return fragmentMapping(rel, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteUnionAll rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteSort rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteTableSpool rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteSortedIndexSpool rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteLimit rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteHashIndexSpool rel) {
                return fragmentMapping(rel, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteSetOp rel) {
                assert rel instanceof SetOp;
                SetOp rel0 = (SetOp) rel;
                return fragmentMapping(rel0, mq, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteTableFunctionScan rel) {
                return fragmentMapping(rel, ctx);
            }

            @Override
            public FragmentMapping visit(IgniteRel rel) {
                throw new AssertionError("Unexpected call: " + rel);
            }
        };

        IgniteRel rel0 = (IgniteRel) rel;

        return rel0.accept(visitor);
    }

    /**
     * Requests meta information about nodes capable to execute a query over particular partitions.
     *
     * @param rel Relational node.
     * @param mq  Metadata query instance. Used to request appropriate metadata from node children.
     * @return Nodes mapping, representing a list of nodes capable to execute a query over particular partitions.
     */
    private static FragmentMapping fragmentMapping(SingleRel rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        return fragmentMappingForMetadataQuery(rel.getInput(), mq, ctx);
    }

    /**
     * See {@link IgniteFragmentMapping#fragmentMapping(SingleRel, RelMetadataQuery, MappingQueryContext)}.
     *
     * <p>{@link ColocationMappingException} may be thrown on two children nodes locations merge. This means that the fragment
     * (which part the parent node is) cannot be executed on any node and additional exchange is needed. This case we throw {@link
     * NodeMappingException} with an edge, where we need the additional exchange. After the exchange is put into the fragment and the
     * fragment is split into two ones, fragment meta information will be recalculated for all fragments.
     */
    private static FragmentMapping fragmentMapping(BiRel rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        RelNode left = rel.getLeft();
        RelNode right = rel.getRight();

        FragmentMapping frgLeft = fragmentMappingForMetadataQuery(left, mq, ctx);
        FragmentMapping frgRight = fragmentMappingForMetadataQuery(right, mq, ctx);

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
     * See {@link IgniteFragmentMapping#fragmentMapping(SingleRel, RelMetadataQuery, MappingQueryContext)}
     *
     * <p>{@link ColocationMappingException} may be thrown on two children nodes locations merge. This means that the
     * fragment (which part the parent node is) cannot be executed on any node and additional exchange is needed. This case we throw {@link
     * NodeMappingException} with an edge, where we need the additional exchange. After the exchange is put into the fragment and the
     * fragment is split into two ones, fragment meta information will be recalculated for all fragments.
     */
    private static FragmentMapping fragmentMapping(SetOp rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        FragmentMapping res = null;

        if (TraitUtils.distribution(rel) == IgniteDistributions.random()) {
            for (RelNode input : rel.getInputs()) {
                res = res == null ? fragmentMappingForMetadataQuery(input, mq, ctx) : res.combine(
                        fragmentMappingForMetadataQuery(input, mq, ctx));
            }
        } else {
            for (RelNode input : rel.getInputs()) {
                try {
                    res = res == null ? fragmentMappingForMetadataQuery(input, mq, ctx) : res.colocate(
                            fragmentMappingForMetadataQuery(input, mq, ctx));
                } catch (ColocationMappingException e) {
                    throw new NodeMappingException("Failed to calculate physical distribution", input, e);
                }
            }
        }

        return res;
    }

    /**
     * See {@link IgniteFragmentMapping#fragmentMapping(SingleRel, RelMetadataQuery, MappingQueryContext)}.
     *
     * <p>Prunes involved partitions (hence nodes, involved in query execution) if possible.
     */
    private static FragmentMapping fragmentMapping(IgniteFilter rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        return fragmentMappingForMetadataQuery(rel.getInput(), mq, ctx).prune(rel);
    }

    /**
     * See {@link IgniteFragmentMapping#fragmentMapping(SingleRel, RelMetadataQuery, MappingQueryContext)}.
     *
     * <p>Prunes involved partitions (hence nodes, involved in query execution) if possible.
     */
    private static FragmentMapping fragmentMapping(IgniteTrimExchange rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        try {
            return FragmentMapping.create(rel.sourceId())
                    .colocate(fragmentMappingForMetadataQuery(rel.getInput(), mq, ctx));
        } catch (ColocationMappingException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * See {@link IgniteFragmentMapping#fragmentMapping(SingleRel, RelMetadataQuery, MappingQueryContext)}.
     */
    private static FragmentMapping fragmentMapping(IgniteReceiver rel) {
        return FragmentMapping.create(rel.exchangeId());
    }

    /**
     * See {@link IgniteFragmentMapping#fragmentMapping(SingleRel, RelMetadataQuery, MappingQueryContext)}.
     */
    private static FragmentMapping fragmentMapping(IgniteIndexScan rel, MappingQueryContext ctx) {
        return getFragmentMapping(rel.sourceId(), rel, ctx);
    }

    /**
     * See {@link IgniteFragmentMapping#fragmentMapping(SingleRel, RelMetadataQuery, MappingQueryContext)}.
     */
    private static FragmentMapping fragmentMapping(IgniteTableScan rel, MappingQueryContext ctx) {
        return getFragmentMapping(rel.sourceId(), rel, ctx);
    }

    /**
     * See {@link IgniteFragmentMapping#fragmentMapping(SingleRel, RelMetadataQuery, MappingQueryContext)}.
     */
    private static FragmentMapping fragmentMapping(IgniteValues rel, MappingQueryContext ctx) {
        ColocationGroup group = ColocationGroup.forNodes(ctx.mappingService().executionNodes(false, null));

        return FragmentMapping.create(rel.sourceId(), group);
    }

    /**
     * See {@link IgniteFragmentMapping#fragmentMapping(SingleRel, RelMetadataQuery, MappingQueryContext)}.
     */
    private static FragmentMapping fragmentMapping(IgniteTableModify rel, RelMetadataQuery mq, MappingQueryContext ctx) {
        RelNode input = rel.getInput();
        FragmentMapping mapping = fragmentMappingForMetadataQuery(input, mq, ctx);

        // In case of the statement like UPDATE t SET a = a + 1
        // this will be the second call to the collation group, hence the result may differ.
        // But such query should be rejected during execution, since we will try to do RW read
        // from replica that is not primary anymore.
        ColocationGroup tableColocationGroup = rel.getTable().unwrap(IgniteTable.class).colocationGroup(ctx);
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
     * See {@link IgniteFragmentMapping#fragmentMapping(SingleRel, RelMetadataQuery, MappingQueryContext)}.
     */
    private static FragmentMapping fragmentMapping(IgniteTableFunctionScan rel, MappingQueryContext ctx) {
        ColocationGroup group = ColocationGroup.forNodes(ctx.mappingService().executionNodes(false, null));

        return FragmentMapping.create(rel.sourceId(), group);
    }

    private static FragmentMapping getFragmentMapping(long sourceId, ProjectableFilterableTableScan rel, MappingQueryContext ctx) {
        ColocationGroup group = rel.getTable().unwrap(IgniteTable.class).colocationGroup(ctx);

        return FragmentMapping.create(sourceId, group);
    }
}
