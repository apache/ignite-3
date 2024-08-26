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

package org.apache.ignite.internal.sql.engine.prepare.pruning;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitor;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
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
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteSetOp;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Visitor which traverse subtree below {@link IgniteTableModify modify node} in order to find all constant-at-runtime expressions to derive
 * metadata for partition pruning.
 */
class ModifyNodeVisitor implements IgniteRelVisitor<List<List<RexNode>>> {
    private static final IgniteLogger LOG = Loggers.forClass(ModifyNodeVisitor.class);

    private ModifyNodeVisitor() {
    }

    /**
     * Returns list of expressions to derive pruning metadata from, or {@code null} if expressions can't be collected.
     *
     * <p>Every element from outer list contributes a partition, so after pruning all partitions defined by these
     * expressions must be preserved in the plan.
     *
     * @param tableModify A modify node to start traversal from.
     * @return A list of expressions. It's guaranteed that returned collection represents complete set of partitions. Returns {@code null}
     *         otherwise.
     */
    static @Nullable List<List<RexNode>> go(IgniteTableModify tableModify) {
        var modifyNodeShuttle = new ModifyNodeVisitor();

        return modifyNodeShuttle.visit((IgniteRel) tableModify.getInput());
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteValues rel) {
        return Commons.cast(rel.tuples);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteProject rel) {
        List<List<RexNode>> result = processSingleInputRel(rel);

        if (nullOrEmpty(result)) {
            return result;
        }

        List<List<RexNode>> projectedResult = new ArrayList<>(result.size());
        for (List<RexNode> inputExpressions : result) {
            RexVisitor<RexNode> inputInliner = new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef inputRef) {
                    return inputExpressions.get(inputRef.getIndex());
                }
            };

            projectedResult.add(inputInliner.visitList(rel.getProjects()));
        }

        return projectedResult;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteUnionAll rel) {
        List<IgniteRel> inputs = Commons.cast(rel.getInputs());
        List<List<RexNode>> results = new ArrayList<>();

        for (IgniteRel input : inputs) {
            List<List<RexNode>> inputResult = visit(input);
            if (inputResult == null) {
                return null;
            }

            results.addAll(inputResult);
        }

        return results;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteSort rel) {
        return processSingleInputRel(rel);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteTableSpool rel) {
        return processSingleInputRel(rel);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteSortedIndexSpool rel) {
        return processSingleInputRel(rel);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteLimit rel) {
        return processSingleInputRel(rel);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteHashIndexSpool rel) {
        return processSingleInputRel(rel);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteSender rel) {
        return processSingleInputRel(rel);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteFilter rel) {
        if (rel.getCondition().isAlwaysFalse()) {
            return List.of();
        }

        return processSingleInputRel(rel);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteTrimExchange rel) {
        return processSingleInputRel(rel);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteExchange rel) {
        return processSingleInputRel(rel);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteNestedLoopJoin rel) {
        // processing of joins are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteHashJoin rel) {
        // processing of joins are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteCorrelatedNestedLoopJoin rel) {
        // processing of joins are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteMergeJoin rel) {
        // processing of joins are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteIndexScan rel) {
        // processing of index scan is not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteTableScan rel) {
        // processing of table scan is not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteSystemViewScan rel) {
        // processing of system view scan is not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteReceiver rel) {
        // receiver is a leaf operator of fragment. It doesn't have information about remote input, 
        // thus pruning is impossible
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteColocatedHashAggregate rel) {
        // processing of aggregates are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteMapHashAggregate rel) {
        // processing of aggregates are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteReduceHashAggregate rel) {
        // processing of aggregates are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteColocatedSortAggregate rel) {
        // processing of aggregates are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteMapSortAggregate rel) {
        // processing of aggregates are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteReduceSortAggregate rel) {
        // processing of aggregates are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteSetOp rel) {
        // processing of set operators are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteTableFunctionScan rel) {
        // technically, we may try to forecast returned values by table functions, but this is not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteTableModify rel) {
        return processUnexpected("Plan can contains only one ModifyNode.");
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteKeyValueGet rel) {
        return processUnexpected("KV plans are not supposed to be part of distributed plans [node={}].", rel.getClass().getName());
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteKeyValueModify rel) {
        return processUnexpected("KV plans are not supposed to be part of distributed plans [node={}].", rel.getClass().getName());
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteRel rel) {
        return rel.accept(this);
    }

    private @Nullable List<List<RexNode>> processSingleInputRel(IgniteRel rel) {
        List<IgniteRel> inputs = Commons.cast(rel.getInputs());

        assert inputs.size() == 1 : "Relation with multiple inputs must be processed explicitly";

        //noinspection ConstantValue
        if (inputs.size() != 1) {
            return null;
        }

        return visit(inputs.get(0));
    }

    private static @Nullable List<List<RexNode>> processUnexpected(String messageTemplate, Object... args) {
        if (IgniteUtils.assertionsEnabled()) {
            String message = format(messageTemplate, args);

            assert false : message;
        }

        LOG.warn(messageTemplate, args);

        return null;
    }
}
