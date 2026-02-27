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

import static org.apache.calcite.rex.RexUtil.expandSearch;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.sql.engine.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.sql.engine.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.jetbrains.annotations.Nullable;

/**
 * IgniteMdSelectivity supplies implementation of {@link RelMetadataQuery#getSelectivity}.
 */
public class IgniteMdSelectivity extends RelMdSelectivity {
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(new IgniteMdSelectivity(), BuiltInMetadata.Selectivity.Handler.class);

    public static final double EQ_SELECTIVITY = 0.333;
    public static final double IS_NOT_NULL_SELECTIVITY = 0.9;
    public static final double COMPARISON_SELECTIVITY = 0.5;
    public static final double DEFAULT_SELECTIVITY_INCREMENT = 0.05;
    public static final double DEFAULT_SELECTIVITY = 0.25;

    private static double computeOpsSelectivity(Map<RexNode, List<SqlKind>> operands, double baseSelectivity) {
        double result = baseSelectivity;

        for (Map.Entry<RexNode, List<SqlKind>> e : operands.entrySet()) {
            int eqNum = 0;
            double result0 = 0.0;

            for (SqlKind kind : e.getValue()) {
                switch (kind) {
                    case IS_NOT_NULL:
                        result0 = Math.max(result0, IS_NOT_NULL_SELECTIVITY);
                        break;
                    case EQUALS:
                        // Take into account Zipf`s distribution.
                        result0 = Math.min(result0 + (EQ_SELECTIVITY / Math.sqrt(++eqNum)), 1.0);
                        break;
                    case GREATER_THAN:
                    case LESS_THAN:
                    case GREATER_THAN_OR_EQUAL:
                    case LESS_THAN_OR_EQUAL:
                        result0 = Math.min(result0 + COMPARISON_SELECTIVITY, 1.0);
                        break;
                    default:
                        // Not clear here, proceed with default.
                        result0 += DEFAULT_SELECTIVITY_INCREMENT;
                }
            }

            result = Math.max(result, result0);
        }

        return result;
    }

    /**
     * Implements selectivity prediction algorithm. <br>
     * Current implementation work as follows: <br><br>
     * 1. If mixed OR-related operands are processed i.e: OR(=($t1, 'D'), =($t1, 'M'), =($t2, 'W')) selectivity computes separately
     * for each local ref and a big one is chosen <br>
     * 2. If mixed OR or AND related operands are processed i.e:
     * OR(<($t3, 110), >($t3, 150), AND(>=($t2, -($t1, 2)), <=($t2, +($t3, 2))), >($t4, $t2), <($t4, $t3)) selectivity computes separately
     * for each local ref with AND selectivity adjustment. <br>
     */
    private static double computeOrSelectivity(RexCall call, @Nullable BitSet primaryKeys, @Nullable Mapping columnMapping) {
        List<RexNode> operands = call.operands;
        List<RexNode> andOperands = new ArrayList<>();
        List<RexNode> otherOperands = new ArrayList<>();
        double baseSelectivity = 0.0;
        Map<RexNode, List<SqlKind>> processOperands = new HashMap<>();

        assert !operands.isEmpty();

        boolean andConsist = operands.stream().anyMatch(op -> op.isA(SqlKind.AND));

        if (andConsist) {
            for (RexNode op : operands) {
                if (op.isA(SqlKind.AND)) {
                    andOperands.add(op);
                } else {
                    otherOperands.add(op);
                }
            }
        }

        // AND inside OR
        for (RexNode andOp : andOperands) {
            baseSelectivity = Math.max(baseSelectivity, guessAndSelectivity(andOp, primaryKeys == null
                    ? null : (BitSet) primaryKeys.clone(), columnMapping));
        }

        List<RexNode> operandsToProcess = andConsist ? otherOperands : call.getOperands();

        for (RexNode node : operandsToProcess) {
            RexNode ref = getLocalRef(node);

            // It can be null for case with expression for example: LENGTH('abc') = 3
            if (ref != null) {
                List<SqlKind> vals = processOperands.computeIfAbsent(ref, (k) -> new ArrayList<>());

                vals.add(node.getKind());
            } else {
                baseSelectivity = Math.max(baseSelectivity, nonColumnRefSelectivity(call));
            }
        }

        return computeOpsSelectivity(processOperands, baseSelectivity);
    }

    private static double nonColumnRefSelectivity(RexCall call) {
        if (call.isA(SqlKind.EQUALS)) {
            return EQ_SELECTIVITY;
        } else if (call.isA(SqlKind.COMPARISON)) {
            return COMPARISON_SELECTIVITY;
        } else {
            // different OTHER_FUNCTION and other uncovered cases
            return 0;
        }
    }

    private static RexLocalRef getLocalRef(RexNode node) {
        RexVisitor<Void> v = new RexVisitorImpl<>(true) {
            @Override
            public Void visitLocalRef(RexLocalRef locRef) {
                throw new Util.FoundOne(locRef);
            }
        };

        try {
            node.accept(v);

            return null;
        } catch (Util.FoundOne e) {
            return (RexLocalRef) e.getNode();
        }
    }

    private static double guessSelectivity(@Nullable RexNode predicate, ProjectableFilterableTableScan rel) {
        double sel = 1.0;
        if ((predicate == null) || predicate.isAlwaysTrue()) {
            return sel;
        }

        @Nullable IgniteTable table = rel.getTable().unwrap(IgniteTable.class);

        ImmutableIntList keyColumns;
        BitSet primaryKeys = null;
        Mapping columnMapping = null;

        // sys view is possible here
        if (table != null) {
            int colCount = table.getRowType(Commons.typeFactory()).getFieldCount();
            columnMapping = rel.requiredColumns() == null
                    ? Mappings.createIdentity(colCount)
                    : Commons.projectedMapping(colCount, rel.requiredColumns());

            keyColumns = table.keyColumns();
            primaryKeys = new BitSet();

            for (int i : keyColumns) {
                primaryKeys.set(i);
            }
        }

        double artificialSel = 1.0;

        // make set of AND ... AND calls
        List<RexNode> conjunctions = RelOptUtil.conjunctions(predicate);

        for (RexNode pred : conjunctions) {
            // Expand sarg`s
            RexNode predicateExpanded = expandSearch(Commons.rexBuilder(), null, pred);

            if (predicateExpanded.isA(SqlKind.OR)) {
                double processed = computeOrSelectivity((RexCall) predicateExpanded, primaryKeys == null
                        ? null : (BitSet) primaryKeys.clone(), columnMapping);
                sel *= processed;
            } else {
                sel *= computeSelectivity(predicateExpanded, primaryKeys, columnMapping);
            }
        }

        return sel * artificialSel;
    }

    private static double guessAndSelectivity(@Nullable RexNode predicate, @Nullable BitSet keyColumns, @Nullable Mapping columnMapping) {
        double sel = 1.0;
        if ((predicate == null) || predicate.isAlwaysTrue()) {
            return sel;
        }

        List<RexNode> conjunctions = RelOptUtil.conjunctions(predicate);

        for (RexNode pred : conjunctions) {
            sel *= computeSelectivity(pred, keyColumns, columnMapping);
        }

        return sel;
    }

    private static double computeSelectivity(RexNode predicate, @Nullable BitSet keyColumns, @Nullable Mapping columnMapping) {
        double sel = 1.0;
        double artificialSel = 1.0;

        if (predicate.getKind() == SqlKind.IS_NOT_NULL) {
            sel *= IS_NOT_NULL_SELECTIVITY;
        } else if (
                (predicate instanceof RexCall)
                        && (((RexCall) predicate).getOperator()
                        == RelMdUtil.ARTIFICIAL_SELECTIVITY_FUNC)) {
            artificialSel *= RelMdUtil.getSelectivityValue(predicate);
        } else if (predicate.isA(SqlKind.EQUALS)) {
            if (keyColumns != null) {
                assert columnMapping != null;
                RexLocalRef localRef = getLocalRef(predicate);

                if (localRef != null) {
                    keyColumns.clear(columnMapping.getSource(localRef.getIndex()));
                    if (keyColumns.isEmpty()) {
                        return 0.0;
                    }
                }
            }
            sel *= EQ_SELECTIVITY;
        } else if (predicate.isA(SqlKind.COMPARISON)) {
            sel *= COMPARISON_SELECTIVITY;
        } else {
            sel *= DEFAULT_SELECTIVITY;
        }

        return sel * artificialSel;
    }

    /** Implements selectivity prediction algorithm.
     *
     * @param rel Relational operator.
     * @param mq Relational metadata.
     * @param predicate Operation predicate.
     */
    public Double getSelectivity(ProjectableFilterableTableScan rel, RelMetadataQuery mq, RexNode predicate) {
        if (predicate == null) {
            return guessSelectivity(rel.condition(), rel);
        }

        RexNode condition = rel.pushUpPredicate();
        if (condition == null) {
            return guessSelectivity(predicate, rel);
        }

        RexNode diff = RelMdUtil.minusPreds(RexUtils.builder(rel), predicate, condition);
        return guessSelectivity(diff, rel);
    }

    /** Implements selectivity prediction algorithm.
     *
     * @param rel Relational operator.
     * @param mq Relational metadata.
     * @param predicate Operation predicate.
     */
    public Double getSelectivity(IgniteSortedIndexSpool rel, RelMetadataQuery mq, RexNode predicate) {
        if (predicate != null) {
            return mq.getSelectivity(rel.getInput(),
                    RelMdUtil.minusPreds(
                            rel.getCluster().getRexBuilder(),
                            predicate,
                            rel.condition()));
        }

        return mq.getSelectivity(rel.getInput(), rel.condition());
    }

    /** Implements selectivity prediction algorithm.
     *
     * @param rel Relational operator.
     * @param mq Relational metadata.
     * @param predicate Operation predicate.
     */
    public Double getSelectivity(IgniteHashIndexSpool rel, RelMetadataQuery mq, RexNode predicate) {
        if (predicate != null) {
            return mq.getSelectivity(rel.getInput(),
                    RelMdUtil.minusPreds(
                            rel.getCluster().getRexBuilder(),
                            predicate,
                            rel.condition()));
        }

        return mq.getSelectivity(rel.getInput(), rel.condition());
    }

}
