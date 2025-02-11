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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.sql.engine.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.sql.engine.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * IgniteMdSelectivity supplies implementation of {@link RelMetadataQuery#getSelectivity}.
 */
public class IgniteMdSelectivity extends RelMdSelectivity {
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.SELECTIVITY.method, new IgniteMdSelectivity());

    /**
     * Implements shuttle for selectivity prediction. <br>
     * Current implementation work as follows: <br><br>
     * 1. If mixed OR-related operands are processed i.e: OR(=($t1, 'D'), =($t1, 'M'), =($t2, 'W')) selectivity computes separately
     * for each local ref. <br>
     * 2. If mixed OR or AND related operands are processed i.e:
     * OR(<($t3, 110), >($t3, 150), AND(>=($t2, -($t1, 2)), <=($t2, +($t3, 2))), >($t4, $t2), <($t4, $t3)) selectivity computes separately
     * for each local ref with AND selectivity adjustment. <br>
     */
    private static class SelectivityShuttle extends RexShuttle {
        private final Map<RexNode, List<SqlKind>> orOps = new HashMap<>();
        private double andSelectivity;

        double computeSelectivity() {
            double result = andSelectivity;

            for (Map.Entry<RexNode, List<SqlKind>> e : orOps.entrySet()) {
                int eqNum = 0;
                double result0 = 0.0;

                for (SqlKind kind : e.getValue()) {
                    switch (kind) {
                        case IS_NOT_NULL:
                            result0 = Math.max(result0, 0.9);
                            break;
                        case EQUALS:
                            // Take into account Zipf`s distribution.
                            result0 = Math.min(result0 + (0.333 / Math.sqrt(++eqNum)), 1.0);
                            break;
                        case GREATER_THAN:
                        case LESS_THAN:
                        case GREATER_THAN_OR_EQUAL:
                        case LESS_THAN_OR_EQUAL:
                            result0 = Math.min(result0 + 0.5, 1.0);
                            break;
                        default:
                            // Not clear here, proceed with default.
                            result0 *= 0.25;
                    }
                }

                result = Math.max(result, result0);
            }

            return result;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            if (call.isA(SqlKind.OR)) {
                ImmutableList<RexNode> operands = call.operands;
                List<RexNode> andOperands = null;
                List<RexNode> otherOperands = null;

                assert !operands.isEmpty();

                for (RexNode op : operands) {
                    if (op.isA(SqlKind.AND)) {
                       if (andOperands == null) {
                           andOperands = new ArrayList<>();
                       }

                       andOperands.add(op);
                    } else {
                        if (otherOperands == null) {
                            otherOperands = new ArrayList<>();
                        }

                        otherOperands.add(op);
                    }
                }

                // AND inside OR
                if (andOperands != null) {
                    for (RexNode andOp : andOperands) {
                        andSelectivity = Math.max(andSelectivity, RelMdUtil.guessSelectivity(andOp));
                    }
                }

                // nothing to process
                if (otherOperands == null) {
                    return null;
                } else if (otherOperands.size() == 1) {
                    call = (RexCall) otherOperands.get(0);
                } else {
                    // otherwise build new OR without AND operands
                    RexCall newCall = (RexCall) Commons.rexBuilder().makeCall(call.op, otherOperands);

                    return super.visitCall(newCall);
                }
            } else if (call.isA(SqlKind.AND)) {
                assert false : "Should never happen";
            }

            RexNode res = getLocalRef(call);

            // It can be null for case with expression for example: LENGTH('abc') = 3
            if (res != null) {
                List<SqlKind> vals = orOps.computeIfAbsent(res, (k) -> new ArrayList<>());

                vals.add(call.getKind());
            }

            return super.visitCall(call);
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

    private static double guessSelectivity(@Nullable RexNode predicate) {
        double sel = 1.0;
        if ((predicate == null) || predicate.isAlwaysTrue()) {
            return sel;
        }

        double artificialSel = 1.0;

        // make set of AND ... AND calls
        List<RexNode> conjunctions = RelOptUtil.conjunctions(predicate);

        for (RexNode pred : conjunctions) {
            // Expand sarg`s
            RexNode predicateExpanded = expandSearch(Commons.rexBuilder(), null, pred);

            if (predicateExpanded.isA(SqlKind.OR)) {
                SelectivityShuttle shuttle = new SelectivityShuttle();
                predicateExpanded.accept(shuttle);
                double processed = shuttle.computeSelectivity();
                sel *= processed;
            } else if (predicateExpanded.getKind() == SqlKind.IS_NOT_NULL) {
                sel *= 0.9;
            } else if (
                    (predicateExpanded instanceof RexCall)
                            && (((RexCall) predicateExpanded).getOperator()
                            == RelMdUtil.ARTIFICIAL_SELECTIVITY_FUNC)) {
                artificialSel *= RelMdUtil.getSelectivityValue(predicateExpanded);
            } else if (predicateExpanded.isA(SqlKind.EQUALS)) {
                sel *= 0.15;
            } else if (predicateExpanded.isA(SqlKind.COMPARISON)) {
                sel *= 0.5;
            } else {
                sel *= 0.25;
            }
        }

        return sel * artificialSel;
    }

    /**
     * GetSelectivity.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public Double getSelectivity(ProjectableFilterableTableScan rel, RelMetadataQuery mq, RexNode predicate) {
        if (predicate == null) {
            return guessSelectivity(rel.condition());
        }

        RexNode condition = rel.pushUpPredicate();
        if (condition == null) {
            return RelMdUtil.guessSelectivity(predicate);
        }

        RexNode diff = RelMdUtil.minusPreds(RexUtils.builder(rel), predicate, condition);
        return RelMdUtil.guessSelectivity(diff);
    }

    /**
     * GetSelectivity.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
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

    /**
     * GetSelectivity.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
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

    /** Guess cost multiplier regarding search bounds only. */
    private static double guessCostMultiplier(SearchBounds bounds) {
        if (bounds instanceof ExactBounds) {
            return .1;
        } else if (bounds instanceof RangeBounds) {
            RangeBounds rangeBounds = (RangeBounds) bounds;

            if (rangeBounds.condition() != null) {
                return ((RexCall) rangeBounds.condition()).op.kind == SqlKind.EQUALS ? .1 : .2;
            } else {
                return .35;
            }
        } else if (bounds instanceof MultiBounds) {
            MultiBounds multiBounds = (MultiBounds) bounds;

            return multiBounds.bounds().stream()
                    .mapToDouble(IgniteMdSelectivity::guessCostMultiplier)
                    .max()
                    .orElseThrow(AssertionError::new);
        }

        return 1.0;
    }
}
