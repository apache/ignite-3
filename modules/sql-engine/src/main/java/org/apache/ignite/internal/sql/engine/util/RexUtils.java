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

package org.apache.ignite.internal.sql.engine.util;

import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;
import static org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING;
import static org.apache.calcite.rel.RelFieldCollation.NullDirection.FIRST;
import static org.apache.calcite.rel.RelFieldCollation.NullDirection.LAST;
import static org.apache.calcite.rex.RexUtil.composeConjunction;
import static org.apache.calcite.rex.RexUtil.composeDisjunction;
import static org.apache.calcite.rex.RexUtil.flattenAnd;
import static org.apache.calcite.rex.RexUtil.flattenOr;
import static org.apache.calcite.rex.RexUtil.removeCast;
import static org.apache.calcite.rex.RexUtil.sargRef;
import static org.apache.calcite.sql.SqlKind.BINARY_COMPARISON;
import static org.apache.calcite.sql.SqlKind.EQUALS;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.IS_NOT_DISTINCT_FROM;
import static org.apache.calcite.sql.SqlKind.IS_NOT_NULL;
import static org.apache.calcite.sql.SqlKind.IS_NULL;
import static org.apache.calcite.sql.SqlKind.LESS_THAN;
import static org.apache.calcite.sql.SqlKind.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.LIKE;
import static org.apache.calcite.sql.SqlKind.NOT;
import static org.apache.calcite.sql.SqlKind.OR;
import static org.apache.calcite.sql.SqlKind.SEARCH;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import org.apache.calcite.DataContext;
import org.apache.calcite.DataContext.Variable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.Util.FoundOne;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.exp.IgniteSqlFunctions;
import org.apache.ignite.internal.sql.engine.prepare.OutOfRangeLiteralComparisonReductionShuttle;
import org.apache.ignite.internal.sql.engine.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.jetbrains.annotations.Nullable;

/**
 * RexUtils.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class RexUtils {
    /** Maximum amount of search bounds tuples per scan. */
    public static final int MAX_SEARCH_BOUNDS_COMPLEXITY = 100;

    /** Hash index permitted search operations. */
    private static final EnumSet<SqlKind> HASH_SEARCH_OPS = EnumSet.of(EQUALS, IS_NOT_DISTINCT_FROM);

    /**
     * Builder.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static RexBuilder builder(RelNode rel) {
        return builder(rel.getCluster());
    }

    /**
     * Builder.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static RexBuilder builder(RelOptCluster cluster) {
        return cluster.getRexBuilder();
    }

    /**
     * Executor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static RexExecutor executor(RelNode rel) {
        return executor(rel.getCluster());
    }

    /**
     * Executor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static RexExecutor executor(RelOptCluster cluster) {
        return Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR);
    }

    /** Returns whether a list of expressions projects the incoming fields. */
    public static boolean isIdentity(List<? extends RexNode> projects, RelDataType inputRowType) {
        if (inputRowType.getFieldCount() != projects.size()) {
            return false;
        }

        List<RelDataTypeField> fields = inputRowType.getFieldList();

        for (int i = 0; i < fields.size(); i++) {
            if (!(projects.get(i) instanceof RexLocalRef)) {
                return false;
            }

            RexSlot ref = (RexSlot) projects.get(i);

            if (ref.getIndex() != i) {
                return false;
            }

            if (!RelOptUtil.eq("t1", projects.get(i).getType(), "t2", fields.get(i).getType(), Litmus.IGNORE)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Try to transform expression into DNF form.
     *
     * @param rexBuilder Expression builder.
     * @param node Expression to process.
     * @param maxOrNodes Max OR nodes in output.
     * @return DNF expression representation or {@code null} if limit is reached.
     */
    public static @Nullable RexNode tryToDnf(RexBuilder rexBuilder, RexNode node, int maxOrNodes) {
        DnfHelper helper = new DnfHelper(Commons.rexBuilder(), maxOrNodes);

        try {
            return helper.toDnf(node);
        } catch (FoundOne e) {
            return null;
        }
    }

    /** Calcite based dnf wrapper with OR nodes limitation. */
    static class DnfHelper {
        final RexBuilder rexBuilder;
        final int maxOrNodes;

        /**
         * Constructor.
         *
         * @param rexBuilder Rex builder.
         * @param maxOrNodes Limit for OR nodes, if limit is reached further processing will be stopped.
         */
        DnfHelper(RexBuilder rexBuilder, int maxOrNodes) {
            this.rexBuilder = rexBuilder;
            this.maxOrNodes = maxOrNodes;
        }

        private RexNode toDnf(RexNode rex) {
            List<RexNode> operands;
            switch (rex.getKind()) {
                case AND:
                    operands = flattenAnd(((RexCall) rex).getOperands());
                    RexNode head = operands.get(0);
                    RexNode headDnf = toDnf(head);
                    List<RexNode> headDnfs = RelOptUtil.disjunctions(headDnf);
                    RexNode tail = and(Util.skip(operands));
                    RexNode tailDnf = toDnf(tail);
                    List<RexNode> tailDnfs = RelOptUtil.disjunctions(tailDnf);
                    List<RexNode> list = new ArrayList<>(headDnfs.size() * tailDnfs.size());
                    for (RexNode h : headDnfs) {
                        for (RexNode t : tailDnfs) {
                            list.add(and(ImmutableList.of(h, t)));
                        }
                    }
                    return or(list);
                case OR:
                    operands = flattenOr(((RexCall) rex).getOperands());
                    List<RexNode> processed = toDnfs(operands);
                    return or(processed);
                case NOT:
                    RexNode arg = ((RexCall) rex).getOperands().get(0);
                    switch (arg.getKind()) {
                        case NOT:
                            return toDnf(((RexCall) arg).getOperands().get(0));
                        case OR:
                            operands = ((RexCall) arg).getOperands();
                            return toDnf(
                                    and(Util.transform(flattenOr(operands), RexUtil::not)));
                        case AND:
                            operands = ((RexCall) arg).getOperands();
                            return toDnf(
                                    or(Util.transform(flattenAnd(operands), RexUtil::not)));
                        default:
                            return rex;
                    }
                default:
                    return rex;
            }
        }

        private List<RexNode> toDnfs(List<RexNode> nodes) {
            List<RexNode> list = new ArrayList<>();
            for (RexNode node : nodes) {
                RexNode dnf = toDnf(node);
                switch (dnf.getKind()) {
                    case OR:
                        list.addAll(((RexCall) dnf).getOperands());
                        break;
                    default:
                        list.add(dnf);
                }
            }
            return list;
        }

        private RexNode and(Iterable<? extends RexNode> nodes) {
            return composeConjunction(rexBuilder, nodes);
        }

        private RexNode or(Iterable<? extends RexNode> nodes) {
            RexNode res = composeDisjunction(rexBuilder, nodes);
            if (res instanceof RexCall) {
                RexCall res0 = (RexCall) res;
                if (res0.getOperands().size() > maxOrNodes) {
                    throw FoundOne.NULL;
                }
            }
            return res;
        }
    }

    /** Supported index operations. */
    private static final Set<SqlKind> TREE_INDEX_COMPARISON =
            EnumSet.of(
                    SEARCH,
                    LIKE,
                    IS_NULL,
                    IS_NOT_NULL,
                    EQUALS,
                    IS_NOT_DISTINCT_FROM,
                    LESS_THAN, GREATER_THAN,
                    GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL);

    /**
     * Builds sorted index search bounds.
     */
    public static @Nullable List<SearchBounds> buildSortedSearchBounds(
            RelOptCluster cluster,
            RelCollation collation,
            @Nullable RexNode condition,
            RelDataType rowType,
            @Nullable ImmutableIntList requiredColumns
    ) {
        return buildSearchBounds(true, cluster, collation, condition, rowType, requiredColumns);
    }

    /**
     * Builds hash index search bounds.
     */
    public static @Nullable List<SearchBounds> buildHashSearchBounds(
            RelOptCluster cluster,
            RelCollation collation,
            @Nullable RexNode condition,
            RelDataType rowType,
            @Nullable ImmutableIntList requiredColumns
    ) {
        List<SearchBounds> bounds = buildSearchBounds(false, cluster, collation, condition, rowType, requiredColumns);

        if (bounds == null) {
            return null;
        }

        if (bounds.stream().anyMatch(Objects::isNull)) {
            // hash index doesn't support search by prefix
            return null;
        }

        return bounds;
    }

    /**
     * Builds hash index search bounds.
     */
    public static List<SearchBounds> buildHashSearchBounds(
            RelOptCluster cluster,
            RexNode condition,
            RelDataType rowType,
            @Nullable ImmutableBitSet requiredColumns
    ) {
        condition = RexUtil.toCnf(builder(cluster), condition);

        Int2ObjectMap<List<RexCall>> fieldsToPredicates = mapPredicatesToFields(condition, cluster);

        if (nullOrEmpty(fieldsToPredicates)) {
            return null;
        }

        List<SearchBounds> bounds = null;

        List<RelDataType> types = RelOptUtil.getFieldTypeList(rowType);

        Mappings.TargetMapping mapping = null;

        if (requiredColumns != null) {
            mapping = Commons.trimmingMapping(types.size(), requiredColumns);
        }

        for (Entry<List<RexCall>> fld : fieldsToPredicates.int2ObjectEntrySet()) {
            List<RexCall> collFldPreds = fld.getValue();

            if (nullOrEmpty(collFldPreds)) {
                break;
            }

            for (RexCall pred : collFldPreds) {
                if (!pred.isA(HASH_SEARCH_OPS)) {
                    return null;
                }

                if (bounds == null) {
                    bounds = Arrays.asList(new SearchBounds[types.size()]);
                }

                int fldIdx;

                if (mapping != null) {
                    fldIdx = mapping.getSourceOpt(fld.getIntKey());
                } else {
                    fldIdx = fld.getIntKey();
                }

                bounds.set(fldIdx, new ExactBounds(pred, pred.operands.get(1)));
            }
        }

        return bounds;
    }

    private static @Nullable List<SearchBounds> buildSearchBounds(
            boolean allowRange,
            RelOptCluster cluster,
            RelCollation collation,
            @Nullable RexNode condition,
            RelDataType rowType,
            @Nullable ImmutableIntList requiredColumns
    ) {
        if (condition == null) {
            return null;
        }

        condition = RexUtil.toCnf(builder(cluster), condition);

        Int2ObjectMap<List<RexCall>> fieldsToPredicates = mapPredicatesToFields(condition, cluster);

        if (nullOrEmpty(fieldsToPredicates)) {
            return null;
        }

        // Force collation for all fields of the condition.
        if (collation == null || collation.isDefault()) {
            IntArrayList fields = new IntArrayList(fieldsToPredicates.size());
            IntArrayList lastFields = new IntArrayList(fieldsToPredicates.size());

            // It's more effective to put equality conditions in the collation first.
            fieldsToPredicates.int2ObjectEntrySet()
                    .forEach(entry -> {
                        (entry.getValue().stream().anyMatch(v -> v.getOperator().getKind() == EQUALS) ? fields : lastFields)
                                .add(entry.getIntKey());
                    });
            fields.addAll(lastFields);

            collation = TraitUtils.createCollation(fields);
        }

        List<RelDataType> types = RelOptUtil.getFieldTypeList(rowType);

        Mappings.TargetMapping mapping = requiredColumns == null
                ? Mappings.createIdentity(types.size())
                : Commons.projectedMapping(types.size(), requiredColumns);

        List<SearchBounds> bounds = Arrays.asList(new SearchBounds[collation.getFieldCollations().size()]);
        boolean boundsEmpty = true;
        int prevComplexity = 1;

        List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
        for (int i = 0; i < fieldCollations.size(); i++) {
            RelFieldCollation fieldCollation = fieldCollations.get(i);
            int collFldIdx = fieldCollation.getFieldIndex();

            List<RexCall> collFldPreds = fieldsToPredicates.get(collFldIdx);

            if (nullOrEmpty(collFldPreds)) {
                break;
            }

            collFldIdx = mapping.getSourceOpt(collFldIdx);

            SearchBounds fldBounds = createBounds(fieldCollation, collFldPreds, cluster, types.get(collFldIdx), prevComplexity, allowRange);

            if (fldBounds == null) {
                break;
            }

            boundsEmpty = false;

            bounds.set(i, fldBounds);

            if (fldBounds instanceof MultiBounds) {
                prevComplexity *= ((MultiBounds) fldBounds).bounds().size();

                // Any bounds after multi range bounds are not allowed, since it can cause intervals intersection.
                if (((MultiBounds) fldBounds).bounds().stream().anyMatch(b -> b.type() != SearchBounds.Type.EXACT)) {
                    break;
                }
            }

            if (fldBounds.type() == SearchBounds.Type.RANGE) {
                break; // TODO https://issues.apache.org/jira/browse/IGNITE-13568
            }
        }

        return boundsEmpty ? null : bounds;
    }

    /** Create index search bound by conditions of the field. */
    private static @Nullable SearchBounds createBounds(
            RelFieldCollation fc,
            List<RexCall> collFldPreds,
            RelOptCluster cluster,
            RelDataType fldType,
            int prevComplexity,
            boolean allowRange
    ) {
        RexBuilder builder = builder(cluster);

        RexNode nullValue = builder.makeNullLiteral(fldType);

        RexNode upperCond = null;
        RexNode lowerCond = null;
        RexNode upperBound = null;
        RexNode lowerBound = null;
        boolean upperInclude = true;
        boolean lowerInclude = true;

        // Give priority to equality operators.
        collFldPreds.sort(Comparator.comparingInt(pred -> {
            switch (pred.getOperator().getKind()) {
                case IS_NULL:
                    return 0;
                case EQUALS:
                    return 1;
                case IS_NOT_DISTINCT_FROM:
                    return 2;
                default:
                    return 10;
            }
        }));

        for (RexCall pred : collFldPreds) {
            RexNode val = null;
            RexNode ref = pred.getOperands().get(0);

            if (isBinaryComparison(pred)) {
                val = pred.getOperands().get(1);
            }

            SqlOperator op = pred.getOperator();

            if (op.kind == EQUALS || op.kind == IS_NOT_DISTINCT_FROM) {
                assert val != null;

                return new ExactBounds(pred, val);
            } else if (op.kind == IS_NULL) {
                return new ExactBounds(pred, nullValue);
            } else if (op.kind == OR) {
                List<SearchBounds> orBounds = new ArrayList<>();
                int curComplexity = 0;

                for (RexNode operand : pred.getOperands()) {
                    SearchBounds opBounds = createBounds(fc, Collections.singletonList((RexCall) operand),
                            cluster, fldType, prevComplexity, allowRange);

                    if (opBounds instanceof MultiBounds) {
                        curComplexity += ((MultiBounds) opBounds).bounds().size();
                        orBounds.addAll(((MultiBounds) opBounds).bounds());
                    } else if (opBounds != null) {
                        curComplexity++;
                        orBounds.add(opBounds);
                    }

                    if (opBounds == null || curComplexity > MAX_SEARCH_BOUNDS_COMPLEXITY) {
                        orBounds = null;
                        break;
                    }
                }

                if (orBounds == null) {
                    continue;
                }

                return new MultiBounds(pred, orBounds);
            } else if (op.kind == SEARCH || op.kind == LIKE) {
                List<SearchBounds> bounds = null;
                if (op.kind == LIKE && allowRange) {
                    RangeBounds bound = expandLikeToBounds(fc, cluster, prevComplexity, pred);

                    if (bound != null) {
                        RexNode shouldComputeLower = bound.shouldComputeLower();
                        RexNode shouldComputeUpper = bound.shouldComputeUpper();
                        if ((shouldComputeLower != null && !shouldComputeLower.isAlwaysTrue())
                                || (shouldComputeUpper != null && !shouldComputeUpper.isAlwaysTrue())) {
                            // Don't try to merge conditional bounds. We might revise this later.
                            return bound;
                        }
                    }

                    bounds = bound == null ? null : List.of(bound);
                } else if (op.kind == SEARCH) {
                    Sarg<?> sarg = ((RexLiteral) pred.operands.get(1)).getValueAs(Sarg.class);

                    bounds = expandSargToBounds(fc, cluster, fldType, prevComplexity, sarg, ref, allowRange);
                }

                if (bounds == null) {
                    continue;
                }

                if (bounds.size() == 1) {
                    if (bounds.get(0) instanceof RangeBounds && collFldPreds.size() > 1) {
                        // Try to merge bounds.
                        boolean ascDir = !fc.getDirection().isDescending();
                        RangeBounds rangeBounds = (RangeBounds) bounds.get(0);
                        if (rangeBounds.lowerBound() != null) {
                            if (lowerBound != null && lowerBound != nullValue) {
                                lowerBound = leastOrGreatest(builder, !ascDir, lowerBound, rangeBounds.lowerBound());
                                lowerInclude |= rangeBounds.lowerInclude();
                            } else {
                                lowerBound = rangeBounds.lowerBound();
                                lowerInclude = rangeBounds.lowerInclude();
                            }
                            lowerCond = lessOrGreater(builder, !ascDir, lowerInclude, ref, lowerBound);
                        }

                        if (rangeBounds.upperBound() != null) {
                            if (upperBound != null && upperBound != nullValue) {
                                upperBound = leastOrGreatest(builder, ascDir, upperBound, rangeBounds.upperBound());
                                upperInclude |= rangeBounds.upperInclude();
                            } else {
                                upperBound = rangeBounds.upperBound();
                                upperInclude = rangeBounds.upperInclude();
                            }
                            upperCond = lessOrGreater(builder, ascDir, upperInclude, ref, upperBound);
                        }

                        continue;
                    } else {
                        return bounds.get(0);
                    }
                }

                return new MultiBounds(pred, bounds);
            }

            if (!allowRange) {
                return null;
            }

            // Range bounds.
            boolean lowerBoundBelow = !fc.getDirection().isDescending();
            boolean includeBound = op.kind == GREATER_THAN_OR_EQUAL || op.kind == LESS_THAN_OR_EQUAL;
            boolean lessCondition = false;

            switch (op.kind) {
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    lessCondition = true;
                    lowerBoundBelow = !lowerBoundBelow;
                    // fallthrough.

                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    if (lowerBoundBelow) {
                        if (lowerBound == null || lowerBound == nullValue) {
                            lowerCond = pred;
                            lowerBound = val;
                            lowerInclude = includeBound;
                        } else {
                            lowerBound = leastOrGreatest(builder, lessCondition, lowerBound, val);
                            lowerInclude |= includeBound;
                            lowerCond = lessOrGreater(builder, lessCondition, lowerInclude, ref, lowerBound);
                        }
                    } else {
                        if (upperBound == null || upperBound == nullValue) {
                            upperCond = pred;
                            upperBound = val;
                            upperInclude = includeBound;
                        } else {
                            upperBound = leastOrGreatest(builder, lessCondition, upperBound, val);
                            upperInclude |= includeBound;
                            upperCond = lessOrGreater(builder, lessCondition, upperInclude, ref, upperBound);
                        }
                    }
                    // fallthrough.

                case IS_NOT_NULL:
                    if (fc.nullDirection == FIRST && lowerBound == null) {
                        lowerCond = pred;
                        lowerBound = nullValue;
                        lowerInclude = false;
                    } else if (fc.nullDirection == LAST && upperBound == null) {
                        upperCond = pred;
                        upperBound = nullValue;
                        upperInclude = false;
                    }
                    break;

                default:
                    throw new AssertionError("Unknown condition: " + op.kind);
            }
        }

        if (lowerBound == null && upperBound == null) {
            return null; // No bounds.
        }

        if (!allowRange) {
            return null;
        }

        // Found upper bound, lower bound or both.
        RexNode cond = lowerCond == null
                ? upperCond
                : upperCond == null
                        ? lowerCond
                        : upperCond == lowerCond
                                ? lowerCond
                                : builder.makeCall(SqlStdOperatorTable.AND, lowerCond, upperCond);

        return new RangeBounds(cond, lowerBound, upperBound, lowerInclude, upperInclude);
    }

    private static @Nullable List<SearchBounds> expandSargToBounds(
            RelFieldCollation fc,
            RelOptCluster cluster,
            RelDataType fldType,
            int prevComplexity,
            Sarg<?> sarg,
            RexNode ref,
            boolean allowRange
    ) {
        int complexity = prevComplexity * sarg.complexity();

        // Limit amount of search bounds tuples.
        if (complexity > MAX_SEARCH_BOUNDS_COMPLEXITY) {
            return null;
        }

        RexBuilder builder = builder(cluster);

        RexNode sargCond = sargRef(builder, ref, sarg, fldType, RexUnknownAs.UNKNOWN)
                .accept(new OutOfRangeLiteralComparisonReductionShuttle(builder));

        List<RexNode> disjunctions = RelOptUtil.disjunctions(RexUtil.toDnf(builder, sargCond));
        List<SearchBounds> bounds = new ArrayList<>(disjunctions.size());

        for (RexNode bound : disjunctions) {
            List<RexNode> conjunctions = RelOptUtil.conjunctions(bound);
            List<RexCall> calls = new ArrayList<>(conjunctions.size());

            for (RexNode rexNode : conjunctions) {
                if (isSupportedTreeComparison(rexNode)) {
                    calls.add((RexCall) rexNode);
                } else { // Cannot filter using this predicate (NOT_EQUALS for example), give a chance to other predicates.
                    return null;
                }
            }

            SearchBounds searchBounds = createBounds(fc, calls, cluster, fldType, complexity, allowRange);

            if (searchBounds == null) {
                // The bounds do not completely cover the search predicate.
                return null;
            }

            bounds.add(searchBounds);
        }

        return bounds;
    }

    private static @Nullable RangeBounds expandLikeToBounds(
            RelFieldCollation fc,
            RelOptCluster cluster,
            int prevComplexity,
            RexCall call
    ) {
        if (!call.isA(LIKE)) {
            return null;
        }

        int complexity = prevComplexity + 1;

        // Limit amount of search bounds tuples.
        if (complexity > MAX_SEARCH_BOUNDS_COMPLEXITY) {
            return null;
        }

        RexBuilder builder = builder(cluster);
        RexNode expressionToTest = call.getOperands().get(0);
        RexNode pattern = call.getOperands().get(1);
        RexNode escape = call.getOperands().size() > 2 ? call.getOperands().get(2) : null;

        RexNode prefixExpression = findPrefix(builder, pattern, escape);
        RexNode nextGreaterPrefix = nextGreaterPrefix(builder, prefixExpression);

        RexNode literalTrue = builder.makeLiteral(true);

        // The `nextGreaterPrefix` may return null if current value is the greatest one,
        // so next is simply doesn't exists. Depending on field collation, a `null` value
        // may or may not be a valid bound.
        //
        // A `null` is valid iff `null` is highest value according to a collation. In other
        // words, if collation is `ASC NULLS LAST` or `DESC NULLS FIRST`, then any bound is
        // valid bound and no extra validation required.
        //
        // Otherwise, we have to make sure that resulting `nextGreaterPrefix` is valid bound.
        // If not, we should just leave this bound open. 
        RexNode shouldComputeBoundExpression = nullIsHighest(fc)
                ? literalTrue
                : builder.makeCall(
                        SqlStdOperatorTable.OR,
                        // Prefix is null, [null, null) is valid range. It's empty though,
                        // but `anything LIKE null` should evaluate to FALSE anyway.
                        builder.makeCall(SqlStdOperatorTable.IS_NULL, prefixExpression),
                        // Prefix is not null, hence nextPrefix must be not null as well.
                        builder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, nextGreaterPrefix)
                );

        RexNode condition = builder.makeCall(
                SqlStdOperatorTable.AND,
                lessOrGreater(builder, false, true, expressionToTest, prefixExpression),
                lessOrGreater(builder, true, false, expressionToTest, nextGreaterPrefix)
        );

        if (fc.getDirection().isDescending()) {
            return new RangeBounds(
                    condition,
                    shouldComputeBoundExpression,
                    nextGreaterPrefix,
                    false,
                    literalTrue,
                    prefixExpression,
                    true
            );
        }

        return new RangeBounds(
                condition,
                literalTrue,
                prefixExpression,
                true,
                shouldComputeBoundExpression,
                nextGreaterPrefix,
                false
        );
    }

    private static boolean nullIsHighest(RelFieldCollation collation) {
        return (collation.getDirection().lax() == ASCENDING && collation.nullDirection == LAST)
                || (collation.getDirection().lax() == DESCENDING && collation.nullDirection == FIRST);
    }

    private static RexNode findPrefix(RexBuilder builder, RexNode pattern, @Nullable RexNode escape) {
        return builder.makeCall(
                IgniteSqlOperatorTable.FIND_PREFIX,
                pattern,
                escape == null ? builder.makeNullLiteral(pattern.getType()) : escape
        );
    }

    private static RexNode nextGreaterPrefix(RexBuilder builder, RexNode string) {
        return builder.makeCall(
                IgniteSqlOperatorTable.NEXT_GREATER_PREFIX,
                string
        );
    }

    private static RexNode leastOrGreatest(RexBuilder builder, boolean least, RexNode arg0, RexNode arg1) {
        return builder.makeCall(
                least ? IgniteSqlOperatorTable.LEAST2 : IgniteSqlOperatorTable.GREATEST2,
                arg0,
                arg1
        );
    }

    private static RexNode lessOrGreater(
            RexBuilder builder,
            boolean less,
            boolean includeBound,
            RexNode arg0,
            RexNode arg1
    ) {
        return builder.makeCall(less
                        ? (includeBound ? SqlStdOperatorTable.LESS_THAN_OR_EQUAL : SqlStdOperatorTable.LESS_THAN)
                        : (includeBound ? SqlStdOperatorTable.GREATER_THAN_OR_EQUAL : SqlStdOperatorTable.GREATER_THAN),
                arg0, arg1);
    }

    private static Int2ObjectMap<List<RexCall>> mapPredicatesToFields(RexNode condition, RelOptCluster cluster) {
        List<RexNode> conjunctions = RelOptUtil.conjunctions(condition);

        if (conjunctions.isEmpty()) {
            return Int2ObjectMaps.emptyMap();
        }

        Int2ObjectMap<List<RexCall>> res = new Int2ObjectOpenHashMap<>(conjunctions.size());

        for (RexNode rexNode : conjunctions) {
            Pair<Integer, RexCall> refPredicate = null;

            if (rexNode instanceof RexCall && rexNode.getKind() == OR) {
                List<RexNode> operands = ((RexCall) rexNode).getOperands();

                Integer ref = null;
                List<RexCall> preds = new ArrayList<>(operands.size());

                for (RexNode operand : operands) {
                    Pair<Integer, RexCall> operandRefPredicate = extractRefPredicate(operand, cluster);

                    // Skip the whole OR condition if any operand does not support tree comparison or not on reference.
                    if (operandRefPredicate == null) {
                        ref = null;
                        break;
                    }

                    // Ensure that we have the same field reference in all operands.
                    if (ref == null) {
                        ref = operandRefPredicate.getKey();
                    } else if (!ref.equals(operandRefPredicate.getKey())) {
                        ref = null;
                        break;
                    }

                    // For correlated variables it's required to resort and merge ranges on each nested loop,
                    // don't support it now.
                    if (containsFieldAccess(operandRefPredicate.getValue())) {
                        ref = null;
                        break;
                    }

                    preds.add(operandRefPredicate.getValue());
                }

                if (ref != null) {
                    refPredicate = Pair.of(ref, (RexCall) builder(cluster).makeCall(((RexCall) rexNode).getOperator(), preds));
                }
            } else {
                refPredicate = extractRefPredicate(rexNode, cluster);
            }

            if (refPredicate != null) {
                List<RexCall> fldPreds = res.computeIfAbsent(refPredicate.getKey(), k -> new ArrayList<>(conjunctions.size()));

                fldPreds.add(refPredicate.getValue());
            }
        }
        return res;
    }

    private static @Nullable Pair<Integer, RexCall> extractRefPredicate(RexNode rexNode, RelOptCluster cluster) {
        rexNode = expandBooleanFieldComparison(rexNode, builder(cluster));

        if (!isSupportedTreeComparison(rexNode)) {
            return null;
        }

        RexCall predCall = (RexCall) rexNode;
        RexSlot ref;

        if (isBinaryComparison(rexNode)) {
            ref = extractRefFromBinary(predCall);

            if (ref == null) {
                return null;
            }

            // Let RexLocalRef be on the left side.
            if (refOnTheRight(predCall)) {
                predCall = (RexCall) invert(builder(cluster), predCall);
            }
        } else {
            ref = extractRefFromOperand(predCall, 0);

            if (ref == null) {
                return null;
            }
        }

        return Pair.of(ref.getIndex(), predCall);
    }

    private static Boolean containsFieldAccess(RexNode node) {
        RexVisitor<Void> v = new RexVisitorImpl<>(true) {
            @Override
            public Void visitFieldAccess(RexFieldAccess fieldAccess) {
                throw Util.FoundOne.NULL;
            }
        };

        try {
            node.accept(v);

            return false;
        } catch (Util.FoundOne e) {
            return true;
        }
    }

    /** Extended version of {@link RexUtil#invert(RexBuilder, RexCall)} with additional operators support. */
    private static RexNode invert(RexBuilder rexBuilder, RexCall call) {
        if (call.getOperator() == SqlStdOperatorTable.IS_NOT_DISTINCT_FROM) {
            return rexBuilder.makeCall(call.getOperator(), call.getOperands().get(1), call.getOperands().get(0));
        } else {
            return RexUtil.invert(rexBuilder, call);
        }
    }

    /**
     * Rewrites a simplified boolean condition so that it can be used to set index scan bounds.
     *
     * <p>For example, Calcite simplifies the condition '{@code AND boolField <> TRUE}' to '{@code NOT($boolField)}',
     * but to perform an index lookup we need to rewrite it back to {@code $boolField = FALSE}.
     *
     * @param rexNode Original row expression.
     * @param builder Row expression builder.
     * @return Rewritten row expression.
     */
    private static RexNode expandBooleanFieldComparison(RexNode rexNode, RexBuilder builder) {
        if (rexNode instanceof RexSlot) {
            return builder.makeCall(SqlStdOperatorTable.EQUALS, rexNode, builder.makeLiteral(true));
        } else if (rexNode instanceof RexCall && rexNode.getKind() == NOT
                && ((RexCall) rexNode).getOperands().get(0) instanceof RexSlot) {
            return builder.makeCall(SqlStdOperatorTable.EQUALS, ((RexCall) rexNode).getOperands().get(0),
                    builder.makeLiteral(false));
        }

        return rexNode;
    }

    private static @Nullable RexSlot extractRefFromBinary(RexCall call) {
        assert isBinaryComparison(call) : "Unsupported RexNode is binary comparison: " + call;

        RexSlot leftRef = extractRefFromOperand(call, 0);
        RexNode rightOp = call.getOperands().get(1);

        if (leftRef != null) {
            return idxOpSupports(removeCast(rightOp)) ? leftRef : null;
        }

        RexSlot rightRef = extractRefFromOperand(call, 1);
        RexNode leftOp = call.getOperands().get(0);

        if (rightRef != null) {
            return idxOpSupports(removeCast(leftOp)) ? rightRef : null;
        }

        return null;
    }

    private static @Nullable RexSlot extractRefFromOperand(RexCall call, int operandNum) {
        assert isSupportedTreeComparison(call) : "Unsupported RexNode is tree comparison: " + call;

        RexNode op = call.getOperands().get(operandNum);
        RelDataType operandType = op.getType();

        while (isLosslessCast(op)) {
            op = ((RexCall) op).getOperands().get(0);
        }

        // We cannot compose search condition if expression requires to be downcasted in order to be put
        // in bound. Downcast is not safe, and may throw `Out of range` error. As of now, such case
        // must be handled by user explicitly by manually CASTing to the required type.
        if (TypeUtils.needCastInSearchBounds(Commons.typeFactory(), operandType, op.getType())) {
            return null;
        }

        if (op instanceof RexSlot) {
            if (!TypeUtils.needCastInSearchBounds(Commons.typeFactory(), op.getType(), operandType)) {
                return (RexSlot) op;
            }
        }

        return null;
    }

    private static boolean refOnTheRight(RexCall predCall) {
        RexNode rightOp = predCall.getOperands().get(1);

        rightOp = removeCast(rightOp);

        return rightOp.isA(SqlKind.LOCAL_REF) || rightOp.isA(SqlKind.INPUT_REF);
    }

    /** Checks if given {@link RexNode} represents binary comparison. */
    public static boolean isBinaryComparison(RexNode exp) {
        return BINARY_COMPARISON.contains(exp.getKind())
                && (exp instanceof RexCall)
                && ((RexCall) exp).getOperands().size() == 2;
    }

    private static boolean isSupportedTreeComparison(RexNode exp) {
        return TREE_INDEX_COMPARISON.contains(exp.getKind())
                && (exp instanceof RexCall);
    }

    private static boolean idxOpSupports(RexNode op) {
        return op instanceof RexLiteral
                || op instanceof RexDynamicParam
                || op instanceof RexFieldAccess
                || !containsRef(op);
    }

    /**
     * IsNotNull.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static boolean isNotNull(RexNode op) {
        if (op == null) {
            return false;
        }

        return !(op instanceof RexLiteral) || !((RexLiteral) op).isNull();
    }

    /**
     * InversePermutation.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static Mappings.TargetMapping inversePermutation(List<RexNode> nodes, RelDataType inputRowType, boolean local) {
        final Mappings.TargetMapping mapping =
                Mappings.create(MappingType.INVERSE_FUNCTION, nodes.size(), inputRowType.getFieldCount());

        Class<? extends RexSlot> clazz = local ? RexLocalRef.class : RexInputRef.class;

        for (Ord<RexNode> node : Ord.zip(nodes)) {
            if (clazz.isInstance(node.e)) {
                mapping.set(node.i, ((RexSlot) node.e).getIndex());
            }
        }
        return mapping;
    }

    /**
     * ReplaceInputRefs.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static List<RexNode> replaceInputRefs(List<RexNode> nodes) {
        return InputRefReplacer.INSTANCE.apply(nodes);
    }

    /**
     * ReplaceInputRefs.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static RexNode replaceInputRefs(RexNode node) {
        return InputRefReplacer.INSTANCE.apply(node);
    }

    /** Replaces all occurrences of {@link RexLocalRef} within given list of nodes with {@link RexInputRef}. */
    public static RexNode replaceLocalRefs(@Nullable RexNode node) {
        //noinspection DataFlowIssue: RexShuttle expects null as input.
        return LocalRefReplacer.INSTANCE.apply(node);
    }

    /** Replaces all occurrences of {@link RexLocalRef} within given rex node with {@link RexInputRef}. */
    public static List<RexNode> replaceLocalRefs(List<RexNode> nodes) {
        return LocalRefReplacer.INSTANCE.apply(nodes);
    }

    /**
     * Set hasCorrelation flag.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static boolean hasCorrelation(RexNode node) {
        return hasCorrelation(Collections.singletonList(node));
    }

    /**
     * Get hasCorrelation flag.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static boolean hasCorrelation(List<RexNode> nodes) {
        try {
            RexVisitor<Void> v = new RexVisitorImpl<>(true) {
                @Override
                public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
                    throw new ControlFlowException();
                }
            };

            nodes.forEach(n -> n.accept(v));

            return false;
        } catch (ControlFlowException e) {
            return true;
        }
    }

    /**
     * ExtractCorrelationIds.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static Set<CorrelationId> extractCorrelationIds(RexNode node) {
        if (node == null) {
            return Collections.emptySet();
        }

        return extractCorrelationIds(Collections.singletonList(node));
    }

    /**
     * ExtractCorrelationIds.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static Set<CorrelationId> extractCorrelationIds(List<RexNode> nodes) {
        final Set<CorrelationId> cors = new HashSet<>();

        RexVisitor<Void> v = new RexVisitorImpl<>(true) {
            @Override
            public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
                cors.add(correlVariable.id);

                return null;
            }
        };

        nodes.forEach(rex -> rex.accept(v));

        return cors;
    }

    /**
     * Double value of the literal expression.
     *
     * @return Double value of the literal expression.
     */
    public static double doubleFromRex(RexNode n, double def) {
        try {
            if (n.isA(SqlKind.LITERAL)) {
                return ((RexLiteral) n).getValueAs(Integer.class);
            } else {
                return def;
            }
        } catch (Exception e) {
            assert false : "Unable to extract value: " + e.getMessage();

            return def;
        }
    }

    /**
     * NotNullKeys.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static IntSet notNullKeys(List<RexNode> row) {
        if (nullOrEmpty(row)) {
            return IntSets.EMPTY_SET;
        }

        IntSet keys = new IntOpenHashSet();

        for (int i = 0; i < row.size(); ++i) {
            if (isNotNull(row.get(i))) {
                keys.add(i);
            }
        }

        return keys;
    }

    private static boolean containsRef(RexNode node) {
        RexVisitor<Void> v = new RexVisitorImpl<>(true) {
            @Override
            public Void visitInputRef(RexInputRef inputRef) {
                throw Util.FoundOne.NULL;
            }

            @Override
            public Void visitLocalRef(RexLocalRef locRef) {
                throw Util.FoundOne.NULL;
            }
        };

        try {
            node.accept(v);

            return false;
        } catch (Util.FoundOne e) {
            return true;
        }
    }

    /**
     * Extracts the value of a literal and converts it to the specified type, 
     * optionally adjusting for time zone offsets when dealing with 
     * {@code TIMESTAMP_WITH_LOCAL_TIME_ZONE} literals.
     *
     * <p>This method processes a {@link RexLiteral} based on its SQL type, 
     * extracting its value and converting it to the specified target type if needed.
     * For numeric types, the literal value is converted according to the specified target type. 
     * For {@code TIMESTAMP_WITH_LOCAL_TIME_ZONE} types, the value is adjusted 
     * using the time zone provided in the {@link ExecutionContext}.
     *
     * @param context The execution context, used to retrieve the client's time zone.
     * @param literal The literal to extract the value from.
     * @param type The target type for conversion.
     * @return The converted literal value, or {@code null} if the literal value is {@code null}.
     */
    public static @Nullable Object literalValue(DataContext context, RexLiteral literal, Class<?> type) {
        RelDataType dataType = literal.getType();

        if (literal.isNull()) {
            return null;
        }

        if (SqlTypeUtil.isNumeric(dataType)) {
            Number value = (Number) literal.getValue();

            return convertNumericLiteral(dataType, value, type);
        }

        Object val = literal.getValueAs(type);

        // Literal was parsed as UTC timestamp, now we need to adjust it to the client's time zone.
        if (literal.getTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            return IgniteSqlDateTimeUtils.subtractTimeZoneOffset((long) val, (TimeZone) context.get(Variable.TIME_ZONE.camelName));
        }

        return val;
    }

    /** Context which triggers assertion if unexpected method is called. */
    public static class FaultyContext implements DataContext {
        public static final FaultyContext INSTANCE = new FaultyContext();

        /** {@inheritDoc} */
        @Override
        public SchemaPlus getRootSchema() {
            throw new AssertionError("should not be called");
        }

        /** {@inheritDoc} */
        @Override
        public JavaTypeFactory getTypeFactory() {
            throw new AssertionError("should not be called");
        }

        /** {@inheritDoc} */
        @Override
        public QueryProvider getQueryProvider() {
            throw new AssertionError("should not be called");
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable Object get(String name) {
            throw new AssertionError("Should not call: [" + name + "] from current context.");
        }
    }

    private static Object convertNumericLiteral(RelDataType dataType, Number value, Class<?> type) {
        Primitive primitive = Primitive.ofBoxOr(type);
        assert primitive != null || type == BigDecimal.class : "Neither primitive nor BigDecimal: " + type;

        switch (dataType.getSqlTypeName()) {
            case TINYINT:
                byte b = IgniteMath.convertToByteExact(value);
                if (primitive != null) {
                    return Primitives.convertPrimitiveExact(primitive, b);
                } else {
                    return new BigDecimal(b);
                }
            case SMALLINT:
                short s = IgniteMath.convertToShortExact(value);
                if (primitive != null) {
                    return Primitives.convertPrimitiveExact(primitive, s);
                } else {
                    return new BigDecimal(s);
                }
            case INTEGER:
                int i = IgniteMath.convertToIntExact(value);
                if (primitive != null) {
                    return Primitives.convertPrimitiveExact(primitive, i);
                } else {
                    return new BigDecimal(i);
                }
            case BIGINT:
                long l = IgniteMath.convertToLongExact(value);
                if (primitive != null) {
                    return Primitives.convertPrimitiveExact(primitive, l);
                } else {
                    return new BigDecimal(l);
                }
            case REAL:
            case FLOAT:
                float r = IgniteMath.convertToFloatExact(value);
                if (primitive != null) {
                    return Primitives.convertPrimitiveExact(primitive, r);
                } else {
                    // Preserve the exact form of a float value.
                    return new BigDecimal(Float.toString(r));
                }
            case DOUBLE:
                double d = IgniteMath.convertToDoubleExact(value);
                if (primitive != null) {
                    return Primitives.convertPrimitiveExact(primitive, d);
                } else {
                    // Preserve the exact form of a double value.
                    return new BigDecimal(Double.toString(d));
                }
            case DECIMAL:
                BigDecimal bd = IgniteSqlFunctions.toBigDecimal(value, dataType.getPrecision(), dataType.getScale());
                assert bd != null;

                if (primitive != null) {
                    return Primitives.convertPrimitiveExact(primitive, bd);
                } else {
                    return bd;
                }
            default:
                throw new IllegalStateException("Unexpected numeric type: " + dataType);
        }
    }

    /** Visitor for replacing scan local refs to input refs. */
    private static class LocalRefReplacer extends RexShuttle {
        private static final RexShuttle INSTANCE = new LocalRefReplacer();

        /** {@inheritDoc} */
        @Override
        public RexNode visitLocalRef(RexLocalRef inputRef) {
            return new RexInputRef(inputRef.getIndex(), inputRef.getType());
        }
    }

    /** Visitor for replacing input refs to local refs. We need it for proper plan serialization. */
    private static class InputRefReplacer extends RexShuttle {
        private static final RexShuttle INSTANCE = new InputRefReplacer();

        /** {@inheritDoc} */
        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            return new RexLocalRef(inputRef.getIndex(), inputRef.getType());
        }
    }

    /** Applies a {@link RexShuttle} to a list of {@link SearchBounds}. */
    public static @Nullable List<SearchBounds> processSearchBounds(RexShuttle shuttle, @Nullable List<SearchBounds> searchBounds) {
        if (nullOrEmpty(searchBounds)) {
            return searchBounds;
        }

        List<SearchBounds> newSearchBounds = new ArrayList<>(searchBounds.size());

        boolean wasChanged = false;
        for (SearchBounds bound : searchBounds) {
            SearchBounds newBound = bound == null ? null : bound.accept(shuttle);
            newSearchBounds.add(newBound);

            wasChanged = wasChanged || newBound != bound;
        }

        return wasChanged ? newSearchBounds : searchBounds;
    }

    /**
     * Check if given {@link RexNode} is a 'loss-less' cast, that is, a cast from which the original value of the field can be certainly
     * recovered.
     */
    public static boolean isLosslessCast(RexNode node) {
        if (!node.isA(SqlKind.CAST)) {
            return false;
        }

        RelDataType source = ((RexCall) node).getOperands().get(0).getType();
        RelDataType target = node.getType();

        if (source.getFamily() != target.getFamily()) {
            return false;
        }

        if (source.getSqlTypeName() == target.getSqlTypeName() && SqlTypeUtil.isDatetime(source)) {
            return source.getPrecision() <= target.getPrecision();
        }

        if (SqlTypeFamily.CHARACTER.getTypeNames().contains(source.getSqlTypeName())
                && SqlTypeFamily.CHARACTER.getTypeNames().contains(target.getSqlTypeName())) {
            return target.getSqlTypeName().compareTo(source.getSqlTypeName()) >= 0
                    && (source.getPrecision() <= target.getPrecision() || target.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED);
        }

        return RexUtil.isLosslessCast(source, target);
    }
}
