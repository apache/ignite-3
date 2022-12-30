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
import static org.apache.calcite.sql.SqlKind.SEARCH;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
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
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.sql.engine.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * RexUtils.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class RexUtils {
    /** Maximum amount of search bounds tuples per scan. */
    public static final int MAX_SEARCH_BOUNDS_COMPLEXITY = 100;

    /**
     * MakeCast.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static RexNode makeCast(RexBuilder builder, RexNode node, RelDataType type) {
        return TypeUtils.needCast(builder.getTypeFactory(), node.getType(), type) ? builder.makeCast(type, node) : node;
    }

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

    /**
     * Simplifier.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static RexSimplify simplifier(RelOptCluster cluster) {
        return new RexSimplify(builder(cluster), RelOptPredicateList.EMPTY, executor(cluster));
    }

    /**
     * MakeCase.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static RexNode makeCase(RexBuilder builder, RexNode... operands) {
        if (IgniteUtils.assertionsEnabled()) {
            // each odd operand except last one has to return a boolean type
            for (int i = 0; i < operands.length; i += 2) {
                if (operands[i].getType().getSqlTypeName() != SqlTypeName.BOOLEAN && i < operands.length - 1) {
                    throw new AssertionError("Unexpected operand type. [operands=" + Arrays.toString(operands) + "]");
                }
            }
        }

        return builder.makeCall(SqlStdOperatorTable.CASE, operands);
    }

    /** Returns whether a list of expressions projects the incoming fields. */
    public static boolean isIdentity(List<? extends RexNode> projects, RelDataType inputRowType) {
        return isIdentity(projects, inputRowType, false);
    }

    /** Returns whether a list of expressions projects the incoming fields. */
    public static boolean isIdentity(List<? extends RexNode> projects, RelDataType inputRowType, boolean local) {
        if (inputRowType.getFieldCount() != projects.size()) {
            return false;
        }

        final List<RelDataTypeField> fields = inputRowType.getFieldList();
        Class<? extends RexSlot> clazz = local ? RexLocalRef.class : RexInputRef.class;

        for (int i = 0; i < fields.size(); i++) {
            if (!clazz.isInstance(projects.get(i))) {
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

    /** Supported index operations. */
    private static final Set<SqlKind> TREE_INDEX_COMPARISON =
            EnumSet.of(
                    SEARCH,
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
            @Nullable ImmutableBitSet requiredColumns
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

        Mappings.TargetMapping mapping = null;

        if (requiredColumns != null) {
            mapping = Commons.inverseTrimmingMapping(types.size(), requiredColumns);
        }

        List<SearchBounds> bounds = Arrays.asList(new SearchBounds[collation.getFieldCollations().size()]);
        boolean boundsEmpty = true;
        int prevComplexity = 1;

        List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
        for (int i = 0; i < fieldCollations.size(); i++) {
            RelFieldCollation fc = fieldCollations.get(i);
            int collFldIdx = fc.getFieldIndex();

            List<RexCall> collFldPreds = fieldsToPredicates.get(collFldIdx);

            if (nullOrEmpty(collFldPreds)) {
                break;
            }

            if (mapping != null) {
                collFldIdx = mapping.getSourceOpt(collFldIdx);
            }

            SearchBounds fldBounds = createBounds(fc, collFldPreds, cluster, types.get(collFldIdx), prevComplexity);

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

    /**
     * Builds hash index search bounds.
     */
    public static List<SearchBounds> buildHashSearchBounds(
            RelOptCluster cluster,
            RelCollation collation,
            RexNode condition,
            RelDataType rowType,
            @Nullable ImmutableBitSet requiredColumns
    ) {
        if (condition == null) {
            return null;
        }

        condition = RexUtil.toCnf(builder(cluster), condition);

        Int2ObjectMap<List<RexCall>> fieldsToPredicates = mapPredicatesToFields(condition, cluster);

        if (nullOrEmpty(fieldsToPredicates)) {
            return null;
        }

        List<RelDataType> types = RelOptUtil.getFieldTypeList(rowType);

        List<SearchBounds> bounds = Arrays.asList(new SearchBounds[collation.getFieldCollations().size()]);

        Mappings.TargetMapping toTrimmedRowMapping = null;
        if (requiredColumns != null) {
            toTrimmedRowMapping = Commons.inverseTrimmingMapping(types.size(), requiredColumns);
        }

        List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
        for (int i = 0; i < fieldCollations.size(); i++) {
            int collFldIdx = fieldCollations.get(i).getFieldIndex();

            List<RexCall> collFldPreds = fieldsToPredicates.get(collFldIdx);

            if (nullOrEmpty(collFldPreds)) {
                return null; // Partial condition implies index scan, which is not supported.
            }

            RexCall columnPred = collFldPreds.stream()
                    .filter(pred -> pred.getOperator().getKind() == EQUALS)
                    .findAny().orElse(null);

            if (columnPred == null) {
                return null; // Non-equality conditions are not expected.
            }

            if (toTrimmedRowMapping != null) {
                collFldIdx = toTrimmedRowMapping.getSourceOpt(collFldIdx);
            }

            bounds.set(i, createBounds(null, Collections.singletonList(columnPred), cluster, types.get(collFldIdx), 1));
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
            mapping = Commons.inverseTrimmingMapping(types.size(), requiredColumns);
        }

        for (Entry<List<RexCall>> fld : fieldsToPredicates.int2ObjectEntrySet()) {
            List<RexCall> collFldPreds = fld.getValue();

            if (nullOrEmpty(collFldPreds)) {
                break;
            }

            for (RexCall pred : collFldPreds) {
                if (!pred.isA(List.of(EQUALS, IS_NOT_DISTINCT_FROM))) {
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

                bounds.set(fldIdx, new ExactBounds(pred,
                        makeCast(builder(cluster), removeCast(pred.operands.get(1)), types.get(fldIdx))));
            }
        }

        return bounds;
    }

    /** Create index search bound by conditions of the field. */
    private static @Nullable SearchBounds createBounds(
            @Nullable RelFieldCollation fc, // Can be null for EQUALS condition.
            List<RexCall> collFldPreds,
            RelOptCluster cluster,
            RelDataType fldType,
            int prevComplexity
    ) {
        RexBuilder builder = builder(cluster);

        RexNode nullVal = builder.makeNullLiteral(fldType);

        RexNode upperCond = null;
        RexNode lowerCond = null;
        RexNode upperBound = null;
        RexNode lowerBound = null;
        boolean upperInclude = true;
        boolean lowerInclude = true;

        for (RexCall pred : collFldPreds) {
            RexNode val = null;

            if (isBinaryComparison(pred)) {
                val = removeCast(pred.operands.get(1));

                assert idxOpSupports(val) : val;

                val = makeCast(builder, val, fldType);
            }

            SqlOperator op = pred.getOperator();

            if (op.kind == EQUALS) {
                return new ExactBounds(pred, val);
            } else if (op.kind == IS_NOT_DISTINCT_FROM) {
                return new ExactBounds(pred, builder.makeCall(SqlStdOperatorTable.COALESCE, val, nullVal));
            } else if (op.kind == IS_NULL) {
                return new ExactBounds(pred, nullVal);
            } else if (op.kind == SEARCH) {
                Sarg<?> sarg = ((RexLiteral) pred.operands.get(1)).getValueAs(Sarg.class);

                int complexity = prevComplexity * sarg.complexity();

                // Limit amount of search bounds tuples.
                if (complexity > MAX_SEARCH_BOUNDS_COMPLEXITY) {
                    return null;
                }

                RexNode sargCond = sargRef(builder, pred.operands.get(0), sarg, fldType, RexUnknownAs.UNKNOWN);
                List<RexNode> disjunctions = RelOptUtil.disjunctions(RexUtil.toDnf(builder, sargCond));
                List<SearchBounds> bounds = new ArrayList<>(disjunctions.size());

                for (RexNode bound : disjunctions) {
                    List<RexNode> conjunctions = RelOptUtil.conjunctions(bound);
                    List<RexCall> calls = new ArrayList<>(conjunctions.size());

                    for (RexNode rexNode : conjunctions) {
                        if (isSupportedTreeComparison(rexNode)) {
                            calls.add((RexCall) rexNode);
                        } else {
                            return null; // Cannot filter using this predicate (NOT_EQUALS for example).
                        }
                    }

                    bounds.add(createBounds(fc, calls, cluster, fldType, complexity));
                }

                if (bounds.size() == 1) {
                    return bounds.get(0);
                }

                return new MultiBounds(pred, bounds);
            }

            // Range bounds.
            boolean lowerBoundBelow = !fc.getDirection().isDescending();
            switch (op.kind) {
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    lowerBoundBelow = !lowerBoundBelow;
                    // fallthrough.

                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    if (lowerBoundBelow) {
                        lowerCond = pred;
                        lowerBound = val;

                        if (op.kind == GREATER_THAN || op.kind == LESS_THAN) {
                            lowerInclude = false;
                        }
                    } else {
                        upperCond = pred;
                        upperBound = val;

                        if (op.kind == GREATER_THAN || op.kind == LESS_THAN) {
                            upperInclude = false;
                        }
                    }
                    // fallthrough.

                case IS_NOT_NULL:
                    if (fc.nullDirection == RelFieldCollation.NullDirection.FIRST && lowerBound == null) {
                        lowerCond = pred;
                        lowerBound = nullVal;
                        lowerInclude = false;
                    } else if (fc.nullDirection == RelFieldCollation.NullDirection.LAST && upperBound == null) {
                        upperCond = pred;
                        upperBound = nullVal;
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

        // Found upper bound, lower bound or both.
        RexNode cond = lowerCond == null ? upperCond :
                upperCond == null ? lowerCond :
                        upperCond == lowerCond ? lowerCond : builder.makeCall(SqlStdOperatorTable.AND, lowerCond, upperCond);

        return new RangeBounds(cond, lowerBound, upperBound, lowerInclude, upperInclude);
    }

    private static Int2ObjectMap<List<RexCall>> mapPredicatesToFields(RexNode condition, RelOptCluster cluster) {
        List<RexNode> conjunctions = RelOptUtil.conjunctions(condition);

        if (conjunctions.isEmpty()) {
            return Int2ObjectMaps.emptyMap();
        }

        Int2ObjectMap<List<RexCall>> res = new Int2ObjectOpenHashMap<>(conjunctions.size());

        for (RexNode rexNode : conjunctions) {
            if (!isSupportedTreeComparison(rexNode)) {
                continue;
            }

            RexCall predCall = (RexCall) rexNode;
            RexSlot ref;

            if (isBinaryComparison(rexNode)) {
                ref = (RexSlot) extractRefFromBinary(predCall, cluster);

                if (ref == null) {
                    continue;
                }

                // Let RexLocalRef be on the left side.
                if (refOnTheRight(predCall)) {
                    predCall = (RexCall) invert(builder(cluster), predCall);
                }
            } else {
                ref = (RexSlot) extractRefFromOperand(predCall, cluster, 0);

                if (ref == null) {
                    continue;
                }
            }

            List<RexCall> fldPreds = res.computeIfAbsent(ref.getIndex(), k -> new ArrayList<>(conjunctions.size()));

            fldPreds.add(predCall);
        }

        return res;
    }

    /** Extended version of {@link RexUtil#invert(RexBuilder, RexCall)} with additional operators support. */
    private static RexNode invert(RexBuilder rexBuilder, RexCall call) {
        if (call.getOperator() == SqlStdOperatorTable.IS_NOT_DISTINCT_FROM) {
            return rexBuilder.makeCall(call.getOperator(), call.getOperands().get(1), call.getOperands().get(0));
        } else {
            return RexUtil.invert(rexBuilder, call);
        }
    }

    private static RexNode extractRefFromBinary(RexCall call, RelOptCluster cluster) {
        assert isBinaryComparison(call);

        RexNode leftRef = extractRefFromOperand(call, cluster, 0);
        RexNode rightOp = call.getOperands().get(1);

        if (leftRef != null) {
            return idxOpSupports(removeCast(rightOp)) ? leftRef : null;
        }

        RexNode rightRef = extractRefFromOperand(call, cluster, 1);
        RexNode leftOp = call.getOperands().get(0);

        if (rightRef != null) {
            return idxOpSupports(removeCast(leftOp)) ? rightRef : null;
        }

        return null;
    }

    private static RexNode extractRefFromOperand(RexCall call, RelOptCluster cluster, int operandNum) {
        assert isSupportedTreeComparison(call);

        RexNode op = call.getOperands().get(operandNum);

        op = removeCast(op);

        // Can proceed without ref cast only if cast was redundant in terms of values comparison.
        if ((op instanceof RexSlot)
                && !TypeUtils.needCast(cluster.getTypeFactory(), op.getType(), call.getOperands().get(operandNum).getType())) {
            return op;
        }

        return null;
    }

    private static boolean refOnTheRight(RexCall predCall) {
        RexNode rightOp = predCall.getOperands().get(1);

        rightOp = RexUtil.removeCast(rightOp);

        return rightOp.isA(SqlKind.LOCAL_REF) || rightOp.isA(SqlKind.INPUT_REF);
    }

    private static boolean isBinaryComparison(RexNode exp) {
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
                || op instanceof RexFieldAccess;
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
     * AsBound.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Deprecated(forRemoval = true)
    public static List<RexNode> asBound(RelOptCluster cluster, Iterable<RexNode> idxCond, RelDataType rowType,
            @Nullable Mappings.TargetMapping mapping) {
        if (nullOrEmpty(idxCond)) {
            return null;
        }

        RexBuilder builder = builder(cluster);
        List<RelDataType> types = RelOptUtil.getFieldTypeList(rowType);
        List<RexNode> res = Arrays.asList(new RexNode[types.size()]);

        for (RexNode pred : idxCond) {
            assert pred instanceof RexCall;

            RexCall call = (RexCall) pred;
            RexSlot ref = (RexSlot) RexUtil.removeCast(call.operands.get(0));
            RexNode cond = RexUtil.removeCast(call.operands.get(1));

            assert idxOpSupports(cond) : cond;

            int index = mapping == null ? ref.getIndex() : mapping.getSourceOpt(ref.getIndex());

            assert index != -1;

            res.set(index, makeCast(builder, cond, types.get(index)));
        }

        return res;
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

    /**
     * ReplaceLocalRefs.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static RexNode replaceLocalRefs(RexNode node) {
        return LocalRefReplacer.INSTANCE.apply(node);
    }

    /**
     * ReplaceLocalRefs.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
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
            RexVisitor<Void> v = new RexVisitorImpl<Void>(true) {
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

        RexVisitor<Void> v = new RexVisitorImpl<Void>(true) {
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
}
