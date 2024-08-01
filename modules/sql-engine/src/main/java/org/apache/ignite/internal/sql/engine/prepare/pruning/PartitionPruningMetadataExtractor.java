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

import static org.apache.calcite.rel.core.TableModify.Operation.INSERT;
import static org.apache.ignite.internal.sql.engine.util.RexUtils.replaceInputRefs;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import it.unimi.dsi.fastutil.ints.Int2IntArrayMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap.Entry;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.mapping.Mappings.TargetMapping;
import org.apache.ignite.internal.sql.engine.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTrimExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteUnionAll;
import org.apache.ignite.internal.sql.engine.rel.IgniteValues;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Extracts partition pruning metadata from a physical plan. Examples:
 * <pre>
 *    SELECT * FROM t WHERE pk = 10
 *    =>
 *    t = [ [pk=10] ]
 *
 *    SELECT * FROM t WHERE pk = ?
 *    =>
 *    t = [ [pk = ?0] ]
 *
 *    SELECT * FROM t WHERE pk = 10 AND col = 42
 *    t = [ [pk=10] ]
 *
 *    SELECT * FROM t WHERE pk = 10 OR pk = 42
 *    =>
 *    t = [ [pk=10], [pk=42] ]
 *
 *    SELECT * FROM t WHERE pk = 10 OR col = 42
 *    =>
 *    []
 *
 *    SELECT * FROM t WHERE colo_key1 = 10 AND colo_key2 = 20
 *    =>
 *    t = [ [colo_key1=10], [colo_key2=20] ]
 *
 *    SELECT * FROM t WHERE colo_key1 = 10 AND colo_key2 = 20 OR colo_key1 = 40 AND colo_key2 = 30
 *    =>
 *    t = [ [colo_key1=10, colo_key2=20], [colo_key1=20, colo_key2=30] ]
 * </pre>
 */
public class PartitionPruningMetadataExtractor extends IgniteRelShuttle {

    private final Long2ObjectMap<PartitionPruningColumns> result = new Long2ObjectOpenHashMap<>();

    /**
     * Extracts partition pruning metadata from the given physical plan.
     * This method traverses a physical plan and attempts to extract metadata from operators that support it
     * and includes such metadata is that complete (e.g. values of all colocation keys present in a scan predicate).
     *
     * @param rel Physical plan.
     * @return Partition pruning metadata.
     */
    public PartitionPruningMetadata go(IgniteRel rel) {
        result.clear();

        rel.accept(this);

        if (result.isEmpty()) {
            return PartitionPruningMetadata.EMPTY;
        } else {
            return new PartitionPruningMetadata(new Long2ObjectOpenHashMap<>(result));
        }
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel visit(IgniteIndexScan rel) {
        RexNode condition = rel.condition();

        IgniteTable table = rel.getTable().unwrap(IgniteTable.class);
        assert table != null : "No table";

        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

        extractFromTable(rel.sourceId(), table, rel.requiredColumns(), condition, rexBuilder);

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel visit(IgniteTableScan rel) {
        RexNode condition = rel.condition();

        IgniteTable table = rel.getTable().unwrap(IgniteTable.class);
        assert table != null : "No table";

        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

        extractFromTable(rel.sourceId(), table, rel.requiredColumns(), condition, rexBuilder);

        return rel;
    }

    private static class ModifyNodeShuttle extends IgniteRelShuttle {
        List<List<RexNode>> projections = null;
        List<List<RexNode>> mappingProjections = null;
        List<List<RexNode>> expressions = null;
        boolean unionRaised = false;
        IgniteRel previous = null;
        private static final Map<Class<?>, Set<Class<?>>> allowRelTransfers = new HashMap<>();

        static {
            allowRelTransfers.put(IgniteValues.class, Set.of(IgniteProject.class, IgniteExchange.class, IgniteTrimExchange.class));
            allowRelTransfers.put(IgniteProject.class, Set.of(IgniteTableModify.class, IgniteUnionAll.class, IgniteExchange.class,
                    IgniteTrimExchange.class));
            allowRelTransfers.put(IgniteUnionAll.class, Set.of(IgniteProject.class, IgniteTableModify.class));
        }

        /** {@inheritDoc} */
        @Override
        public IgniteRel visit(IgniteProject rel) {
            if (unionRaised) {
                unionRaised = false;
                // unexpected branch
                if (projections != null) {
                    //throw Util.FoundOne.NULL;
                    mappingProjections = new ArrayList<>(projections);
                    projections.clear();
                }
                if (projections == null) {
                    projections = new ArrayList<>();
                }
                projections.add(rel.getProjects());
                //projections = rel.getProjects();
            } else {
                // projections after union
                if (projections == null) {
                    projections = new ArrayList<>();
                }
                projections.add(rel.getProjects());
            }

            return super.visit(rel);
        }

        /** {@inheritDoc} */
        @Override
        public IgniteRel visit(IgniteValues rel) {
            if (expressions == null) {
                expressions = new ArrayList<>();
            }

            //List<RexNode> values = Commons.cast(rel.getTuples());
            //var values = rel.getTuples();

            for (List<RexLiteral> values : rel.getTuples()) {
                expressions.add(new ArrayList<>(values));
            }

            return super.visit(rel);
        }

        /** {@inheritDoc} */
        @Override
        public IgniteRel visit(IgniteUnionAll rel) {
            unionRaised = true;
            return super.visit(rel);
        }

        @Override
        protected IgniteRel processNode(IgniteRel rel) {
            previous = rel;
            return super.processNode(rel);
        }

        @Override
        protected void visitChild(IgniteRel parent, int i, IgniteRel child) {
            if (child instanceof IgniteExchange || child instanceof IgniteTrimExchange) {
                super.visitChild(parent, i, child);
            } else if (allowRelTransfers.getOrDefault(child.getClass(), Set.of()).contains(previous.getClass())) {
                super.visitChild(parent, i, child);
            } else {
                throw Util.FoundOne.NULL;
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel visit(IgniteTableModify rel) {
        if (rel.getOperation() != INSERT) {
            return super.visit(rel);
        }

        IgniteTable table = rel.getTable().unwrap(IgniteTable.class);

        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

        ModifyNodeShuttle modify = new ModifyNodeShuttle();

        try {
            rel.accept(modify);
        } catch (Util.FoundOne e) {
            return rel;
        }

        if (modify.mappingProjections != null) {
            List<RexNode> mappingProjections = replaceInputRefs(modify.mappingProjections.get(0));
            TargetMapping mapping = RexUtils.inversePermutation(mappingProjections, // !!!!
                    table.getRowType(Commons.typeFactory()), true);

            //assert mapping.size() == table.getRowType(Commons.typeFactory()).getFieldCount(); // to think !!

            for (int i = 0; i < modify.projections.size(); ++i) {
                List<RexNode> projections = modify.projections.get(i);
                List<RexNode> newProjections = new ArrayList<>(projections);
                for (int nodeIdx = 0; nodeIdx < projections.size(); ++nodeIdx) {
                    newProjections.set(nodeIdx, projections.get(mapping.getSourceOpt(nodeIdx)));
                }
                modify.projections.set(i, newProjections);
            }
        }

        extractFromValues(rel.sourceId(), table, modify.projections, modify.expressions, rexBuilder);

        return super.visit(rel);
    }

    private void extractFromValues(
            long sourceId,
            IgniteTable table,
            @Nullable List<List<RexNode>> projections,
            @Nullable List<List<RexNode>> expressions,
            RexBuilder rexBuilder
    ) {
        if (expressions == null) {
            return;
        }

        IntList keysList = distributionKeys(table);

        if (keysList.isEmpty()) {
            return;
        }

        boolean processed = false;

        if (projections != null) {
            boolean refFound = false;
            for (int i = 0; i < projections.size(); ++i) {
                List<RexNode> projectionsReplaced = replaceInputRefs(projections.get(i));
                refFound = !projectionsReplaced.equals(projections.get(i));

                if (refFound) {
                    projections.set(i, projectionsReplaced);
                }
            }

            processed = true;

            if (projections.size() == 1) {
                for (int i = 0; i < expressions.size(); ++i) {
                    List<RexNode> prjNodes = projections.get(0);
                    List<RexNode> exprNodes = expressions.get(i);
                    List<RexNode> newExprNodes = new ArrayList<>(prjNodes);

                    for (int prjIndex = 0; prjIndex < prjNodes.size(); ++prjIndex) {
                        RexNode prjNode = prjNodes.get(prjIndex);
                        if (prjNode instanceof RexDynamicParam) {
                            newExprNodes.set(prjIndex, prjNode);
                        } else if (prjNode instanceof RexLocalRef) {
                            RexLocalRef node0 = (RexLocalRef) prjNode;
                            newExprNodes.set(prjIndex, exprNodes.get(node0.getIndex()));
                        }
                    }

                    expressions.set(i, newExprNodes);
                }
            } else {
                //assert expressions.size() == 1;
                for (int prjIndex = 0; prjIndex < projections.size(); ++prjIndex) {
                    List<RexNode> prjNode = projections.get(prjIndex);
                    expressions.set(prjIndex, prjNode);
                }
            }

            // projections after union
            if (processed) {
/*                if (projections.size() > 1) {
                    for (List<RexNode> exprs : expressions) {
                        for (List<RexNode> prjs : projections) {
                            for (int i = 0; i < prjs.size(); ++i) {
                                RexNode node = prjs.get(i);
                                if (node instanceof RexLocalRef) {
                                    RexLocalRef node0 = (RexLocalRef) node;
                                    prjs.set(i, exprs.get(node0.getIndex()));
                                }
                            }
                        }
                    }
                }*/

                //projections = projectionsReplaced;
                //expressions = List.of(projections);

/*                mapping = RexUtils.inversePermutation(projections,
                        table.getRowType(Commons.typeFactory()), true);*/
            } else {
                // no references are found, projections contains materialized data without any refs.
                expressions = projections;
            }
        }

        List<RexNode> andEqNodes = new ArrayList<>();

        RelDataType rowTypes = table.getRowType(Commons.typeFactory());

        boolean dynParamsFound = false;

        for (List<RexNode> items : expressions) {
            // if dynamic parameters are present seems we need to skip the next one gathered from values and useless here.
            if (dynParamsFound) {
                dynParamsFound = false;
                continue;
            }

            List<RexNode> andNodes = new ArrayList<>(keysList.size());
            for (int key : keysList) {
                RexNode node = items.get(key);

                if (!isValueExpr(node)) {
                    return;
                }

                RexLocalRef ref = rexBuilder.makeLocalRef(rowTypes.getFieldList().get(key).getType(), key);

                RexNode eq = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref, node);
                andNodes.add(eq);
            }
            if (andNodes.size() > 1) {
                RexNode node0 = rexBuilder.makeCall(SqlStdOperatorTable.AND, andNodes);
                andEqNodes.add(node0);
            } else {
                andEqNodes.add(andNodes.get(0));
            }

            //dynParamsFound = items.stream().anyMatch(RexDynamicParam.class::isInstance);
        }

        if (!nullOrEmpty(andEqNodes)) {
            RexNode call;

            if (andEqNodes.size() > 1) {
                call = rexBuilder.makeCall(SqlStdOperatorTable.OR, andEqNodes);
            } else {
                call = andEqNodes.get(0);
            }

            PartitionPruningColumns metadata = extractMetadata(keysList, call, rexBuilder);

            if (metadata != null) {
                result.put(sourceId, metadata);
            }
        }
    }

    private void extractFromTable(
            long sourceId,
            IgniteTable table,
            @Nullable ImmutableBitSet requiredColumns,
            @Nullable RexNode condition,
            RexBuilder rexBuilder) {

        if (condition == null) {
            return;
        }

        IntList keysList = distributionKeys(table);

        RexNode remappedCondition;
        if (requiredColumns != null) {
            remappedCondition = remapColumns(table, requiredColumns, condition, rexBuilder);
        } else {
            remappedCondition = condition;
        }

        PartitionPruningColumns metadata = extractMetadata(keysList, remappedCondition, rexBuilder);

        if (metadata != null) {
            result.put(sourceId, metadata);
        }
    }

    private static RexNode remapColumns(IgniteTable table, ImmutableBitSet requiredColumns, RexNode condition, RexBuilder rexBuilder) {
        Int2IntMap mapping  = new Int2IntArrayMap(requiredColumns.cardinality());

        int i = 0;
        for (int r : requiredColumns) {
            mapping.put(i, r);
            i++;
        }

        RelDataType rowType = table.getRowType(Commons.typeFactory(), requiredColumns);

        return condition.accept(new RexShuttle() {
            @Override
            public RexNode visitLocalRef(RexLocalRef localRef) {
                int fieldIdx = localRef.getIndex();
                int index = mapping.get(fieldIdx);
                RelDataType fieldType = rowType.getFieldList().get(fieldIdx).getType();

                return rexBuilder.makeLocalRef(fieldType, index);
            }
        });
    }

    /** Extracts values of colocated columns from the given condition. */
    @VisibleForTesting
    public static @Nullable PartitionPruningColumns extractMetadata(IntList keys, RexNode condition, RexBuilder rexBuilder) {
        Result res = extractMetadata(condition, keys, rexBuilder, false);

        // Both unknown condition and additional condition can not be used to extract metadata.
        if (res == Result.UNKNOWN || res == Result.RESTRICT) {
            return null;
        }

        PruningColumnSets columnSets;

        if (res instanceof PruningColumnSet) {
            PruningColumnSet columnSet = (PruningColumnSet) res;
            columnSets = new PruningColumnSets();
            columnSets.candidates.add(columnSet);
        } else {
            columnSets = (PruningColumnSets) res;
        }

        // no candidates -> no metadata.
        if (columnSets.candidates.isEmpty()) {
            return null;
        }

        for (PruningColumnSet columnSet : columnSets.candidates) {
            if (!columnSet.columns.keySet().containsAll(keys)) {
                return null;
            }
        }

        List<Int2ObjectMap<RexNode>> result = new ArrayList<>(columnSets.candidates.size());

        for (PruningColumnSet columnSet : columnSets.candidates) {
            assert !columnSet.columns.isEmpty() : "Column set should not be empty";

            result.add(columnSet.columns);
        }

        return new PartitionPruningColumns(result);
    }

    private static Result extractMetadata(RexNode node, IntList keys, RexBuilder rexBuilder, boolean negate) {

        if (isColocationKey(node, keys)) {
            // a standalone <bool_col> ref.
            if (node.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
                RexLocalRef ref = (RexLocalRef) node;
                return new PruningColumnSet(ref.getIndex(), rexBuilder.makeLiteral(!negate));
            }
        } else if (node.isA(SqlKind.LOCAL_REF)) {
            return Result.RESTRICT;
        }

        List<RexNode> operands;
        if (node instanceof RexCall) {
            operands = ((RexCall) node).getOperands();
        } else {
            return Result.UNKNOWN;
        }

        switch (node.getKind()) {
            case IS_NOT_DISTINCT_FROM:
            case EQUALS: {
                // NOT (c1 = <val>) -> c1 != <val> and we can not use a value.
                // But NOT (c1 != <val>) -> c1 = <val>, and it can be used.
                RexNode lhs;
                RexNode rhs;

                if (operands.get(0).isA(SqlKind.LOCAL_REF)) {
                    lhs = operands.get(0);
                    rhs = operands.get(1);
                } else {
                    lhs = operands.get(1);
                    rhs = operands.get(0);
                }

                if (isColocationKey(lhs, keys) && isValueExpr(rhs)) {
                    if (negate) {
                        return Result.UNKNOWN;
                    } else {
                        RexLocalRef column = (RexLocalRef) lhs;
                        return new PruningColumnSet(column.getIndex(), rhs);
                    }
                } else if (lhs.isA(SqlKind.LOCAL_REF) && isValueExpr(rhs)) {
                    // some column = <val> - preserve, in case of AND it can be ignored.
                    return Result.RESTRICT;
                } else {
                    // Not a simple expression.
                    return Result.UNKNOWN;
                }
            }
            case NOT_EQUALS:
            case IS_DISTINCT_FROM: {
                RexNode lhs;
                RexNode rhs;

                if (operands.get(0).isA(SqlKind.LOCAL_REF)) {
                    lhs = operands.get(0);
                    rhs = operands.get(1);
                } else {
                    lhs = operands.get(1);
                    rhs = operands.get(0);
                }

                if (isColocationKey(lhs, keys) && isValueExpr(rhs)) {
                    // NOT(colo_key != <val>) => colo_key = <val>
                    if (negate) {
                        RexLocalRef column = (RexLocalRef) lhs;
                        return new PruningColumnSet(column.getIndex(), rhs);
                    } else {
                        return Result.UNKNOWN;
                    }
                } else if (lhs.isA(SqlKind.LOCAL_REF) && isValueExpr(rhs)) {
                    // some column != <val> - preserve, in case of AND it can be ignored.
                    return Result.RESTRICT;

                } else {
                    // Not a simple expression.
                    return Result.UNKNOWN;
                }
            }
            case OR: {
                PruningColumnSets res = new PruningColumnSets();

                for (RexNode operand : operands) {
                    Result child = extractMetadata(operand, keys, rexBuilder, negate);

                    // In case of OR: we can not ignore additional condition,
                    // because OR increases the search space.
                    if (child == Result.UNKNOWN || child == Result.RESTRICT) {
                        return Result.UNKNOWN;
                    }

                    res.add(child);
                }

                return res;
            }
            case AND: {
                PruningColumnSets res = new PruningColumnSets();

                for (RexNode operand : operands) {
                    Result child = extractMetadata(operand, keys, rexBuilder, negate);

                    if (child == Result.UNKNOWN) {
                        return Result.UNKNOWN;
                    }

                    // In case of AND: we can ignore additional condition, because
                    // such condition only narrows the search space.
                    if (child == Result.RESTRICT) {
                        continue;
                    }

                    res.combine(child);

                    if (res.conflict) {
                        return Result.UNKNOWN;
                    }
                }

                return res;
            }
            case SEARCH: {
                RexNode expandedSearch = RexUtil.expandSearch(rexBuilder, null, node);
                assert !expandedSearch.isA(SqlKind.SEARCH) : "Search operation is not expanded: " + node;

                return extractMetadata(expandedSearch, keys, rexBuilder, false);
            }
            case NOT: {
                if (isColocationKey(operands.get(0), keys)) {
                    RexLocalRef column = (RexLocalRef) operands.get(0);

                    return new PruningColumnSet(column.getIndex(), rexBuilder.makeLiteral(negate));
                } else {
                    return extractMetadata(operands.get(0), keys, rexBuilder, !negate);
                }
            }
            case IS_NULL:
            case IS_NOT_NULL: {
                RexNode operand = operands.get(0);

                if (operand.isA(SqlKind.LOCAL_REF)) {
                    return Result.RESTRICT;
                } else {
                    // If we reach this branch with a colocated key which is never null,
                    // then IS_NOT_NULL is always true / IS_NULL is always false so there is something wrong here.
                    return Result.UNKNOWN;
                }
            }
            case IS_FALSE:
            case IS_TRUE: {
                RexNode operand = operands.get(0);

                if (isColocationKey(operand, keys)) {
                    RexLocalRef ref = (RexLocalRef) operand;

                    boolean value;
                    if (negate) {
                        // NOT (col IS FALSE) => col IS TRUE => col = true
                        value = node.getKind() == SqlKind.IS_FALSE;
                    } else {
                        // NOT (col IS TRUE) => col IS FALSE => col = false
                        value = node.getKind() == SqlKind.IS_TRUE;
                    }

                    return new PruningColumnSet(ref.getIndex(), rexBuilder.makeLiteral(value));
                } else if (operand.isA(SqlKind.LOCAL_REF)) {
                    return Result.RESTRICT;
                } else {
                    return Result.UNKNOWN;
                }
            }
            case IS_NOT_FALSE:
            case IS_NOT_TRUE: {
                // IS_NOT_TRUE is used by case/when expression rewriter.
                // IS_NOT_FALSE is added for symmetry.

                boolean value;
                if (negate) {
                    value = node.getKind() != SqlKind.IS_NOT_FALSE;
                } else {
                    value = node.getKind() != SqlKind.IS_NOT_TRUE;
                }

                if (isColocationKey(operands.get(0), keys)) {
                    RexLocalRef column = (RexLocalRef) operands.get(0);
                    return new PruningColumnSet(column.getIndex(), rexBuilder.makeLiteral(value));
                } else {
                    return extractMetadata(operands.get(0), keys, rexBuilder, !negate);
                }
            }
            default: {
                if (node.isA(SqlKind.BINARY_COMPARISON)) {
                    // Convert binary comparision operations to Result::RESTRICT
                    RexNode lhs;
                    RexNode rhs;

                    if (operands.get(0).isA(SqlKind.LOCAL_REF)) {
                        lhs = operands.get(0);
                        rhs = operands.get(1);
                    } else {
                        lhs = operands.get(1);
                        rhs = operands.get(0);
                    }
                    if (isColocationKey(lhs, keys) && isValueExpr(rhs)) {
                        // We can not extract values from expressions such as colo_key > 10
                        return Result.UNKNOWN;
                    } else if (lhs.isA(SqlKind.LOCAL_REF) && isValueExpr(rhs)) {
                        // We can use non_colo_key > 10 to narrow the search space.
                        return Result.RESTRICT;
                    } else {
                        return Result.UNKNOWN;
                    }
                } else {
                    return Result.UNKNOWN;
                }
            }
        }
    }

    private static boolean isColocationKey(RexNode node, IntList keys) {
        if (node instanceof RexLocalRef) {
            RexLocalRef localRef = (RexLocalRef) node;
            return keys.contains(localRef.getIndex());
        } else {
            return false;
        }
    }

    private static boolean isValueExpr(RexNode node) {
        return node instanceof RexLiteral || node instanceof RexDynamicParam || isCorrelatedVariable(node);
    }

    static boolean isCorrelatedVariable(RexNode node) {
        // Correlated variables a referenced via field access expressions
        //
        // SELECT * FROM t1 as cor WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.c1 = cor.c1)
        //
        // So condition `t2.c1 = cor.c1` is translated to $t0 = $cor0.C1
        if (node.isA(SqlKind.FIELD_ACCESS)) {
            RexFieldAccess fieldAccess = (RexFieldAccess) node;
            return fieldAccess.getReferenceExpr().isA(SqlKind.CORREL_VARIABLE);
        } else {
            return false;
        }
    }

    /** Intermediate result of extracting partition pruning metadata. */
    private abstract static class Result {

        /** Represents an unknown result, information can not used. */
        private static final Result UNKNOWN = new Result() {
            @Override
            public String toString() {
                return "<unknown>";
            }
        };

        /** Represents additional condition, such condition can be ignored by AND operator, since they narrow the search space. */
        private static final Result RESTRICT = new Result() {
            @Override
            public String toString() {
                return "<restrict>";
            }
        };
    }

    /** A set of colocation key columns with their values. */
    private static class PruningColumnSet extends Result {

        private final Int2ObjectMap<RexNode> columns;

        PruningColumnSet(Int2ObjectMap<RexNode> columns) {
            this.columns = columns;
        }

        PruningColumnSet(int column, RexNode value) {
            columns = new Int2ObjectArrayMap<>();
            columns.put(column, value);
        }

        @Override
        public String toString() {
            return columns.toString();
        }
    }

    /** A collection of colocation key column sets. */
    private static class PruningColumnSets extends Result {

        private final List<PruningColumnSet> candidates = new ArrayList<>();

        private boolean conflict;

        /** Adds the given result to this collection of sets. Argument can be either PruningColumnSet or PruningColumnSets.  */
        void add(Result res) {
            // Add (OR) simply adds another candidate
            //
            // lhs   :  [c1 = 1, c2 = 2]
            // rhs  :  [c3 = 3]
            // result :  [c1 = 1, c2 = 2], [c3 = 3]
            //
            // lhs   :  [c1 = 1, c2 = 2]
            // rhs  :  [a = 2]
            // result :  [c1 = 1, c2 = 2], [a = 2]
            //
            // lhs   :  [c1 = 1, c2 = 2]
            // rhs  :  [c1 = 2, c2 = 3]
            // result :  [c1 = 1, c2 = 2], [c1 = 2, c2 = 3]


            if (res instanceof PruningColumnSet) {
                PruningColumnSet columnSet = (PruningColumnSet) res;
                candidates.add(columnSet);
            } else {
                PruningColumnSets columnSets = (PruningColumnSets) res;
                candidates.addAll(columnSets.candidates);
            }
        }

        /** Combines this column sets with the given result. Argument can be either PruningColumnSet or PruningColumnSets. */
        void combine(Result res) {
            // Combine (AND) merges each existing column sets with each candidate column set an for each of those
            // produces new result

            // lhs    :  [c1 = 1, c2 = 2]
            // rhs    :  [c3 = 3]
            // result :  [c1 = 1, c2 = 2, c3 = 3]

            // lhs    :  [c1 = 1, c2 = 2]
            // rhs    :  [c3 = 3], [c4 = 4]
            // result :  [c1 = 1, c2 = 2, c3 = 3], [c1 = 1, c2 = 2, c4 = 4]

            // ADD also does not allow to have the same columns with different values:

            // lhs    :  [c1 = 1, c2 = 2]
            // rhs    :  [c1 = 2]
            // result :  conflict -> c1 # c1 can't be equal to both 1 and 2

            // lhs    :  [c1 = 1, c2 = 2]
            // rhs    :  [c1 = 2, c2 = 3]
            // result :  conflict -> c1, c2

            if (candidates.isEmpty()) {
                if (res instanceof PruningColumnSet) {
                    PruningColumnSet columnSet = (PruningColumnSet) res;
                    candidates.add(columnSet);
                } else {
                    PruningColumnSets other = (PruningColumnSets) res;
                    candidates.addAll(other.candidates);
                }
            } else {

                if (conflict) {
                    return;
                }

                PruningColumnSets other;
                if (res instanceof PruningColumnSet) {
                    other = new PruningColumnSets();
                    other.candidates.add((PruningColumnSet) res);
                } else {
                    other = (PruningColumnSets) res;
                }

                List<PruningColumnSet> newOutput = new ArrayList<>();

                for (PruningColumnSet candidate : other.candidates) {
                    for (PruningColumnSet val : candidates) {
                        for (Entry<RexNode> ckv : candidate.columns.int2ObjectEntrySet()) {
                            if (!val.columns.containsKey(ckv.getIntKey())) {
                                continue;
                            }

                            Object existing = val.columns.get(ckv.getIntKey());
                            if (!Objects.equals(existing, ckv.getValue())) {
                                conflict = true;
                                return;
                            }
                        }

                        Int2ObjectMap<RexNode> newValue = new Int2ObjectArrayMap<>(val.columns.size() + candidate.columns.size());
                        newValue.putAll(val.columns);
                        newValue.putAll(candidate.columns);

                        PruningColumnSet newColSet = new PruningColumnSet(newValue);
                        newOutput.add(newColSet);
                    }
                }

                candidates.clear();
                candidates.addAll(newOutput);
            }
        }

        @Override
        public String toString() {
            return S.toString(this);
        }
    }

    private static IntList distributionKeys(IgniteTable table) {
        IgniteDistribution distribution = table.distribution();
        if (!distribution.function().affinity()) {
            return IntArrayList.of();
        }

        IntArrayList keysList = new IntArrayList(distribution.getKeys().size());
        for (Integer key : distribution.getKeys()) {
            keysList.add(key.intValue());
        }

        return keysList;
    }
}
