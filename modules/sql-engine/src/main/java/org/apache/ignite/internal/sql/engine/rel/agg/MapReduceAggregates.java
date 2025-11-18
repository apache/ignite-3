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

package org.apache.ignite.internal.sql.engine.rel.agg;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.math.BigDecimal;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.jetbrains.annotations.TestOnly;

/**
 * Map/reduce aggregate utility methods.
 */
public class MapReduceAggregates {

    private static final Set<String> AGG_SUPPORTING_MAP_REDUCE = Set.of(
            "COUNT",
            "MIN",
            "MAX",
            "SUM",
            "$SUM0",
            "EVERY",
            "SOME",
            "ANY",
            "AVG",
            "SINGLE_VALUE",
            "ANY_VALUE",
            "GROUPING"
    );

    /**
     * Creates an expression that returns a value of {@code args.get(0)} field of its input.
     * Should be used by aggregates that use the same operator for both MAP and REDUCE phases.
     */
    private static final MakeReduceExpr USE_INPUT_FIELD = (rexBuilder, input, args, typeFactory) ->
            rexBuilder.makeInputRef(input, args.getInt(0));

    private MapReduceAggregates() {

    }

    /** Checks whether the given list or aggregates can be represented in MAP/REDUCE form. */
    public static boolean canBeImplementedAsMapReduce(List<AggregateCall> aggCalls) {
        for (AggregateCall call : aggCalls) {
            SqlAggFunction agg = call.getAggregation();
            if (!AGG_SUPPORTING_MAP_REDUCE.contains(agg.getName())) {
                return false;
            }
        }

        return true;
    }

    /**
     * Creates a physical operator that implements the given logical aggregate as MAP/REDUCE.
     *
     * <p>Final expression consists of an aggregate for map phase (MapNode) and another aggregate for reduce phase (ReduceNode).
     * Depending on a accumulator function there can be an intermediate projection between MAP and REDUCE, and a projection after REDUCE
     * as well.
     *
     * @param agg Logical aggregate expression.
     * @param builder Builder to create implementations of MAP and REDUCE phases.
     * @param fieldMappingOnReduce Mapping to be applied to group sets on REDUCE phase.
     *
     * @return A physical node tree that implements the given logical operator.
     */
    public static IgniteRel buildAggregates(LogicalAggregate agg, AggregateRelBuilder builder, Mapping fieldMappingOnReduce) {

        //
        // To implement MAP/REDUCE aggregate LogicalAggregate is transformed into
        // a map aggregate node, a reduce aggregate node, and an optional project node
        // (since some aggregate can be split into multiple ones, or require some additional work after REDUCE phase,
        // to combine the results).
        //
        // SELECT c1, MIN(c2), COUNT(c3) FROM test GROUP BY c1, c2
        //
        // MAP      [c1, c2, map_agg1, map_agg2]
        // REDUCE   [c1, c2, reduce_agg1, reduce_agg2]
        // PROJECT: [c1, c2, expr_agg1, expr_agg2]
        //
        // =>
        //
        // {map: map_agg1, reduce: reduce_agg1, expr: expr_agg1, ..}
        // {map: map_agg2, reduce: reduce_agg2, expr: expr_agg2, ..}
        //

        // Create a list of descriptors for map/reduce version of the given arguments.
        // This list is later used to create MAP/REDUCE version of each aggregate.

        List<MapReduceAgg> mapReduceAggs = new ArrayList<>(agg.getAggCallList().size());
        // groupSet includes all columns from GROUP BY/GROUPING SETS clauses.
        int argumentOffset = agg.getGroupSet().cardinality();

        // MAP PHASE AGGREGATE

        List<AggregateCall> mapAggCalls = new ArrayList<>(agg.getAggCallList().size());

        for (AggregateCall call : agg.getAggCallList()) {
            // See ReturnTypes::AVG_AGG_FUNCTION, Result type of a aggregate with no grouping or with filtering can be nullable.
            boolean canBeNull = agg.getGroupCount() == 0 || call.hasFilter();

            MapReduceAgg mapReduceAgg = createMapReduceAggCall(
                    Commons.cluster(),
                    call,
                    argumentOffset,
                    agg.getInput().getRowType(),
                    canBeNull
            );
            argumentOffset += mapReduceAgg.reduceCalls.size();
            mapReduceAggs.add(mapReduceAgg);

            mapAggCalls.addAll(mapReduceAgg.mapCalls);
        }

        // MAP phase should have no less than the number of arguments as original aggregate.
        // Otherwise there is a bug, because some aggregates were ignored.
        assert mapAggCalls.size() >= agg.getAggCallList().size() :
                format("The number of MAP aggregates is not correct. Original: {}\nMAP: {}", agg.getAggCallList(), mapAggCalls);

        RelNode map = builder.makeMapAgg(
                agg.getCluster(),
                agg.getInput(),
                agg.getGroupSet(),
                agg.getGroupSets(),
                mapAggCalls
        );

        //
        // REDUCE INPUT PROJECTION
        //

        RelDataTypeFactory.Builder reduceType = new Builder(Commons.typeFactory());

        int groupByColumns = agg.getGroupSet().cardinality();
        boolean sameAggsForBothPhases = true;

        // Build row type for input of REDUCE phase.
        // It consists of columns from agg.groupSet and aggregate expressions.

        for (int i = 0; i < groupByColumns; i++) {
            List<RelDataTypeField> outputRowFields = agg.getRowType().getFieldList();
            RelDataType type = outputRowFields.get(i).getType();
            reduceType.add("f" + reduceType.getFieldCount(), type);
        }

        RexBuilder rexBuilder = agg.getCluster().getRexBuilder();
        IgniteTypeFactory typeFactory = (IgniteTypeFactory) agg.getCluster().getTypeFactory();

        List<RexNode> reduceInputExprs = new ArrayList<>();

        for (int i = 0; i < map.getRowType().getFieldList().size(); i++) {
            RelDataType type = map.getRowType().getFieldList().get(i).getType();
            RexInputRef ref = new RexInputRef(i, type);
            reduceInputExprs.add(ref);
        }

        // Build a list of projections for reduce operator,
        // if all projections are identity, it is not necessary
        // to create a projection between MAP and REDUCE operators.

        boolean additionalProjectionsForReduce = false;

        for (int i = 0, argOffset = 0; i < mapReduceAggs.size(); i++) {
            MapReduceAgg mapReduceAgg = mapReduceAggs.get(i);
            int argIdx = groupByColumns + argOffset;

            for (int j = 0; j < mapReduceAgg.reduceCalls.size(); j++) {
                RexNode projExpr = mapReduceAgg.makeReduceInputExpr.makeExpr(rexBuilder, map, IntList.of(argIdx), typeFactory);
                reduceInputExprs.set(argIdx, projExpr);

                if (mapReduceAgg.makeReduceInputExpr != USE_INPUT_FIELD) {
                    additionalProjectionsForReduce = true;
                }

                argIdx += 1;
            }

            argOffset += mapReduceAgg.reduceCalls.size();
        }

        RelNode reduceInputNode;
        if (additionalProjectionsForReduce) {
            RelDataTypeFactory.Builder projectRow = new Builder(agg.getCluster().getTypeFactory());

            for (int i = 0; i < reduceInputExprs.size(); i++) {
                RexNode rexNode = reduceInputExprs.get(i);
                projectRow.add(String.valueOf(i), rexNode.getType());
            }

            RelDataType projectRowType = projectRow.build();

            reduceInputNode = builder.makeProject(agg.getCluster(), map, reduceInputExprs, projectRowType);
        } else {
            reduceInputNode = map;
        }

        //
        // REDUCE PHASE AGGREGATE
        //
        // Build a list of aggregate calls for REDUCE phase.
        // Build a list of projections (arg-list, expr) that accept reduce phase and combine/collect/cast results.

        List<AggregateCall> reduceAggCalls = new ArrayList<>();
        List<Map.Entry<IntList, MakeReduceExpr>> projection = new ArrayList<>(mapReduceAggs.size());

        for (MapReduceAgg mapReduceAgg : mapReduceAggs) {
            // Update row type returned by REDUCE node.
            int i = 0;
            for (AggregateCall reduceCall : mapReduceAgg.reduceCalls) {
                reduceType.add("f" + i + "_" + reduceType.getFieldCount(), reduceCall.getType());
                reduceAggCalls.add(reduceCall);
                i += 1;
            }

            // Update projection list
            IntList reduceArgList = mapReduceAgg.argList;
            MakeReduceExpr projectionExpr = mapReduceAgg.makeReduceOutputExpr;
            projection.add(new SimpleEntry<>(reduceArgList, projectionExpr));

            if (projectionExpr != USE_INPUT_FIELD) {
                sameAggsForBothPhases = false;
            }
        }

        RelDataType reduceTypeToUse;
        if (sameAggsForBothPhases) {
            reduceTypeToUse = agg.getRowType();
        } else {
            reduceTypeToUse = reduceType.build();
        }

        // if the number of aggregates on MAP phase is larger then the number of aggregates on REDUCE phase,
        // assume that some of MAP aggregates are not used by REDUCE phase and this is a bug.
        //
        // NOTE: In general case REDUCE phase can use more aggregates than MAP phase,
        // but at the moment there is no support for such aggregates.
        assert mapAggCalls.size() <= reduceAggCalls.size() :
                format("The number of MAP/REDUCE aggregates is not correct. MAP: {}\nREDUCE: {}", mapAggCalls, reduceAggCalls);

        // Apply mapping to groupSet/groupSets on REDUCE phase.
        ImmutableBitSet groupSetOnReduce = Mappings.apply(fieldMappingOnReduce, agg.getGroupSet());
        List<ImmutableBitSet> groupSetsOnReduce = agg.getGroupSets().stream()
                .map(g -> Mappings.apply(fieldMappingOnReduce, g))
                .collect(Collectors.toList());

        IgniteRel reduce = builder.makeReduceAgg(
                agg.getCluster(),
                reduceInputNode,
                groupSetOnReduce,
                groupSetsOnReduce,
                reduceAggCalls,
                reduceTypeToUse
        );

        //
        // FINAL PROJECTION
        //
        // if aggregate MAP phase uses the same aggregates as REDUCE phase,
        // there is no need to add a projection because no additional actions are required to compute final results.
        if (sameAggsForBothPhases) {
            return reduce;
        }

        List<RexNode> projectionList = new ArrayList<>(projection.size() + groupByColumns);

        // Projection list returned by AggregateNode consists of columns from GROUP BY clause
        // and expressions that represent aggregate calls.
        // In case of MAP/REDUCE those expressions should compute final results for each MAP/REDUCE aggregate.

        int i = 0;
        for (; i < groupByColumns; i++) {
            List<RelDataTypeField> outputRowFields = agg.getRowType().getFieldList();
            RelDataType type = outputRowFields.get(i).getType();
            RexInputRef ref = new RexInputRef(i, type);
            projectionList.add(ref);
        }

        for (Map.Entry<IntList, MakeReduceExpr> expr : projection) {
            RexNode resultExpr = expr.getValue().makeExpr(rexBuilder, reduce, expr.getKey(), typeFactory);
            projectionList.add(resultExpr);
        }

        assert projectionList.size() == agg.getRowType().getFieldList().size() :
                format("Projection size does not match. Expected: {} but got {}",
                        agg.getRowType().getFieldList().size(), projectionList.size());

        for (i = 0;  i < projectionList.size(); i++) {
            RexNode resultExpr = projectionList.get(i);
            List<RelDataTypeField> outputRowFields = agg.getRowType().getFieldList();

            // Put assertion here so we can see an expression that caused a type mismatch,
            // since Project::isValid only shows types.
            assert resultExpr.getType().equals(outputRowFields.get(i).getType()) :
                    format("Type at position#{} does not match. Expected: {} but got {}.\nREDUCE aggregates: {}\nRow: {}.\nExpr: {}",
                            i, resultExpr.getType(), outputRowFields.get(i).getType(), reduceAggCalls, outputRowFields, resultExpr);

        }

        return new IgniteProject(agg.getCluster(), reduce.getTraitSet(), reduce, projectionList, agg.getRowType());
    }

    /**
     * Creates a MAP/REDUCE details for this call.
     */
    public static MapReduceAgg createMapReduceAggCall(
            RelOptCluster cluster,
            AggregateCall call,
            int reduceArgumentOffset,
            RelDataType input,
            boolean canBeNull
    ) {
        String aggName = call.getAggregation().getName();

        assert AGG_SUPPORTING_MAP_REDUCE.contains(aggName) : "Aggregate does not support MAP/REDUCE " + call;

        switch (aggName) {
            case "COUNT":
                return createCountAgg(call, reduceArgumentOffset);
            case "AVG":
                return createAvgAgg(cluster, call, reduceArgumentOffset, input, canBeNull);
            case "GROUPING":
                return createGroupingAgg(call, reduceArgumentOffset);
            default:
                return createSimpleAgg(call, reduceArgumentOffset);
        }
    }

    /**
     * Used by {@link #buildAggregates(LogicalAggregate, AggregateRelBuilder, Mapping)}.
     * to create MAP/REDUCE aggregate nodes.
     */
    public interface AggregateRelBuilder {

        /** Creates a rel node that represents a MAP phase.*/
        IgniteRel makeMapAgg(RelOptCluster cluster, RelNode input,
                ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggregateCalls);

        /**
         * Creates intermediate projection operator that transforms results from MAP phase, and transforms them to inputs to REDUCE phase.
         */
        IgniteRel makeProject(RelOptCluster cluster, RelNode input, List<RexNode> reduceInputExprs, RelDataType projectRowType);

        /** Creates a rel node that represents a REDUCE phase.*/
        IgniteRel makeReduceAgg(RelOptCluster cluster, RelNode input,
                ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
                List<AggregateCall> aggregateCalls, RelDataType outputType);
    }

    /** Contains information on how to build MAP/REDUCE version of an aggregate. */
    public static class MapReduceAgg {

        /** Argument list on reduce phase. */
        final IntList argList;

        /** MAP phase aggregate, an initial aggregation function was transformed into. */
        final List<AggregateCall> mapCalls;

        /** REDUCE phase aggregate, an initial aggregation function was transformed into. */
        final List<AggregateCall> reduceCalls;

        /** Produces expressions to consume results of a MAP phase aggregation. */
        final MakeReduceExpr makeReduceInputExpr;

        /** Produces expressions to consume results of a REDUCE phase aggregation to comprise final result. */
        final MakeReduceExpr makeReduceOutputExpr;

        MapReduceAgg(
                IntList argList,
                AggregateCall mapCalls,
                AggregateCall reduceCalls,
                MakeReduceExpr makeReduceOutputExpr
        ) {
            this(argList, List.of(mapCalls), USE_INPUT_FIELD, List.of(reduceCalls), makeReduceOutputExpr);
        }

        MapReduceAgg(
                IntList argList,
                List<AggregateCall> mapCalls,
                MakeReduceExpr makeReduceInputExpr,
                List<AggregateCall> reduceCalls,
                MakeReduceExpr makeReduceOutputExpr
        ) {
            this.argList = argList;
            this.mapCalls = mapCalls;
            this.reduceCalls = reduceCalls;
            this.makeReduceInputExpr = makeReduceInputExpr;
            this.makeReduceOutputExpr = makeReduceOutputExpr;
        }

        /** A call for REDUCE phase. */
        @TestOnly
        public AggregateCall getReduceCall() {
            return reduceCalls.get(0);
        }
    }

    private static MapReduceAgg createCountAgg(AggregateCall call, int reduceArgumentOffset) {
        IntList argList = IntList.of(reduceArgumentOffset);

        AggregateCall sum0 = AggregateCall.create(
                SqlStdOperatorTable.SUM0,
                call.isDistinct(),
                call.isApproximate(),
                call.ignoreNulls(),
                ImmutableList.of(),
                argList,
                // there is no filtering on REDUCE phase
                -1,
                null,
                call.collation,
                call.type,
                "COUNT_" + reduceArgumentOffset + "_MAP_SUM");

        // COUNT(x) aggregate have type BIGINT, but the type of SUM(COUNT(x)) is DECIMAL,
        // so we should convert it to back to BIGINT.
        MakeReduceExpr exprBuilder = (rexBuilder, input, args, typeFactory) -> {
            RexInputRef ref = rexBuilder.makeInputRef(input, args.getInt(0));
            return rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.BIGINT), ref, true, false);
        };

        return new MapReduceAgg(argList, call, sum0, exprBuilder);
    }

    private static MapReduceAgg createSimpleAgg(AggregateCall call, int reduceArgumentOffset) {
        IntList argList = IntList.of(reduceArgumentOffset);

        AggregateCall reduceCall = AggregateCall.create(
                call.getAggregation(),
                call.isDistinct(),
                call.isApproximate(),
                call.ignoreNulls(),
                ImmutableList.of(),
                argList,
                // there is no filtering on REDUCE phase
                -1,
                call.distinctKeys,
                call.collation,
                call.type,
                call.name);

        // For aggregate that use the same aggregate function for both MAP and REDUCE phases
        // use the result of an aggregate as is.
        return new MapReduceAgg(argList, call, reduceCall, USE_INPUT_FIELD);
    }

    private static MapReduceAgg createGroupingAgg(AggregateCall call, int reduceArgumentOffset) {
        IntList argList = IntList.of(reduceArgumentOffset);

        AggregateCall reduceCall = AggregateCall.create(
                IgniteSqlOperatorTable.SAME_VALUE,
                call.isDistinct(),
                call.isApproximate(),
                call.ignoreNulls(),
                ImmutableList.of(),
                argList,
                // there is no filtering on REDUCE phase
                -1,
                null,
                call.collation,
                call.type,
                "GROUPING" + reduceArgumentOffset);

        // For aggregate that use the same aggregate function for both MAP and REDUCE phases
        // use the result of an aggregate as is.
        return new MapReduceAgg(argList, call, reduceCall, USE_INPUT_FIELD);
    }

    /**
     * Produces intermediate expressions that modify results of MAP/REDUCE aggregate.
     * For example: after splitting a function into a MAP aggregate and REDUCE aggregate it is necessary to add casts to
     * output of a REDUCE phase aggregate.
     *
     * <p>In order to avoid creating unnecessary projections, use {@link MapReduceAggregates#USE_INPUT_FIELD}.
     */
    @FunctionalInterface
    private interface MakeReduceExpr {

        /**
         * Creates an expression that applies a performs computation (e.g. applies some function, adds a cast)
         * on {@code args} fields of input relation.
         *
         * @param rexBuilder Expression builder.
         * @param input Input relation.
         * @param args Arguments.
         * @param typeFactory Type factory.
         *
         * @return Expression.
         */
        RexNode makeExpr(RexBuilder rexBuilder, RelNode input, IntList args, IgniteTypeFactory typeFactory);
    }

    private static MapReduceAgg createAvgAgg(
            RelOptCluster cluster,
            AggregateCall call,
            int reduceArgumentOffset,
            RelDataType inputType,
            boolean canBeNull
    ) {
        RelDataTypeFactory tf = cluster.getTypeFactory();
        RelDataTypeSystem typeSystem = tf.getTypeSystem();

        RelDataType fieldType = inputType.getFieldList().get(call.getArgList().get(0)).getType();

        // In case of AVG(NULL) return a simple version of an aggregate, because result is always NULL.
        if (fieldType.getSqlTypeName() == SqlTypeName.NULL) {
            return createSimpleAgg(call, reduceArgumentOffset);
        }

        // AVG(x) : SUM(x)/COUNT0(x)
        // MAP    : SUM(x) / COUNT(x)

        // SUM(x) as s
        RelDataType mapSumType = typeSystem.deriveSumType(tf, fieldType);
        if (canBeNull) {
            mapSumType = tf.createTypeWithNullability(mapSumType, true);
        }

        AggregateCall mapSum0 = AggregateCall.create(
                SqlStdOperatorTable.SUM,
                call.isDistinct(),
                call.isApproximate(),
                call.ignoreNulls(),
                ImmutableList.of(),
                call.getArgList(),
                call.filterArg,
                null,
                call.collation,
                mapSumType,
                "AVG_SUM" + reduceArgumentOffset);

        // COUNT(x) as c
        RelDataType mapCountType = tf.createSqlType(SqlTypeName.BIGINT);

        AggregateCall mapCount0 = AggregateCall.create(
                SqlStdOperatorTable.COUNT,
                call.isDistinct(),
                call.isApproximate(),
                call.ignoreNulls(),
                ImmutableList.of(),
                call.getArgList(),
                call.filterArg,
                null,
                call.collation,
                mapCountType,
                "AVG_COUNT" + reduceArgumentOffset);

        // REDUCE : SUM(s) as reduce_sum, SUM0(c) as reduce_count
        IntList reduceSumArgs = IntList.of(reduceArgumentOffset);

        // SUM0(s)
        RelDataType reduceSumType = typeSystem.deriveSumType(tf, mapSumType);
        if (canBeNull) {
            reduceSumType = tf.createTypeWithNullability(reduceSumType, true);
        }

        AggregateCall reduceSum0 = AggregateCall.create(
                SqlStdOperatorTable.SUM,
                call.isDistinct(),
                call.isApproximate(),
                call.ignoreNulls(),
                ImmutableList.of(),
                reduceSumArgs,
                // there is no filtering on REDUCE phase
                -1,
                null,
                call.collation,
                reduceSumType,
                "AVG_SUM" + reduceArgumentOffset);

        // SUM0(c)
        RelDataType reduceSumCountType = typeSystem.deriveSumType(tf, mapCount0.type);
        IntList reduceSumCountArgs = IntList.of(reduceArgumentOffset + 1);

        AggregateCall reduceSumCount = AggregateCall.create(
                SqlStdOperatorTable.SUM0,
                call.isDistinct(),
                call.isApproximate(),
                call.ignoreNulls(),
                ImmutableList.of(),
                reduceSumCountArgs,
                // there is no filtering on REDUCE phase
                -1,
                null,
                call.collation,
                reduceSumCountType,
                "AVG_SUM0" + reduceArgumentOffset);

        RelDataType finalReduceSumType = reduceSumType;

        MakeReduceExpr reduceInputExpr = (rexBuilder, input, args, typeFactory) -> {
            RexInputRef argExpr = rexBuilder.makeInputRef(input, args.getInt(0));

            if (args.getInt(0) == reduceArgumentOffset) {
                // Accumulator functions handle NULL, so it is safe to ignore it.
                if (!SqlTypeUtil.equalSansNullability(finalReduceSumType, argExpr.getType())) {
                    return rexBuilder.makeCast(finalReduceSumType, argExpr, true, false);
                } else {
                    return argExpr;
                }
            } else {
                return rexBuilder.makeCast(reduceSumCount.type, argExpr, true, false);
            }

        };

        // PROJECT: reduce_sum/reduce_count

        MakeReduceExpr reduceOutputExpr = (rexBuilder, input, args, typeFactory) -> {
            RexNode numeratorRef = rexBuilder.makeInputRef(input, args.get(0));
            RexInputRef denominatorRef = rexBuilder.makeInputRef(input, args.get(1));

            numeratorRef = rexBuilder.ensureType(mapSum0.type, numeratorRef, true);

            RelDataType resultType = typeSystem.deriveAvgAggType(typeFactory, call.getType());

            RexNode sumDivCnt;
            if (SqlTypeUtil.isExactNumeric(resultType)) {
                // Return correct decimal type with correct scale and precision.
                int precision = resultType.getPrecision(); // not used.
                int scale = resultType.getScale();

                RexLiteral p = rexBuilder.makeExactLiteral(BigDecimal.valueOf(precision), tf.createSqlType(SqlTypeName.INTEGER));
                RexLiteral s = rexBuilder.makeExactLiteral(BigDecimal.valueOf(scale), tf.createSqlType(SqlTypeName.INTEGER));

                sumDivCnt = rexBuilder.makeCall(IgniteSqlOperatorTable.DECIMAL_DIVIDE, numeratorRef, denominatorRef, p, s);

                if (call.getType().getSqlTypeName() != SqlTypeName.DECIMAL) {
                    sumDivCnt = rexBuilder.makeCast(call.getType(), sumDivCnt, false, false);
                }
            } else {
                sumDivCnt = rexBuilder.makeCall(IgniteSqlOperatorTable.DIVIDE, numeratorRef, denominatorRef);
            }

            if (canBeNull) {
                // CASE cnt == 0 THEN null
                // OTHERWISE sum / cnt
                RexLiteral zero = rexBuilder.makeExactLiteral(BigDecimal.ZERO, denominatorRef.getType());
                RexNode eqZero = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, numeratorRef, zero);
                RexLiteral nullRes = rexBuilder.makeNullLiteral(call.getType());

                return rexBuilder.makeCall(SqlStdOperatorTable.CASE, eqZero, nullRes, sumDivCnt);
            } else {
                return sumDivCnt;
            }
        };

        IntList argList = IntList.of(reduceArgumentOffset, reduceArgumentOffset + 1);
        return new MapReduceAgg(
                argList,
                List.of(mapSum0, mapCount0),
                reduceInputExpr,
                List.of(reduceSum0, reduceSumCount),
                reduceOutputExpr
        );
    }
}
