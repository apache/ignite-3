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

import static org.apache.ignite.lang.IgniteStringFormatter.format;

import com.google.common.collect.ImmutableList;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;

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
            "SINGLE_VALUE",
            "ANY_VALUE"
    );

    /**
     * Creates an expression that returns a value of {@code args.get(0)} field of its input.
     * Should be used by aggregates that use the same operator for both MAP and REDUCE phases.
     */
    private static final MakeReduceExpr USE_INPUT_FIELD = (rexBuilder, input, args, typeFactory) ->
            rexBuilder.makeInputRef(input, args.get(0));

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
     * Creates a physical operator that implements the given logical aggregate as MAP/reduce.
     */
    public static IgniteRel buildAggregates(LogicalAggregate agg, AggregateRelBuilder builder) {

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

        List<AggregateCall> mapAggCalls = new ArrayList<>(agg.getAggCallList().size());

        for (AggregateCall call : agg.getAggCallList()) {
            MapReduceAgg mapReduceAgg = createMapReduceAggCall(call, argumentOffset);
            argumentOffset += 1;
            mapReduceAggs.add(mapReduceAgg);

            mapAggCalls.add(mapReduceAgg.mapCall);
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

        List<RelDataTypeField> outputRowFields = agg.getRowType().getFieldList();
        RelDataTypeFactory.Builder reduceType = new Builder(Commons.typeFactory());

        int groupByColumns = agg.getGroupSet().cardinality();
        boolean sameAggsForBothPhases = true;

        // Build row type for input of REDUCE phase.
        // It consists of columns from agg.groupSet and aggregate expressions.

        for (int i = 0; i < groupByColumns; i++) {
            RelDataType type = outputRowFields.get(i).getType();
            reduceType.add("f" + reduceType.getFieldCount(), type);
        }

        // Build a list of aggregate calls for REDUCE phase.
        // Build a list of projection that accept reduce phase and combine/collect/cast results.

        List<AggregateCall> reduceAggCalls = new ArrayList<>();
        List<Map.Entry<List<Integer>, MakeReduceExpr>> projection = new ArrayList<>();

        for (MapReduceAgg mapReduceAgg : mapReduceAggs) {
            // Update row type returned by REDUCE node.
            AggregateCall reduceCall = mapReduceAgg.reduceCall;
            reduceType.add("f" + reduceType.getFieldCount(), reduceCall.getType());
            reduceAggCalls.add(reduceCall);

            // Update projection list
            List<Integer> argList = mapReduceAgg.argList;
            MakeReduceExpr projectionExpr = mapReduceAgg.makeReduceExpr;
            projection.add(new SimpleEntry<>(argList, projectionExpr));

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
        // then some of MAP aggregates are not used by REDUCE phase and this is a bug.
        //
        // NOTE: In general case REDUCE phase can use more aggregates than MAP phase,
        // but at the moment there is no support for such aggregates.
        assert mapAggCalls.size() <= reduceAggCalls.size() :
                format("The number of MAP/REDUCE aggregates is not correct. MAP: {}\nREDUCE: {}", mapAggCalls, reduceAggCalls);

        IgniteRel reduce = builder.makeReduceAgg(
                agg.getCluster(),
                map,
                agg.getGroupSet(),
                agg.getGroupSets(),
                reduceAggCalls,
                reduceTypeToUse
        );

        // if aggregate MAP phase uses the same aggregates as REDUCE phase,
        // there is no need to add a projection because no additional actions are required to compute final results.
        if (sameAggsForBothPhases) {
            return reduce;
        }

        List<RexNode> projectionList = new ArrayList<>(projection.size());

        // Projection list returned by AggregateNode consists of columns from GROUP BY clause
        // and expressions that represent aggregate calls.
        // In case of MAP/REDUCE those expressions should compute final results for each MAP/REDUCE aggregate.

        int i = 0;
        for (; i < groupByColumns; i++) {
            RelDataType type = outputRowFields.get(i).getType();
            RexInputRef ref = new RexInputRef(i, type);
            projectionList.add(ref);
        }

        RexBuilder rexBuilder = agg.getCluster().getRexBuilder();
        IgniteTypeFactory typeFactory = (IgniteTypeFactory) agg.getCluster().getTypeFactory();

        for (Map.Entry<List<Integer>, MakeReduceExpr> expr : projection) {
            RexNode resultExpr = expr.getValue().makeExpr(rexBuilder, reduce, expr.getKey(), typeFactory);
            projectionList.add(resultExpr);

            // Put assertion here so we can see an expression that caused a type mismatch,
            // since Project::isValid only shows types.
            assert resultExpr.getType().equals(outputRowFields.get(i).getType()) :
                    format("Type at position#{} does not match. Expected: {} but got {}.\nREDUCE aggregates: {}\nRow: {}.\nExpr: {}",
                            i, resultExpr.getType(), outputRowFields.get(i).getType(), reduceAggCalls, outputRowFields, resultExpr);

            i++;
        }

        return new IgniteProject(agg.getCluster(), reduce.getTraitSet(), reduce, projectionList, agg.getRowType());
    }

    /**
     * Creates a MAP/REDUCE details for this call.
     */
    public static MapReduceAgg createMapReduceAggCall(AggregateCall call, int reduceArgumentOffset) {
        String aggName = call.getAggregation().getName();

        assert AGG_SUPPORTING_MAP_REDUCE.contains(aggName) : "Aggregate does not support MAP/REDUCE " + call;

        if ("COUNT".equals(aggName)) {
            return createCountAgg(call, reduceArgumentOffset);
        } else {
            return createSimpleAgg(call, reduceArgumentOffset);
        }
    }

    /**
     * Used by {@link #buildAggregates(LogicalAggregate, AggregateRelBuilder)}
     * to create MAP/REDUCE aggregate nodes.
     */
    public interface AggregateRelBuilder {

        /** Creates a rel node that represents a MAP phase.*/
        IgniteRel makeMapAgg(RelOptCluster cluster, RelNode input,
                ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggregateCalls);

        /** Creates a rel node that represents a REDUCE phase.*/
        IgniteRel makeReduceAgg(RelOptCluster cluster, RelNode map,
                ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
                List<AggregateCall> aggregateCalls, RelDataType outputType);
    }

    /** Contains information on how to build MAP/REDUCE version of an aggregate. */
    public static class MapReduceAgg {

        final List<Integer> argList;

        final AggregateCall mapCall;

        final AggregateCall reduceCall;

        final MakeReduceExpr makeReduceExpr;

        MapReduceAgg(List<Integer> argList, AggregateCall mapCall, AggregateCall reduceCall, MakeReduceExpr makeReduceExpr) {
            this.argList = argList;
            this.mapCall = mapCall;
            this.reduceCall = reduceCall;
            this.makeReduceExpr = makeReduceExpr;
        }

        /** A call for REDUCE phase. */
        public AggregateCall getReduceCall() {
            return reduceCall;
        }
    }

    private static MapReduceAgg createCountAgg(AggregateCall call, int reduceArgumentOffset) {
        List<Integer> argList = List.of(reduceArgumentOffset);

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
                null);

        // COUNT(x) aggregate have type BIGINT, but the type of SUM(COUNT(x)) is DECIMAL,
        // so we should convert it to back to BIGINT.
        MakeReduceExpr exprBuilder = (rexBuilder, input, args, typeFactory) -> {
            RexInputRef ref = rexBuilder.makeInputRef(input, args.get(0));
            return rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.BIGINT), ref);
        };

        return new MapReduceAgg(argList, call, sum0, exprBuilder);
    }

    private static MapReduceAgg createSimpleAgg(AggregateCall call, int reduceArgumentOffset) {
        List<Integer> argList = List.of(reduceArgumentOffset);

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

    @FunctionalInterface
    private interface MakeReduceExpr {

        /** Creates an expression that produces result of REDUCE phase of an aggregate. */
        RexNode makeExpr(RexBuilder rexBuilder, RelNode input, List<Integer> args, IgniteTypeFactory typeFactory);
    }
}
