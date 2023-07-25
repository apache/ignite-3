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

import static org.apache.calcite.plan.RelOptRule.convert;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
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
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
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

    private static final MakeReduceExpr SAME_AGG = (rexBuilder, input, args, typeFactory) -> rexBuilder.makeInputRef(input, args.get(0));

    private MapReduceAggregates() {

    }

    /** Check. */
    public static boolean canImplementAsMapReduce(List<AggregateCall> aggCalls) {
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
    public static IgniteRel buildAggregates(LogicalAggregate agg, AggregateRelBuilder builder,
            RelTraitSet inTrait, RelTraitSet outTrait) {

        List<MapReduceAgg> mapReduceAggs = new ArrayList<>(agg.getAggCallList().size());
        // groupSet include all columns from GROUP BY/GROUPING SETS clauses.
        int argumentOffset = agg.getGroupSet().cardinality();

        // SELECT c1, MIN(c2) FROM test GROUP BY c1, c2
        //
        // MAP    [c1, c2, agg1, agg2]
        // REDUCE [c1, c2, agg1, agg2]

        // Creates a list of descriptors for map/reduce version of the given arguments.

        for (AggregateCall call : agg.getAggCallList()) {
            MapReduceAgg mapReduceAgg = createMapReduceAggCall(call, argumentOffset);
            argumentOffset += 1;
            mapReduceAggs.add(mapReduceAgg);
        }

        // Create a MAP aggregate.

        RelNode map = builder.makeMapAgg(
                agg.getCluster(),
                convert(agg.getInput(), inTrait.replace(IgniteDistributions.random())),
                outTrait.replace(IgniteDistributions.random()),
                agg.getGroupSet(),
                agg.getGroupSets(),
                agg.getAggCallList()
        );

        List<RelDataTypeField> outputRowFields = agg.getRowType().getFieldList();
        RelDataTypeFactory.Builder reduceType = new Builder(Commons.typeFactory());

        int groupByColumns = agg.getGroupSet().cardinality();
        boolean sameAggsForBothPhases = true;

        // Build row type for input of reduce phase.
        // It consists of columns from GROUP BY clause.

        for (int i = 0; i < groupByColumns; i++) {
            RelDataType type = outputRowFields.get(i).getType();
            reduceType.add("f" + reduceType.getFieldCount(), type);
        }

        // Build a list of aggregate calls for REDUCE phase.
        // Build a list of projection that accept reduce phase and combine/collect/cast results.

        List<AggregateCall> aggCalls = new ArrayList<>();
        List<Map.Entry<List<Integer>, MakeReduceExpr>> projection = new ArrayList<>();

        for (MapReduceAgg mapReduceAgg : mapReduceAggs) {
            AggregateCall reduceCall = mapReduceAgg.reduceCall;
            List<Integer> argList = mapReduceAgg.argList;
            MakeReduceExpr projectionExpr = mapReduceAgg.makeReduceExpr;

            projection.add(new SimpleEntry<>(argList, projectionExpr));
            reduceType.add("f" + reduceType.getFieldCount(), reduceCall.getType());
            aggCalls.add(reduceCall);

            if (projectionExpr != SAME_AGG) {
                sameAggsForBothPhases = false;
            }
        }

        RelDataType reduceTypeToUse;
        if (sameAggsForBothPhases) {
            reduceTypeToUse = agg.getRowType();
        } else {
            reduceTypeToUse = reduceType.build();
        }

        // Reduce node.
        IgniteRel reduce = builder.makeReduceAgg(
                agg.getCluster(),
                convert(map, inTrait.replace(IgniteDistributions.single())),
                outTrait.replace(IgniteDistributions.single()),
                agg.getGroupSet(),
                agg.getGroupSets(),
                aggCalls,
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
                            i, resultExpr.getType(), outputRowFields.get(i).getType(), aggCalls, outputRowFields, resultExpr);

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
     * Used by {@link #buildAggregates(LogicalAggregate, AggregateRelBuilder, RelTraitSet, RelTraitSet)}
     * to create MAP/REDUCE aggregate nodes.
     */
    public interface AggregateRelBuilder {

        /** Creates a rel node that represents a MAP phase.*/
        IgniteRel makeMapAgg(RelOptCluster cluster, RelNode input, RelTraitSet traits,
                ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggregateCalls);

        /** Creates a rel node that represents a REDUCE phase.*/
        IgniteRel makeReduceAgg(RelOptCluster cluster, RelNode input, RelTraitSet traits,
                ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
                List<AggregateCall> aggregateCalls, RelDataType outputType);
    }

    /** Contains information on how to build MAP/REDUCE version of an aggregate. */
    public static class MapReduceAgg {

        final List<Integer> argList;

        final AggregateCall reduceCall;

        final MakeReduceExpr makeReduceExpr;

        MapReduceAgg(List<Integer> argList, AggregateCall reduceCall, MakeReduceExpr makeReduceExpr) {
            this.argList = argList;
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
                argList,
                call.filterArg,
                null,
                call.collation,
                call.type,
                null);

        // For COUNT aggregate SUM(COUNT(count_x)) is DECIMAL, we should convert it to BIGINT.
        MakeReduceExpr exprBuilder = (rexBuilder, input, args, typeFactory) -> {
            RexInputRef ref = rexBuilder.makeInputRef(input, args.get(0));
            return rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.BIGINT), ref);
        };

        return new MapReduceAgg(argList, sum0, exprBuilder);
    }

    private static MapReduceAgg createSimpleAgg(AggregateCall call, int reduceArgumentOffset) {
        List<Integer> argList = List.of(reduceArgumentOffset);
        AggregateCall sameAgg = AggregateCall.create(
                call.getAggregation(),
                call.isDistinct(),
                call.isApproximate(),
                call.ignoreNulls(),
                argList,
                call.filterArg,
                call.distinctKeys,
                call.collation,
                call.type,
                call.name);

        // For aggregate that use the same aggregate function for both MAP and REDUCE phases.
        // Uses the result of an aggregate as is.
        return new MapReduceAgg(argList, sameAgg, SAME_AGG);
    }

    @FunctionalInterface
    private interface MakeReduceExpr {

        /** Creates an expression that produces result of REDUCE phase of an aggregate. */
        RexNode makeExpr(RexBuilder rexBuilder, RelNode input, List<Integer> args, IgniteTypeFactory typeFactory);
    }
}
