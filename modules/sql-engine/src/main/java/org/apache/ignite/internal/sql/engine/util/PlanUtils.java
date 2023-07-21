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

import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulator;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;

/**
 * Plan util methods.
 */
public class PlanUtils {
    /**
     * Return {@code true} if observes AGGREGATE and DISTINCT simultaneously.
     *
     * @param aggCalls Aggregates.
     * @return {@code true} If found, {@code false} otherwise.
     */
    public static boolean complexDistinctAgg(List<AggregateCall> aggCalls) {
        for (AggregateCall call : aggCalls) {
            if (call.isDistinct()
                    && (call.getAggregation() instanceof SqlCountAggFunction
                    || call.getAggregation() instanceof SqlAvgAggFunction
                    || call.getAggregation() instanceof SqlSumAggFunction
                    || call.getAggregation() instanceof SqlMinMaxAggFunction)) {
                return true;
            }
        }
        return false;
    }

    /** Returns {@code true} if one of the aggregates has complex state. */
    public static boolean complexStateAgg(List<AggregateCall> aggCalls) {
        for (AggregateCall call : aggCalls) {
            if (call.getAggregation() instanceof SqlAvgAggFunction) {
                return true;
            }
        }
        return false;
    }

    /** Converts a list of accumulators for map phase to a list accumulators for reduce phase. */
    public static List<AggregateCall> convertAggsForReduce(List<AggregateCall> calls, List<ImmutableBitSet> groupSets) {
        Mapping mapping = computeAggFieldMapping(groupSets, AggregateType.REDUCE);
        int argumentsOffset = mapping.getTargetCount();
        List<AggregateCall> result = new ArrayList<>(calls.size());

        for (AggregateCall call : calls) {
            SqlAggFunction func = call.getAggregation();

            List<Integer> argList;
            SqlAggFunction aggFunction;
            boolean distinct;
            ImmutableBitSet distinctKeys;

            if (func instanceof SqlCountAggFunction) {
                argList = Collections.singletonList(argumentsOffset);

                argumentsOffset += 1;

                aggFunction = IgniteSqlOperatorTable.REDUCE_COUNT;
                distinct = false;
                distinctKeys = null;
            } else {
                argList = new ArrayList<>(call.getArgList().size());
                for (int i = 0; i < call.getArgList().size(); i++) {
                    argList.add(argumentsOffset + i);
                    argumentsOffset += 1;
                }

                aggFunction = func;
                distinct = call.isDistinct();
                distinctKeys = call.distinctKeys;
            }

            AggregateCall newCall = AggregateCall.create(
                    aggFunction,
                    distinct,
                    call.isApproximate(),
                    call.ignoreNulls(),
                    argList,
                    call.filterArg,
                    distinctKeys,
                    call.collation,
                    call.type,
                    call.name);

            result.add(newCall);
        }

        return result;
    }


    /**
     * Creates a row type for REDUCE phase of a two phase aggregate used by hash-based map/reduce implementation.
     *
     * <p>Given the grouping keys f0, f1, f2 and aggregates (agg1 and agg2) in produces the following type:
     *
     * <pre>
     *     field_0 -> f0_type
     *     field_1 -> f1_type
     *     field_2 -> f2_type
     *     field_3 -> agg1_result_type
     *     field_4 -> agg2_result_type
     * </pre>
     */
    public static RelDataType createSortAggRowType(ImmutableBitSet grpKeys,
            IgniteTypeFactory typeFactory, RelDataType inputType, List<AggregateCall> aggregateCalls) {

        RelDataTypeFactory.Builder builder = typeFactory.builder();

        for (int fieldIdx : grpKeys) {
            RelDataTypeField fld = inputType.getFieldList().get(fieldIdx);
            builder.add(fld);
        }

        addAccumulatorFields(typeFactory, aggregateCalls, builder);

        return builder.build();
    }

    /**
     * Creates a row type returned from a MAP phase of a two phase aggregate used by hash-based map/reduce implementation.
     *
     * <p>Given the input row [f0, f1, f2, f3, f4], grouping sets [f1], [f2, f0], [f4], and 2 aggregates (agg1 and agg2)
     * it produces the following type:
     *
     * <pre>
     *     field_0 -> f0_type
     *     field_1 -> f1_type
     *     field_2 -> f2_type
     *     field_3 -> f4_type
     *     field_4 -> agg1_result_type
     *     field_5 -> agg2_result_type
     *     _group_id -> byte # each grouping set has id assigned to id and that id is equal to its position in GROUP BY GROUPING SET clause.
     * </pre>
     */
    public static RelDataType createHashAggRowType(List<ImmutableBitSet> groupSets,
            IgniteTypeFactory typeFactory, RelDataType inputType, List<AggregateCall> aggregateCalls) {

        Mapping mapping = computeAggFieldMapping(groupSets, AggregateType.REDUCE);

        RelDataTypeFactory.Builder builder = typeFactory.builder();

        for (int i = 0; i < mapping.getTargetCount(); i++) {
            int source = mapping.getSource(i);
            RelDataTypeField fld = inputType.getFieldList().get(source);
            builder.add(fld);
        }

        addAccumulatorFields(typeFactory, aggregateCalls, builder);

        builder.add("_GROUP_ID", SqlTypeName.TINYINT);

        return builder.build();
    }

    /**
     * Field mapping for hash execution nodes. See {@link #createHashAggRowType(List, IgniteTypeFactory, RelDataType, List)}.
     *
     * <p>For REDUCE phase it produces the following mapping:
     *
     * <pre>
     * group sets:
     *   [0], [2, 0], [3]
     * mapping:
     *   0 -> 0
     *   2 -> 1
     *   3 -> 2
     * </pre>
     *
     * <p>Generates identity mapping for other phases.
     */
    public static Mapping computeAggFieldMapping(List<ImmutableBitSet> groupingSets, AggregateType aggregateType) {
        BitSet fieldIndices = new BitSet();

        for (ImmutableBitSet groupingSet : groupingSets) {
            for (int field : groupingSet) {
                fieldIndices.set(field);
            }
        }

        if (aggregateType == AggregateType.REDUCE) {
            Mapping mapping = Mappings.create(MappingType.INVERSE_SURJECTION, fieldIndices.length(), fieldIndices.cardinality());
            int[] position = new int[1];

            fieldIndices.stream().forEach(b -> {
                int i = position[0];
                mapping.set(b, i);
                position[0] = i + 1;
            });

            return mapping;
        } else {
            return Mappings.createIdentity(fieldIndices.length());
        }

    }

    private static void addAccumulatorFields(IgniteTypeFactory typeFactory, List<AggregateCall> aggregateCalls, Builder builder) {
        Accumulators accumulators = new Accumulators(typeFactory);

        for (int i = 0; i < aggregateCalls.size(); i++) {
            AggregateCall call = aggregateCalls.get(i);

            Accumulator acc = accumulators.accumulatorFactory(call).get();
            RelDataType fieldType = acc.returnType(typeFactory);
            String fieldName = format("_ACC{}", i);

            builder.add(fieldName, fieldType);
        }
    }
}
