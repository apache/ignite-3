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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateRow;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;

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
        Mapping mapping = AggregateRow.computeFieldMapping(groupSets, AggregateType.REDUCE);
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
}
