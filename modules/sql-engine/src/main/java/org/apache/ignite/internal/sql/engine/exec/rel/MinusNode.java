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

package org.apache.ignite.internal.sql.engine.exec.rel;

import org.apache.ignite.internal.sql.engine.api.expressions.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.GroupKey;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteMapMinus;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteMinus;

/**
 * Execution node for MINUS (EXCEPT) operator.
 */
public class MinusNode<RowT> extends AbstractSetOpNode<RowT> {

    /**
     * Constructor.
     *
     * @param ctx An execution context.
     * @param columnCnt The number of columns in an input row of a set operator.
     * @param type Aggregation mode.
     * @param all Whether this operator should return all rows or only distinct rows.
     * @param rowFactory The row factory.
     */
    public MinusNode(ExecutionContext<RowT> ctx, int columnCnt, AggregateType type, boolean all,
            RowFactory<RowT> rowFactory) {
        super(ctx, type, new MinusGrouping<>(ctx, rowFactory, columnCnt, type, all));
    }

    /**
     * Implementation of EXCEPT (EXCEPT DISTINCT) / EXCEPT ALL operators.
     *
     * <pre>
     *     Given sets A, B, C operator returns elements from in A that are not present in B and C.
     *
     *     MAP:
     *      for each distinct row count the number of such rows in A (rows_A) and in sets except A (rows_rest)
     *      return col1, .. , colN, rows_A, rows_rest
     *
     *     REDUCE:
     *      Group inputs by row and sum all row counts elements:
     *        col1, .. , colN, c1, c2, c3
     *        col1, .. , colN, c3, c4, c6
     *        ->
     *        col1, .. , colN, c1+c3, c2+c4, c3+c6
     *
     *      return:
     *        EXCEPT ALL        : for each distinct row return the number of rows equal to rows_A - rows_rest or 0.
     *        EXCEPT (DISTINCT) : for each distinct row return one row if rows_rest is 0 or 0 if rows_A is positive
     *        (both sides contain a row, then such row should not be returned).
     *
     *     COLOCATED:
     *        for each distinct row count the number of rows in A (rows_A) and in sets except A (rows_rest)
     *        return: the same as REDUCE.
     * </pre>
     *
     * @param <RowT> Type of row.
     */
    private static class MinusGrouping<RowT> extends Grouping<RowT> {
        private MinusGrouping(ExecutionContext<RowT> ctx, RowFactory<RowT> rowFactory,
                int columnCnt, AggregateType type, boolean all) {
            super(ctx, rowFactory, columnCnt, type, all);
        }

        /** {@inheritDoc} */
        @Override
        protected void endOfSet(int setIdx) {

        }

        /** {@inheritDoc} */
        @Override
        protected void addOnSingle(RowT row, int setIdx) {
            int[] cntrs;

            GroupKey key = createKey(row);

            if (setIdx == 0) {
                // Value in the map will always have 2 elements, first - count of keys in the first set,
                // second - count of keys in all sets except first.
                cntrs = groups.computeIfAbsent(key, k -> new int[IgniteMapMinus.COUNTER_FIELDS_CNT]);

                cntrs[0]++;
            } else if (all) {
                cntrs = groups.get(key);

                if (cntrs != null) {
                    cntrs[1]++;

                    if (cntrs[1] >= cntrs[0]) {
                        groups.remove(key);
                    }
                }
            } else {
                groups.remove(key);
            }
        }

        /** {@inheritDoc} */
        @Override
        protected int getCounterFieldsCount() {
            return IgniteMinus.COUNTER_FIELDS_CNT;
        }

        /** {@inheritDoc} */
        @Override
        protected void addOnMapper(RowT row, int setIdx) {
            // Value in the map will always have 2 elements, first - count of keys in the first set,
            // second - count of keys in all sets except first.
            int[] cntrs = groups.computeIfAbsent(createKey(row), k -> new int[IgniteMapMinus.COUNTER_FIELDS_CNT]);

            cntrs[setIdx == 0 ? 0 : 1]++;
        }

        /** {@inheritDoc} */
        @Override
        protected boolean affectResult(int[] cntrs) {
            // This is an optimization to decrease the number of intermediate rows produced on MAP phase:
            // group(1): A EXCEPT ALL B: A = 10 rows, and B = 10 rows
            // There is no need to send this group to REDUCE phase, because num(A) - num(B) = 0 and difference operation
            // is going to produce no results. See availableRows.
            //
            // In case of group(1): A EXCEPT (DISTINCT) B, where A = 10 rows, and B = 10 rows
            // We still have to send this group because all groups are processed
            // independently of each other and it is possible that on REDUCE phase (where all results are combined)
            // the number of rows in group(1) is going to increase.
            return !all || cntrs[0] != cntrs[1];
        }

        /** {@inheritDoc} */
        @Override
        protected int availableRows(int[] cntrs) {
            // For group(1): A EXCEPT B EXCEPT C
            // In case of EXCEPT ALL return (rows(A) - (rows(B) + rows(C))) rows
            // In case of EXCEPT (DISTINCT) return at most one row if (rows(B) + rows(C)) == 0,
            // because difference between A and an empty set is A.
            assert cntrs.length == IgniteMapMinus.COUNTER_FIELDS_CNT;

            if (all) {
                return Math.max(cntrs[0] - cntrs[1], 0);
            } else {
                return cntrs[1] == 0 ? 1 : 0;
            }
        }

        /** {@inheritDoc} */
        @Override
        protected void updateAvailableRows(int[] cntrs, int availableRows) {
            // There is no need to update counters in case of EXCEPT operator.
        }

        /** {@inheritDoc} */
        @Override
        protected void decrementAvailableRows(int[] cntrs, int availableRows) {
            cntrs[0] -= availableRows;
        }
    }
}
