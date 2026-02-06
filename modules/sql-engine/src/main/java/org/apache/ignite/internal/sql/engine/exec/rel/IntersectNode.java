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

/**
 * Execution node for INTERSECT operator.
 */
public class IntersectNode<RowT> extends AbstractSetOpNode<RowT> {

    /**
     * Constructor.
     *
     * @param ctx An execution context.
     * @param columnCnt The number of columns in an input row of a set operator.
     * @param type Aggregation mode.
     * @param all Whether this operator should return all rows or only distinct rows.
     * @param rowFactory The row factory.
     * @param inputsCnt The number of input relations this operator accepts.
     */
    public IntersectNode(ExecutionContext<RowT> ctx, int columnCnt, AggregateType type, boolean all,
            RowFactory<RowT> rowFactory, int inputsCnt) {
        super(ctx, type, all, rowFactory, new IntersectGrouping<>(ctx, rowFactory, columnCnt, type, all,  inputsCnt));
    }

    /**
     * Implementation of INTERSECT (INTERSECT DISTINCT) / INTERSECT ALL operators.
     *
     * <pre>
     *     Given sets A, B, C operator returns elements that present in all sets.
     *
     *     MAP:
     *       for each distinct row count the number of such rows in every input
     *       return: col1, .. , colN, rows_A, rows_B, rows_C
     *
     *     REDUCE:
     *       Group inputs by row and sum all row counts elements:
     *       col1, .. , colN, c1, c2
     *       col1, .. , colN, c3, c4
     *       ->
     *       col1, .. , colN, c1+c3, c2+c4
     *
     *       return:
     *        INTERCEPT ALL: for each distinct row return the smallest number of rows across rows_*.
     *        INTERCEPT DISTINCT: for each distinct row return a single row if all inputs has such rows.
     *
     *     COLOCATED:
     *       for each distinct row count the number of such rows in every input
     *       return: the same as REDUCE
     * </pre>
     *
     * @param <RowT> Type of row.
     */
    private static class IntersectGrouping<RowT> extends Grouping<RowT> {
        /** The number of inputs. */
        private final int inputsCnt;

        /** The number of rows processed by this operator. */
        private int rowsCnt = 0;

        private IntersectGrouping(ExecutionContext<RowT> ctx, RowFactory<RowT> rowFactory, int columnCnt,
                AggregateType type, boolean all, int inputsCnt) {
            super(ctx, rowFactory, columnCnt, type, all);

            this.inputsCnt = inputsCnt;
        }

        /** {@inheritDoc} */
        @Override
        protected void add(RowT row, int setIdx) {
            rowsCnt++;

            super.add(row, setIdx);
        }

        /** {@inheritDoc} */
        @Override
        protected void endOfSet(int setIdx) {
            if (type == AggregateType.SINGLE && rowsCnt == 0) {
                groups.clear();
            }

            rowsCnt = 0;
        }

        /** {@inheritDoc} */
        @Override
        protected void addOnSingle(RowT row, int setIdx) {
            int[] cntrs;

            GroupKey key = createKey(row);

            if (setIdx == 0) {
                cntrs = groups.computeIfAbsent(key, k -> new int[inputsCnt]);

                cntrs[0]++;
            } else {
                cntrs = groups.get(key);

                if (cntrs != null) {
                    if (cntrs[setIdx - 1] == 0) {
                        groups.remove(key);
                    } else {
                        cntrs[setIdx]++;
                    }
                }
            }
        }

        /** {@inheritDoc} */
        @Override
        protected int getCounterFieldsCount() {
            return inputsCnt;
        }

        /** {@inheritDoc} */
        @Override
        protected void addOnMapper(RowT row, int setIdx) {
            GroupKey key = createKey(row);
            int[] cntrs = groups.computeIfAbsent(key, k -> new int[inputsCnt]);

            cntrs[setIdx]++;
        }

        /** {@inheritDoc} */
        @Override
        protected boolean affectResult(int[] cntrs) {
            return true;
        }

        /** {@inheritDoc} */
        @Override
        protected int availableRows(int[] cntrs) {
            // Find the smallest number of rows across all inputs and use it as the number of rows to return.
            // if the number is zero there is no intersection.
            //
            // In case of INTERSECT ALL return the smallest number of rows.
            // In case of INTERSECT DISTINCT return at most one row because if cnt is positive
            // (because it means that every input has at least one row thus intersection exists).
            //
            int cnt = cntrs[0];

            for (int i = 1; i < cntrs.length; i++) {
                if (cntrs[i] < cnt) {
                    cnt = cntrs[i];
                }
            }

            if (all) {
                return cnt;
            } else {
                return cnt == 0 ? 0 : 1;
            }
        }

        /** {@inheritDoc} */
        @Override
        protected void updateAvailableRows(int[] cntrs, int availableRows) {
            if (all) {
                cntrs[0] = availableRows;
            }
        }

        /** {@inheritDoc} */
        @Override
        protected void decrementAvailableRows(int[] cntrs, int availableRows) {
            cntrs[0] -= availableRows;
        }
    }
}
