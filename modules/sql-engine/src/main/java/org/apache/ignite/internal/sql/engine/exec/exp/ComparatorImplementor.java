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

package org.apache.ignite.internal.sql.engine.exec.exp;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.List;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.exec.RowAccessor;
import org.apache.ignite.internal.sql.engine.exec.SqlEvaluationContext;
import org.jetbrains.annotations.Nullable;

/** Implementor which implements {@link SqlComparator}. */
@SuppressWarnings("MethodMayBeStatic")
class ComparatorImplementor {
    /**
     * Implements comparator {@link SqlComparator} from given collation.
     *
     * @param collation The collation to implement comparator from.
     * @return An implementation of comparator.
     * @see SqlComparator
     */
    SqlComparator implement(RelCollation collation) {
        assert collation != null && !nullOrEmpty(collation.getFieldCollations()) : collation;

        return new SqlComparator() {
            @Override
            public <RowT> int compare(SqlEvaluationContext<RowT> context, RowT r1, RowT r2) {
                RowAccessor<RowT> hnd = context.rowAccessor();
                List<RelFieldCollation> collations = collation.getFieldCollations();

                int colsCountRow1 = hnd.columnsCount(r1);
                int colsCountRow2 = hnd.columnsCount(r2);

                // The index range condition can contain the prefix of the index columns (not all index columns).
                int maxCols = Math.min(Math.max(colsCountRow1, colsCountRow2), collations.size());

                for (int i = 0; i < maxCols; i++) {
                    RelFieldCollation field = collations.get(i);
                    boolean ascending = field.direction == Direction.ASCENDING;

                    if (i == colsCountRow1) {
                        // There is no more values in first row.
                        return ascending ? -1 : 1;
                    }

                    if (i == colsCountRow2) {
                        // There is no more values in second row.
                        return ascending ? 1 : -1;
                    }

                    int fieldIdx = field.getFieldIndex();

                    Object c1 = hnd.get(fieldIdx, r1);
                    Object c2 = hnd.get(fieldIdx, r2);

                    int nullComparison = field.nullDirection.nullComparison;

                    int res = ascending
                            ? ComparatorImplementor.compare(c1, c2, nullComparison)
                            : ComparatorImplementor.compare(c2, c1, -nullComparison);

                    if (res != 0) {
                        return res;
                    }
                }

                return 0;
            }
        };
    }

    /**
     * Implements comparator {@link SqlComparator} from given collations.
     *
     * <p>The main difference from {@link #implement(RelCollation)} is that this comparator
     * assumes rows provided on left and right side as of different types. This implies that rows
     * from right side should never be passed as first argument of comparator and vice versa.
     *
     * <p>Designed to be use as comparator for merge join. 
     *
     * @param left The collation of the left side of the join.
     * @param right The collation of the right side of the join.
     * @param equalNulls Bit set of the fields in provided collations which must threat NULLs as equal.
     * @return An implementation of comparator.
     * @see SqlComparator
     */
    SqlComparator implement(List<RelFieldCollation> left, List<RelFieldCollation> right, ImmutableBitSet equalNulls) {
        if (nullOrEmpty(left) || nullOrEmpty(right) || left.size() != right.size()) {
            throw new IllegalArgumentException("Both inputs should be non-empty and have the same size: left="
                    + (left != null ? left.size() : "null") + ", right=" + (right != null ? right.size() : "null"));
        }

        // Check that collations is correct.
        for (int i = 0; i < left.size(); i++) {
            if (left.get(i).nullDirection.nullComparison != right.get(i).nullDirection.nullComparison) {
                throw new IllegalArgumentException("Can't be compared: left=" + left.get(i) + ", right=" + right.get(i));
            }

            if (left.get(i).direction != right.get(i).direction) {
                throw new IllegalArgumentException("Can't be compared: left=" + left.get(i) + ", right=" + right.get(i));
            }
        }

        return new SqlComparator() {
            @Override
            public <RowT> int compare(SqlEvaluationContext<RowT> context, RowT r1, RowT r2) {
                boolean hasNulls = false;
                RowAccessor<RowT> hnd = context.rowAccessor();

                for (int i = 0; i < left.size(); i++) {
                    RelFieldCollation leftField = left.get(i);
                    RelFieldCollation rightField = right.get(i);

                    int leftIdx = leftField.getFieldIndex();
                    int rightIdx = rightField.getFieldIndex();

                    Object c1 = hnd.get(leftIdx, r1);
                    Object c2 = hnd.get(rightIdx, r2);

                    if (!equalNulls.get(leftIdx) && c1 == null && c2 == null) {
                        hasNulls = true;
                        continue;
                    }

                    int nullComparison = leftField.nullDirection.nullComparison;

                    int res = leftField.direction == RelFieldCollation.Direction.ASCENDING
                            ? ComparatorImplementor.compare(c1, c2, nullComparison)
                            : ComparatorImplementor.compare(c2, c1, -nullComparison);

                    if (res != 0) {
                        return res;
                    }
                }

                // If compared rows contain NULLs, they shouldn't be treated as equals, since NULL <> NULL in SQL.
                // Expect for cases with IS NOT DISTINCT
                return hasNulls ? 1 : 0;
            }
        };
    }

    @SuppressWarnings("rawtypes")
    private static int compare(@Nullable Object o1, @Nullable Object o2, int nullComparison) {
        Comparable c1 = (Comparable) o1;
        Comparable c2 = (Comparable) o2;
        return RelFieldCollation.compare(c1, c2, nullComparison);
    }
}
