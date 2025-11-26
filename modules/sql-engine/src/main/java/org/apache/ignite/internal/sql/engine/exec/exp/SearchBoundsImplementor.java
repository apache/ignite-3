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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.jetbrains.annotations.Nullable;

/** Implementor which implements {@link SqlScalar} returning iterator over index's range conditions. */
@SuppressWarnings({"NestedAssignment", "MethodMayBeStatic"})
class SearchBoundsImplementor {
    /**
     * Implements given search bounds as {@link SqlScalar} returning iterator over index's range conditions.
     *
     * @param searchBounds The search bounds to implement.
     * @param indexKeyType The type of the index key.
     * @param comparator Optional comparator to sort resulting ranges in order to provide result matching index collation.
     * @param rowProviderImplementor The function to use to implement every single bound within a compound bounds.
     * @param <RowT> The type of the execution row.
     * @return An implementation of scalar.
     * @see SqlScalar
     */
    <RowT> SqlScalar<RowT, RangeIterable<RowT>> implement(
            List<SearchBounds> searchBounds,
            RelDataType indexKeyType,
            @Nullable SqlComparator<RowT> comparator,
            Function<List<RexNode>, SqlRowProvider<RowT>> rowProviderImplementor,
            Function<RexNode, SqlScalar<RowT, Boolean>> scalarImplementor
    ) {
        return context -> {
            List<RangeCondition<RowT>> ranges = new ArrayList<>();

            expandBounds(
                    rowProviderImplementor,
                    scalarImplementor,
                    context,
                    ranges,
                    searchBounds,
                    0,
                    Arrays.asList(new RexNode[indexKeyType.getFieldCount()]),
                    Arrays.asList(new RexNode[indexKeyType.getFieldCount()]),
                    true,
                    true
            );

            return new RangeIterableImpl<>(context, ranges, comparator);
        };
    }

    /**
     * Transforms input bound, stores only sequential non null elements.
     * i.e. (literal1, literal2, null, literal3) -> (literal1, literal2).
     * Return transformed bound and appropriate type.
     */
    private static List<RexNode> shrinkBounds(List<RexNode> bound) {
        List<RexNode> newBound = new ArrayList<>();
        for (RexNode node : bound) {
            if (node != null) {
                newBound.add(node);
            } else {
                break;
            }
        }

        return Collections.unmodifiableList(newBound);
    }

    /**
     * Expand column-oriented {@link SearchBounds} to a row-oriented list of ranges ({@link RangeCondition}).
     *
     * @param ranges List of ranges.
     * @param searchBounds Search bounds.
     * @param fieldIdx Current field index (field to process).
     * @param curLower Current lower row.
     * @param curUpper Current upper row.
     * @param lowerInclude Include current lower row.
     * @param upperInclude Include current upper row.
     */
    private <RowT> void expandBounds(
            Function<List<RexNode>, SqlRowProvider<RowT>> rowProviderFunction,
            Function<RexNode, SqlScalar<RowT, Boolean>> scalarImplementor,
            ExecutionContext<RowT> context,
            List<RangeCondition<RowT>> ranges,
            List<SearchBounds> searchBounds,
            int fieldIdx,
            List<RexNode> curLower,
            List<RexNode> curUpper,
            boolean lowerInclude,
            boolean upperInclude
    ) {
        if ((fieldIdx >= searchBounds.size())
                || (!lowerInclude && !upperInclude)
                || searchBounds.get(fieldIdx) == null) {
            // we need to shrink bounds here due to the recursive logic for processing lower and upper bounds,
            // after division this logic into upper and lower calculation such approach need to be removed.
            curLower = shrinkBounds(curLower);
            curUpper = shrinkBounds(curUpper);

            ranges.add(new RangeConditionImpl<>(
                    context,
                    nullOrEmpty(curLower) ? null : rowProviderFunction.apply(curLower), 
                    nullOrEmpty(curUpper) ? null : rowProviderFunction.apply(curUpper),
                    lowerInclude,
                    upperInclude
            ));

            return;
        }

        SearchBounds fieldBounds = searchBounds.get(fieldIdx);

        Collection<SearchBounds> fieldMultiBounds = fieldBounds instanceof MultiBounds
                ? ((MultiBounds) fieldBounds).bounds()
                : Collections.singleton(fieldBounds);

        for (SearchBounds fieldSingleBounds : fieldMultiBounds) {
            RexNode fieldLowerBound;
            RexNode fieldUpperBound;
            boolean fieldLowerInclude = lowerInclude;
            boolean fieldUpperInclude = upperInclude;

            if (fieldSingleBounds instanceof ExactBounds) {
                fieldLowerBound = fieldUpperBound = ((ExactBounds) fieldSingleBounds).bound();
                fieldLowerInclude = fieldUpperInclude = true;
            } else if (fieldSingleBounds instanceof RangeBounds) {
                RangeBounds fieldRangeBounds = (RangeBounds) fieldSingleBounds;

                fieldLowerBound = deriveBound(
                        scalarImplementor, context, fieldRangeBounds, true
                );
                if (fieldLowerBound != null) {
                    fieldLowerInclude = fieldRangeBounds.lowerInclude();
                }

                fieldUpperBound = deriveBound(
                        scalarImplementor, context, fieldRangeBounds, false
                );
                if (fieldUpperBound != null) {
                    fieldUpperInclude = fieldRangeBounds.upperInclude();
                }
            } else {
                throw new IllegalStateException("Unexpected bounds: " + fieldSingleBounds);
            }

            if (lowerInclude) {
                curLower.set(fieldIdx, fieldLowerBound);
            }

            if (upperInclude) {
                curUpper.set(fieldIdx, fieldUpperBound);
            }

            expandBounds(
                    rowProviderFunction,
                    scalarImplementor,
                    context,
                    ranges,
                    searchBounds,
                    fieldIdx + 1,
                    curLower,
                    curUpper,
                    lowerInclude && fieldLowerInclude,
                    upperInclude && fieldUpperInclude
            );
        }

        curLower.set(fieldIdx, null);
        curUpper.set(fieldIdx, null);
    }

    private static class RangeConditionImpl<RowT> implements RangeCondition<RowT> {
        private final ExecutionContext<RowT> context;

        /** Lower bound expression. */
        private final @Nullable SqlRowProvider<RowT> lowerBound;

        /** Upper bound expression. */
        private final @Nullable SqlRowProvider<RowT> upperBound;

        /** Inclusive lower bound flag. */
        private final boolean lowerInclude;

        /** Inclusive upper bound flag. */
        private final boolean upperInclude;

        /** Lower row. */
        private @Nullable RowT lowerRow;

        /** Upper row. */
        private @Nullable RowT upperRow;

        private RangeConditionImpl(
                ExecutionContext<RowT> context,
                @Nullable SqlRowProvider<RowT> lowerScalar,
                @Nullable SqlRowProvider<RowT> upperScalar,
                boolean lowerInclude,
                boolean upperInclude
        ) {
            this.context = context;
            this.lowerBound = lowerScalar;
            this.upperBound = upperScalar;
            this.lowerInclude = lowerInclude;
            this.upperInclude = upperInclude;
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable RowT lower() {
            if (lowerBound == null) {
                return null;
            }

            return lowerRow != null ? lowerRow : (lowerRow = lowerBound.get(context));
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable RowT upper() {
            if (upperBound == null) {
                return null;
            }

            return upperRow != null ? upperRow : (upperRow = upperBound.get(context));
        }

        /** {@inheritDoc} */
        @Override
        public boolean lowerInclude() {
            return lowerInclude;
        }

        /** {@inheritDoc} */
        @Override
        public boolean upperInclude() {
            return upperInclude;
        }

        /** Clear cached rows. */
        void clearCache() {
            lowerRow = null;
            upperRow = null;
        }
    }

    private static class RangeIterableImpl<RowT> implements RangeIterable<RowT> {
        private final ExecutionContext<RowT> context;
        private final @Nullable SqlComparator<RowT> comparator;

        private List<RangeCondition<RowT>> ranges;

        private boolean sorted;

        RangeIterableImpl(
                ExecutionContext<RowT> context,
                List<RangeCondition<RowT>> ranges,
                @Nullable SqlComparator<RowT> comparator
        ) {
            this.context = context;
            this.ranges = ranges;
            this.comparator = comparator;
        }

        /** {@inheritDoc} */
        @Override public boolean multiBounds() {
            return ranges.size() > 1;
        }

        /** {@inheritDoc} */
        @Override
        public Iterator<RangeCondition<RowT>> iterator() {
            ranges.forEach(b -> ((RangeConditionImpl<RowT>) b).clearCache());

            if (ranges.size() == 1) {
                return ranges.iterator();
            }

            // Sort ranges using collation comparator to produce sorted output. There should be no ranges
            // intersection.
            // Do not sort again if ranges already were sorted before, different values of correlated variables
            // should not affect ordering.
            if (!sorted && comparator != null) {
                ranges.sort(this::compareRanges);

                List<RangeCondition<RowT>> ranges0 = new ArrayList<>(ranges.size());

                RangeConditionImpl<RowT> prevRange = null;

                for (RangeCondition<RowT> range0 : ranges) {
                    RangeConditionImpl<RowT> range = (RangeConditionImpl<RowT>) range0;

                    if (compareLowerAndUpperBounds(range.lower(), range.upper()) > 0) {
                        // Invalid range (low > up).
                        continue;
                    }

                    if (prevRange != null) {
                        RangeConditionImpl<RowT> merged = tryMerge(prevRange, range);

                        if (merged == null) {
                            ranges0.add(prevRange);
                        } else {
                            range = merged;
                        }
                    }

                    prevRange = range;
                }

                if (prevRange != null) {
                    ranges0.add(prevRange);
                }

                ranges = ranges0;
                sorted = true;
            }

            return ranges.iterator();
        }

        private int compareRanges(RangeCondition<RowT> first, RangeCondition<RowT> second) {
            int cmp = compareBounds(first.lower(), second.lower(), true);

            if (cmp != 0) {
                return cmp;
            }

            return compareBounds(first.upper(), second.upper(), false);
        }

        private int compareBounds(@Nullable RowT row1, @Nullable RowT row2, boolean lower) {
            assert comparator != null;

            if (row1 == null || row2 == null) {
                if (row1 == row2) {
                    return 0;
                }

                RowT row = lower ? row2 : row1;

                return row == null ? 1 : -1;
            }

            return comparator.compare(context, row1, row2);
        }

        private int compareLowerAndUpperBounds(@Nullable RowT lower, @Nullable RowT upper) {
            assert comparator != null;

            if (lower == null || upper == null) {
                if (lower == upper) {
                    return 0;
                } else {
                    // lower = null -> lower < any upper
                    // upper = null -> upper > any lower
                    return -1;
                }
            }

            return comparator.compare(context, lower, upper);
        }

        /** Returns combined range if the provided ranges intersect, {@code null} otherwise. */
        @Nullable RangeConditionImpl<RowT> tryMerge(RangeConditionImpl<RowT> first, RangeConditionImpl<RowT> second) {
            if (compareLowerAndUpperBounds(first.lower(), second.upper()) > 0
                    || compareLowerAndUpperBounds(second.lower(), first.upper()) > 0) {
                // Ranges are not intersect.
                return null;
            }

            SqlRowProvider<RowT> newLowerBound;
            RowT newLowerRow;
            boolean newLowerInclude;

            int cmp = compareBounds(first.lower(), second.lower(), true);

            if (cmp < 0 || (cmp == 0 && first.lowerInclude())) {
                newLowerBound = first.lowerBound;
                newLowerRow = first.lower();
                newLowerInclude = first.lowerInclude();
            } else {
                newLowerBound = second.lowerBound;
                newLowerRow = second.lower();
                newLowerInclude = second.lowerInclude();
            }

            SqlRowProvider<RowT> newUpperBound;
            RowT newUpperRow;
            boolean newUpperInclude;

            cmp = compareBounds(first.upper(), second.upper(), false);

            if (cmp > 0 || (cmp == 0 && first.upperInclude())) {
                newUpperBound = first.upperBound;
                newUpperRow = first.upper();
                newUpperInclude = first.upperInclude();
            } else {
                newUpperBound = second.upperBound;
                newUpperRow = second.upper();
                newUpperInclude = second.upperInclude();
            }

            RangeConditionImpl<RowT> newRangeCondition = new RangeConditionImpl<>(
                    first.context, newLowerBound, newUpperBound, newLowerInclude, newUpperInclude);

            newRangeCondition.lowerRow = newLowerRow;
            newRangeCondition.upperRow = newUpperRow;

            return newRangeCondition;
        }
    }

    private static <RowT> @Nullable RexNode deriveBound(
            Function<RexNode, SqlScalar<RowT, Boolean>> scalarImplementor,
            ExecutionContext<RowT> context,
            RangeBounds bounds,
            boolean lower
    ) {
        RexNode shouldComputeExpression;
        RexNode bound;
        if (lower) {
            shouldComputeExpression = bounds.shouldComputeLower();
            bound = bounds.lowerBound();
        } else {
            shouldComputeExpression = bounds.shouldComputeUpper();
            bound = bounds.upperBound();
        }

        if (shouldComputeExpression == null) {
            return null;
        }

        boolean shouldCompute = shouldComputeExpression.isAlwaysTrue();

        if (!shouldCompute) {
            SqlScalar<RowT, Boolean> scalar = scalarImplementor.apply(shouldComputeExpression);

            shouldCompute = scalar.get(context) == Boolean.TRUE;
        }

        return shouldCompute ? bound : null;
    }
}
