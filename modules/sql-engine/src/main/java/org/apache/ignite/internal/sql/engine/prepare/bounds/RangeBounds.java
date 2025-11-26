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

package org.apache.ignite.internal.sql.engine.prepare.bounds;

import java.util.Objects;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.ignite.internal.sql.engine.rex.IgniteRexBuilder;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Range bounds holder for search row.
 */
public class RangeBounds extends SearchBounds {
    private static final RexLiteral ALWAYS_TRUE = IgniteRexBuilder.INSTANCE.makeLiteral(true);

    private final @Nullable RexNode shouldComputeLower;
    private final @Nullable RexNode lowerBound;
    private final boolean lowerInclude;

    private final @Nullable RexNode shouldComputeUpper;
    private final @Nullable RexNode upperBound;
    private final boolean upperInclude;

    /**
     * Create range bounds.
     *
     * @param condition Condition.
     * @param lowerBound Range lower bound.
     * @param upperBound Range upper bound.
     * @param lowerInclude Inclusive lower bound flag.
     * @param upperInclude Inclusive upper bound flag.
     */
    public RangeBounds(
            RexNode condition,
            @Nullable RexNode lowerBound,
            @Nullable RexNode upperBound,
            boolean lowerInclude,
            boolean upperInclude
    ) {
        this(condition, ALWAYS_TRUE, lowerBound, lowerInclude, ALWAYS_TRUE, upperBound, upperInclude);
    }

    /**
     * Create range bounds.
     *
     * @param condition Condition.
     * @param shouldComputeLower An expression denoting whether lower bound should be computed (if evaluates to {@code true}), or left open.
     * @param lowerBound Range lower bound.
     * @param lowerInclude Inclusive lower bound flag.
     * @param shouldComputeUpper An expression denoting whether upper bound should be computed (if evaluates to {@code true}), or left open.
     * @param upperBound Range upper bound.
     * @param upperInclude Inclusive upper bound flag.
     */
    public RangeBounds(
            RexNode condition,
            @Nullable RexNode shouldComputeLower,
            @Nullable RexNode lowerBound,
            boolean lowerInclude,
            @Nullable RexNode shouldComputeUpper,
            @Nullable RexNode upperBound,
            boolean upperInclude
    ) {
        super(condition);
        this.shouldComputeLower = shouldComputeLower;
        this.lowerBound = lowerBound;
        this.lowerInclude = lowerInclude;
        this.shouldComputeUpper = shouldComputeUpper;
        this.upperBound = upperBound;
        this.upperInclude = upperInclude;
    }

    public @Nullable RexNode shouldComputeLower() {
        return shouldComputeLower;
    }

    /**
     * Returns lower search bound.
     */
    public @Nullable RexNode lowerBound() {
        return lowerBound;
    }

    public @Nullable RexNode shouldComputeUpper() {
        return shouldComputeUpper;
    }

    /**
     * Returns upper search bound.
     */
    public @Nullable RexNode upperBound() {
        return upperBound;
    }

    /**
     * Returns {@code True} if the lower bound is inclusive, {@code false} otherwise.
     */
    public boolean lowerInclude() {
        return lowerInclude;
    }

    /**
     * Returns {@code True} if the upper bound is inclusive, {@code false} otherwise.
     */
    public boolean upperInclude() {
        return upperInclude;
    }

    /** {@inheritDoc} */
    @Override
    public Type type() {
        return Type.RANGE;
    }

    @SuppressWarnings("DataFlowIssue")
    @Override
    public SearchBounds accept(RexShuttle shuttle) {
        RexNode condition = condition();
        RexNode newCondition = shuttle.apply(condition);
        RexNode newLowerBound = shuttle.apply(lowerBound);
        RexNode newUpperBound = shuttle.apply(upperBound);
        RexNode newShouldComputeLower = shuttle.apply(shouldComputeLower);
        RexNode newShouldComputeUpper = shuttle.apply(shouldComputeUpper);

        if (newLowerBound == lowerBound && newUpperBound == upperBound && newCondition == condition
                && newShouldComputeLower == shouldComputeLower && newShouldComputeUpper == shouldComputeUpper) {
            return this;
        }

        return new RangeBounds(condition, newShouldComputeLower, newLowerBound,
                lowerInclude, newShouldComputeUpper, newUpperBound, upperInclude);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        //noinspection SimplifiableIfStatement
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return lowerInclude == ((RangeBounds) o).lowerInclude
                && upperInclude == ((RangeBounds) o).upperInclude
                && Objects.equals(shouldComputeLower, ((RangeBounds) o).shouldComputeLower)
                && Objects.equals(shouldComputeUpper, ((RangeBounds) o).shouldComputeUpper)
                && Objects.equals(lowerBound, ((RangeBounds) o).lowerBound)
                && Objects.equals(upperBound, ((RangeBounds) o).upperBound);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(
                lowerBound, upperBound, shouldComputeLower, shouldComputeUpper, lowerInclude, upperInclude
        );
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(RangeBounds.class, this);
    }
}
