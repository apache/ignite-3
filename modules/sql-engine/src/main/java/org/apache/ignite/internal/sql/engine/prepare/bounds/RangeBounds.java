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
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Range bounds holder for search row.
 */
public class RangeBounds extends SearchBounds {
    /** Lower search bound. */
    private final RexNode lowerBound;

    /** Upper search bound. */
    private final RexNode upperBound;

    /** Inclusive lower bound flag. */
    private final boolean lowerInclude;

    /** Inclusive upper bound flag. */
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
        super(condition);
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.lowerInclude = lowerInclude;
        this.upperInclude = upperInclude;
    }

    /**
     * Returns lower search bound.
     */
    public RexNode lowerBound() {
        return lowerBound;
    }

    /**
     * Returns upper search bound.
     */
    public RexNode upperBound() {
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

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return lowerInclude == ((RangeBounds) o).lowerInclude
                && upperInclude == ((RangeBounds) o).upperInclude
                && Objects.equals(lowerBound, ((RangeBounds) o).lowerBound)
                && Objects.equals(upperBound, ((RangeBounds) o).upperBound);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(lowerBound, upperBound, lowerInclude, upperInclude);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(RangeBounds.class, this);
    }
}
