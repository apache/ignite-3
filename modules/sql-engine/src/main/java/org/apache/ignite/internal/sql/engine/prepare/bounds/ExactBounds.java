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
import org.apache.calcite.rex.RexShuttle;
import org.apache.ignite.internal.tostring.S;

/**
 * Exact (equals) bounds holder for search row.
 */
public class ExactBounds extends SearchBounds {
    /** Search bound. */
    private final RexNode bound;

    public ExactBounds(RexNode condition, RexNode bound) {
        super(condition);
        this.bound = bound;
    }

    /**
     * Returns search bound.
     */
    public RexNode bound() {
        return bound;
    }

    /** {@inheritDoc} */
    @Override
    public Type type() {
        return Type.EXACT;
    }

    @Override
    public SearchBounds accept(RexShuttle shuttle) {
        RexNode condition = condition();
        RexNode newCondition = shuttle.apply(condition);
        RexNode newBound = shuttle.apply(bound);

        if (newCondition == condition && newBound == bound) {
            return this;
        }

        return new ExactBounds(newCondition, newBound);
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

        return bound.equals(((ExactBounds) o).bound);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(bound);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(ExactBounds.class, this);
    }
}
