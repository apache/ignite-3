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

import java.util.List;
import java.util.Objects;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;

/**
 * Multiple bounds holder for search row.
 */
public class MultiBounds extends SearchBounds {
    @IgniteToStringInclude
    private final List<SearchBounds> bounds;

    public MultiBounds(RexNode condition, List<SearchBounds> bounds) {
        super(condition);
        this.bounds = bounds;
    }

    /**
     * Returns search bounds.
     */
    public List<SearchBounds> bounds() {
        return bounds;
    }

    /** {@inheritDoc} */
    @Override
    public Type type() {
        return Type.MULTI;
    }

    @Override
    public SearchBounds accept(RexShuttle shuttle) {
        RexNode condition = condition();
        RexNode newCondition = shuttle.apply(condition);
        List<SearchBounds> newBounds = RexUtils.processSearchBounds(shuttle, bounds);

        if (newCondition == condition && newBounds == bounds) {
            return this;
        }

        return new MultiBounds(newCondition, newBounds);
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

        return bounds.equals(((MultiBounds) o).bounds);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(bounds);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(MultiBounds.class, this);
    }
}
