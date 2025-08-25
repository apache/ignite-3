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

import java.util.Collections;
import java.util.Iterator;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeIterable;

/**
 * Implementation of {@link RangeIterable} that contains single bounds.
 */
final class SingleRangeIterable<T> implements RangeIterable<T> {
    private final T lower;

    private final T upper;

    private final boolean lowerInclusive;

    private final boolean upperInclusive;

    /**
     * Constructor.
     */
    SingleRangeIterable(T lower, T upper, boolean lowerInclusive, boolean upperInclusive) {
        this.lower = lower;
        this.upper = upper;
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
    }

    /** {@inheritDoc} */
    @Override
    public boolean multiBounds() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<RangeCondition<T>> iterator() {
        RangeCondition<T> range = new RangeCondition<>() {
            @Override
            public T lower() {
                return lower;
            }

            @Override
            public T upper() {
                return upper;
            }

            @Override
            public boolean lowerInclude() {
                return lowerInclusive;
            }

            @Override
            public boolean upperInclude() {
                return upperInclusive;
            }
        };

        return Collections.singleton(range).iterator();
    }
}
