/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage.index;

import java.util.Objects;
import org.apache.ignite.internal.tostring.S;

/**
 * Index column collation: direction and NULLs order that a column's value is ordered in.
 */
public class SortedIndexColumnCollation {
    private final boolean asc;

    private final boolean nullsFirst;

    /**
     * Constructor.
     *
     * @param asc {@code true} if this column is sorted in ascending order or {@code false} otherwise.
     */
    public SortedIndexColumnCollation(
            boolean asc
    ) {
        this(asc, false);
    }

    /**
     * Constructor.
     *
     * @param asc {@code true} if this column is sorted in ascending order or {@code false} otherwise.
     * @param nullsFirst NULL direction. If the flag is {@code true} NULLs are placed before all values {@code false} otherwise.
     */
    public SortedIndexColumnCollation(
            boolean asc,
            boolean nullsFirst
    ) {
        this.asc = asc;
        this.nullsFirst = nullsFirst;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SortedIndexColumnCollation that = (SortedIndexColumnCollation) o;
        return asc == that.asc && nullsFirst == that.nullsFirst;
    }

    public boolean asc() {
        return asc;
    }

    public boolean isNullsFirst() {
        return nullsFirst;
    }

    @Override
    public int hashCode() {
        return Objects.hash(asc, nullsFirst);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(SortedIndexColumnCollation.class, this);
    }
}
