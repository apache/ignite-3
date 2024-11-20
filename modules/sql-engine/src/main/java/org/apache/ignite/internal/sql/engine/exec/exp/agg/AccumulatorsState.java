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

package org.apache.ignite.internal.sql.engine.exec.exp.agg;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Stores states of multiple accumulator functions.
 */
public class AccumulatorsState {

    private final Object[] row;

    private final BitSet set = new BitSet();

    private int index;

    /** Constructor. */
    public AccumulatorsState(int rowSize) {
        this.row = new Object[rowSize];
    }

    /** Creates a copy from the given state. */
    public AccumulatorsState(AccumulatorsState src) {
        this.row = new Object[src.row.length];
        System.arraycopy(src.row, 0, this.row, 0, src.row.length);
    }

    /** Sets current field index. */
    public void setIndex(int i) {
        this.index = i;
    }

    /** Resets current field index. */
    public void resetIndex() {
        this.index = -1;
    }

    /** Returns a value of the current field. */
    public @Nullable Object get() {
        return row[index];
    }

    /** Set a value of the current field. */
    public void set(@Nullable Object value) {
        row[index] = value;
        set.set(index);
    }

    /** Returns {@code true} if current field has been set. */
    public boolean hasValue() {
        return set.get(index);
    }

    /** The number of elements. */
    public int size() {
        return row.length;
    }

    /** Elements of this state as list. */
    public List<Object> toList() {
        return Arrays.asList(row);
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
        AccumulatorsState state = (AccumulatorsState) o;
        return Objects.deepEquals(row, state.row);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Arrays.hashCode(row);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}
