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

package org.apache.ignite.lang;

import java.util.Objects;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a value that can be {@code null}. Used to distinguish 'value is absent' and `value is null` cases.
 *
 * @param <T> Value type.
 * @see org.apache.ignite.table.KeyValueView#getNullable(Transaction, Object)
 */
public final class NullableValue<T> {
    /** Wrapped value. */
    private T value;

    /**
     * Creates a wrapper for nullable value.
     *
     * @param value Value.
     */
    public NullableValue(@Nullable T value) {
        this.value = value;
    }

    /**
     * Returns wrapped value.
     *
     * @return Value.
     */
    public @Nullable T value() {
        return value;
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
        NullableValue<?> that = (NullableValue<?>) o;
        return Objects.equals(value, that.value);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "NullableValue{value=" + value + '}';
    }
}
