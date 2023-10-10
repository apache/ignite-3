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

package org.apache.ignite.internal.sql.engine.datatypes.tests;

import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.NativeTypeWrapper;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.sql.ColumnType;

/**
 * {@code DataTypeTestSpec} describes a data type, provides convenient methods for testing.
 *
 * <ul>
 *     <li>If {@code storageType} is not {@link Comparable} must use provide implementation of
 *     {@link NativeTypeWrapper} and use it instead of their storage type.</li>
 *     <li>If type has SQL literal {@link #hasLiterals()} should return {@code true}
 *     and {@link #toLiteral(Comparable)} must convert values into corresponding literals.</li>
 * </ul>
 *
 * @param <T> java type used by tests.
 */
public abstract class DataTypeTestSpec<T extends Comparable<T>> {

    private final ColumnType columnType;

    private final String typeName;

    private final Class<?> storageType;

    /** Constructor. */
    public DataTypeTestSpec(ColumnType columnType, String typeName, @SuppressWarnings("unused") Class<T> javaType) {
        // javaType is only used to restrict the generic parameter.
        this.columnType = columnType;
        this.typeName = typeName;
        this.storageType = columnType.javaClass();
    }

    /** {@link ColumnType}. */
    public final ColumnType columnType() {
        return columnType;
    }

    /** SQL type name. */
    public final String typeName() {
        return typeName;
    }

    /** Storage type. */
    public final Class<?> storageType() {
        return storageType;
    }

    /**
     * Returns {@code true} if there is SQL literal syntax for this type.
     */
    public abstract boolean hasLiterals();

    /**
     * Produces a SQL literal for the given value.
     * Such literals can be used in queries as {@code $N_lit}, where {@code N} is zero-based.
     *
     * @param value A value.
     * @return An SQL literal for the given value.
     * @throws UnsupportedOperationException if this type does not have literal syntax.
     */
    public abstract String toLiteral(T value);

    /**
     * Produces an SQL expression that produces the given value.
     * Such expressions can be used in queries as {@code $N}, where {@code N} is zero-bases.
     *
     * @param value A value.
     * @return an SQL expression.
     */
    public abstract String toValueExpr(T value);

    /**
     * Converts the given value to its string representation.
     * A result is used to check type conversion from a string to value of this data type.
     *
     * @param value A value.
     * @return A string representation of the given value.
     */
    public abstract String toStringValue(T value);

    /**
     * Wraps original {@link NativeType} into a {@link NativeTypeWrapper comparable wrapper}.
     * If storage type of this data type is {@link Comparable} then this method must return {@code null}.
     */
    public abstract T wrapIfNecessary(Object storageValue);

    /**
     * Unwraps {@link NativeTypeWrapper comparable wrapper} into a {@link NativeType}.
     *
     * <p>If passed values are not wrapped, then this method should return original instance.
     */
    public Object unwrapIfNecessary(T value) {
        return value;
    }

    /** Creates {@link TestDataSamples test samples} for the given type. */
    public abstract TestDataSamples<T> createSamples(IgniteTypeFactory typeFactory);
}
