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

import java.util.List;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomTypeSpec;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.sql.ColumnType;

/**
 * {@link IgniteCustomTypeSpec} + its values + convenient methods.
 */
public abstract class CustomDataTypeTestSpec<T extends Comparable<T>> {

    private final ColumnType columnType;

    private final String typeName;

    protected final List<T> values;

    private final Class<?> storageType;

    /** Constructor. */
    public CustomDataTypeTestSpec(ColumnType columnType, String typeName, Class<T> javaType, T[] values) {
        this.columnType = columnType;
        this.typeName = typeName;
        this.values = List.of(values);
        this.storageType = ColumnType.columnTypeToClass(columnType);
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
     *
     * @param value A value.
     * @return An SQL literal for the given value.
     * @throws UnsupportedOperationException if this type does not have literal syntax.
     */
    public abstract String toLiteral(T value);

    /**
     * Produces an expression that is used to replace placeholder values ({@code $N}). If a type has its own SQL literals, implementation of
     * this method must call {@link #toLiteral(T)}.
     *
     * @param value A value.
     * @return an SQL expression.
     */
    public abstract String toValueExpr(T value);

    public abstract String toStringValue(T value);

    /** Creates {@link TestDataSamples test samples} for the given type. */
    public abstract TestDataSamples<T> createSamples(IgniteTypeFactory typeFactory);
}
