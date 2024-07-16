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

package org.apache.ignite.internal.sql.engine.schema;

import java.util.Objects;
import java.util.function.Supplier;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link ColumnDescriptor} for a catalog-based table.
 */
public final class CatalogColumnDescriptor implements ColumnDescriptor {

    private static final Supplier<Object> NULL_SUPPLIER = () -> null;

    private final boolean nullable;

    private final boolean key;

    private final String name;

    private final Supplier<Object> dfltVal;

    private final DefaultValueStrategy defaultStrategy;

    private final int index;

    private final ColumnType columnType;

    private final int precision;

    private final int scale;

    private final int length;

    private NativeType nativeType;

    /**
     * Constructor.
     *
     * @param name The name of the column.
     * @param key If {@code true}, this column will be considered as a part of PK.
     * @param nullable If {@code true}, this column will be considered as a nullable.
     * @param index A 0-based index in a schema defined by a user.
     * @param columnType An SQL type of this column.
     * @param precision Precision of this type, if applicable.
     * @param scale Scale of this type, if applicable.
     * @param length Length of this type, if applicable.
     * @param defaultStrategy A strategy to follow when generating value for column not specified in the INSERT statement.
     * @param dfltVal A value generator to use when generating value for column not specified in the INSERT statement.
     *               If {@link #defaultStrategy} is {@link DefaultValueStrategy#DEFAULT_NULL DEFAULT_NULL} then the passed supplier will
     *               be ignored, thus may be {@code null}. In other cases value supplier MUST be specified.
     */
    public CatalogColumnDescriptor(
            String name,
            boolean key,
            boolean nullable,
            int index,
            ColumnType columnType,
            int precision,
            int scale,
            int length,
            @Nullable DefaultValueStrategy defaultStrategy,
            @Nullable Supplier<Object> dfltVal
    ) {
        this.key = key;
        this.nullable = nullable;
        this.name = name;
        this.index = index;
        this.defaultStrategy = defaultStrategy;
        this.columnType = columnType;
        this.precision = precision;
        this.scale = scale;
        this.length = length;

        if (defaultStrategy != null) {
            this.dfltVal = defaultStrategy == DefaultValueStrategy.DEFAULT_NULL
                    ? NULL_SUPPLIER
                    : Objects.requireNonNull(dfltVal, "dfltVal");
        } else {
            this.dfltVal = NULL_SUPPLIER;
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean virtual() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nullable() {
        return nullable;
    }

    /** {@inheritDoc} */
    @Override
    public boolean key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override
    public DefaultValueStrategy defaultStrategy() {
        return defaultStrategy;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override
    public int logicalIndex() {
        return index;
    }

    /** {@inheritDoc} */
    @Override
    public NativeType physicalType() {
        if (nativeType == null) {
            nativeType = TypeUtils.columnType2NativeType(columnType, precision, scale, length);
        }
        return nativeType;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Object defaultValue() {
        return dfltVal.get();
    }
}
