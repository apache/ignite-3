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
import org.apache.ignite.internal.type.NativeType;
import org.jetbrains.annotations.Nullable;

/**
 * Simple implementation of {@link ColumnDescriptor}.
 */
public class ColumnDescriptorImpl implements ColumnDescriptor {
    private static final Supplier<Object> NULL_SUPPLIER = () -> null;

    private final boolean nullable;

    private final boolean key;
    private final boolean hidden;

    private final String name;

    private final Supplier<Object> dfltVal;

    private final DefaultValueStrategy defaultStrategy;

    private final int logicalIndex;

    private final NativeType storageType;

    /**
     * Constructor.
     *
     * @param name The name of the column.
     * @param key If {@code true}, this column will be considered as a part of PK.
     * @param hidden If {@code true}, this column will not be expanded until explicitly mentioned.
     * @param nullable If {@code true}, this column will be considered as a nullable.
     * @param logicalIndex A 0-based index in a schema defined by a user.
     * @param type Type of the value in the underlying storage.
     * @param defaultStrategy A strategy to follow when generating value for column not specified in the INSERT statement.
     * @param dfltVal A value generator to use when generating value for column not specified in the INSERT statement.
     *               If {@link #defaultStrategy} is {@link DefaultValueStrategy#DEFAULT_NULL DEFAULT_NULL} then the passed supplier will
     *               be ignored, thus may be {@code null}. In other cases value supplier MUST be specified.
     */
    public ColumnDescriptorImpl(
            String name,
            boolean key,
            boolean hidden,
            boolean nullable,
            int logicalIndex,
            NativeType type,
            DefaultValueStrategy defaultStrategy,
            @Nullable Supplier<Object> dfltVal
    ) {
        this.key = key;
        this.hidden = hidden;
        this.nullable = nullable;
        this.name = name;
        this.defaultStrategy = defaultStrategy;
        this.logicalIndex = logicalIndex;
        this.storageType = type;

        this.dfltVal = defaultStrategy != DefaultValueStrategy.DEFAULT_NULL
                ? Objects.requireNonNull(dfltVal, "dfltVal")
                : NULL_SUPPLIER;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hidden() {
        return hidden;
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
    public Object defaultValue() {
        return dfltVal.get();
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override
    public int logicalIndex() {
        return logicalIndex;
    }

    /** {@inheritDoc} */
    @Override
    public NativeType physicalType() {
        return storageType;
    }
}
