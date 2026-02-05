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

package org.apache.ignite.table.mapper;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Maps object fields to columns by name.
 *
 * @param <T> Target type.
 */
class PojoMapperImpl<T> implements PojoMapper<T> {
    /** Target type. */
    private final Class<T> targetType;

    /** Column-to-field name mapping. */
    private final Map<String, String> columnToFieldName;

    private final Map<String, Field> columnToField;

    private final Map<String, TypeConverter<?, ?>> converters;

    /**
     * Creates a mapper for a given type.
     *
     * @param targetType Target type.
     * @param columnToFieldName    Column-to-field name mapping.
     * @param converters Column converters.
     */
    PojoMapperImpl(
            Class<T> targetType,
            Map<String, String> columnToFieldName,
            Map<String, Field> columnToField,
            Map<String, TypeConverter<?, ?>> converters
    ) {
        this.converters = converters;
        if (Objects.requireNonNull(columnToFieldName).isEmpty()) {
            throw new IllegalArgumentException("Empty mapping isn't allowed.");
        }

        this.targetType = targetType;
        this.columnToFieldName = columnToFieldName;
        this.columnToField = columnToField;
    }

    /** {@inheritDoc} */
    @Override
    public Class<T> targetType() {
        return targetType;
    }

    /** {@inheritDoc} */
    @Override
    public String fieldForColumn(String columnName) {
        return columnToFieldName.get(columnName);
    }

    @Override
    public @Nullable Field declaredFieldForColumn(String columnName) {
        return columnToField.get(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<String> fields() {
        return columnToFieldName.values();
    }

    /** {@inheritDoc} */
    @Override
    public <FieldT, ColumnT> TypeConverter<FieldT, ColumnT> converterForColumn(String columnName) {
        return (TypeConverter<FieldT, ColumnT>) converters.get(columnName);
    }
}
