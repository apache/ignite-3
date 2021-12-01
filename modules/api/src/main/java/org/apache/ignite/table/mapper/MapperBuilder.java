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

package org.apache.ignite.table.mapper;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;

/**
 * Mapper builder.
 *
 * <p>Note: builder can't be reused after the {@link #build()} method is called.
 *
 * @param <T> Type of which objects the mapper will handle.
 */
public final class MapperBuilder<T> {
    /** Target type. */
    private Class<T> targetType;

    /** Column-to-field name mapping. */
    private Map<String, String> columnToFields;

    /** {@code True} if the {@link #build()} method was called, {@code false} otherwise. */
    private boolean isStale;
    /**
     * Creates a mapper builder for a type.
     *
     * @param targetType Target type.
     */
    MapperBuilder(@NotNull Class<T> targetType) {
        this.targetType = targetType;

        columnToFields = new HashMap<>(targetType.getDeclaredFields().length);
    }

    /**
     * Maps a field to a column.
     *
     * @param fieldName        Field name.
     * @param columnName       Column name.
     * @param fieldColumnPairs Vararg that accepts (fieldName, columnName) pairs.
     * @return {@code this} for chaining.
     * @throws IllegalArgumentException If a field name has not paired column name in {@code fieldColumnPairs}, or a column was already
     *                                  mapped to another field.
     * @throws IllegalStateException    if tries to reuse the builder after a mapping has been built.
     */
    public MapperBuilder<T> map(@NotNull String fieldName, @NotNull String columnName, String... fieldColumnPairs) {
        if (isStale) {
            throw new IllegalStateException("Mapper builder can't be reused.");
        } else if (fieldColumnPairs.length % 2 != 0) {
            throw new IllegalArgumentException("Missed a column name, which the field is mapped to.");
        } else if (columnToFields.put(Objects.requireNonNull(columnName), Objects.requireNonNull(fieldName)) != null) {
            throw new IllegalArgumentException("Mapping for a column already exists: " + columnName);
        }

        for (int i = 0; i < fieldColumnPairs.length; i += 2) {
            if (columnToFields.put(Objects.requireNonNull(fieldColumnPairs[i + 1]), Objects.requireNonNull(fieldColumnPairs[i])) != null) {
                throw new IllegalArgumentException("Mapping for a column already exists: " + columnName);
            }
        }

        return this;
    }

    /**
     * Sets a converter for a column, which value must be converted before write/after read.
     *
     * @param converter  Converter for objects of {@link ColumnT} and {@link ObjectT}.
     * @param columnName Column name.
     * @param <ObjectT>  MUST match the object field type, if a field mapped to given column, or the object type {@link T}
     * @param <ColumnT>  MUST be a type, which compatible with the column type.
     */
    public <ObjectT, ColumnT> MapperBuilder<T> convert(
            @NotNull TypeConverter<ObjectT, ColumnT> converter,
            @NotNull String columnName
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Sets a converter for a column, which value must be converted before write/after read, and maps given field to the column.
     *
     * @param converter  Converter for objects of {@link ColumnT} and {@link ObjectT}.
     * @param fieldName  Field name.
     * @param columnName Column name.
     * @param <ObjectT>  MUST match the object field type, if the individual field mapped to given column.
     * @param <ColumnT>  MUST be a type, which compatible with the column type.
     * @throws IllegalArgumentException If converter object type doesn't match field type.
     */
    public <ObjectT, ColumnT> MapperBuilder<T> convert(
            @NotNull TypeConverter<ObjectT, ColumnT> converter,
            @NotNull String fieldName,
            @NotNull String columnName
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Adds a manual functional mapping for an object and row represented by tuple.
     *
     * @param objectToRow Object to tuple function.
     * @param rowToObject Tuple to object function.
     * @return {@code this} for chaining.
     */
    public MapperBuilder<T> map(Function<T, Tuple> objectToRow, Function<Tuple, T> rowToObject) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Map a field to a type of given class.
     * <p>
     * TODO: Method has ambiguous signature (column name is missed) and must be dropped, TODO: use {@link #convert(TypeConverter, String)}
     * instead.
     */
    public MapperBuilder<T> map(@NotNull String fieldName, Class<?> targetClass) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Adds a functional mapping for a field, the result depends on function call for every particular row.
     * <p>
     * TODO: Method has ambiguous signature, and must be dropped, use {@link #map(Function, Function)} instead.
     */
    @Deprecated
    public MapperBuilder<T> map(@NotNull String fieldName, Function<Tuple, Object> mappingFunction) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }
    /**
     * Builds mapper.
     *
     * @return Mapper.
     * @throws IllegalStateException if nothing were mapped, or more than one column were mapped to the same field, or converter object type
     *                               doesn't match the field type.
     */
    public Mapper<T> build() {
        isStale = true;

        if (columnToFields.isEmpty()) {
            throw new IllegalStateException("Empty mapping isn't allowed.");
        }

        Map<String, String> mapping = this.columnToFields;

        this.columnToFields = null;

        HashSet<String> fields = new HashSet<>(mapping.size());

        for (String f : mapping.values()) {
            if (!fields.add(f)) {
                throw new IllegalStateException("More than one column is mapped to the field: field=" + f);
            }
        }

        return new DefaultColumnMapper<>(targetType, mapping);
    }
}
