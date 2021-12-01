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
 * Mapper builder provides methods for mapping object fields to columns.
 *
 * <p>By default, a user must explicitly map needed fields with columns
 * in one-to-one manner using {@link #map} and/or {@link #map(String, String, TypeConverter)} methods, all missed columns and/or fields
 * become unmapped, and will be ignored during further table operations.
 *
 * <p>Calling {@link #automap()} method changes default behavior, and maps all the missed fields to the
 * columns, which names are match. A field or a column pair for which wasn't found will be ignored.
 *
 * <p>TBD: add some code examples.
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
     * Maps a field to a column with using type converter. The value will be converted before write to and after read from column using
     * provided converter.
     *
     * @param <ObjectT>  MUST match the object field type, if the individual field mapped to given column.
     * @param <ColumnT>  MUST be a type, which compatible with the column type.
     * @param fieldName  Field name.
     * @param columnName Column name.
     * @param converter  Converter for objects of {@link ColumnT} and {@link ObjectT}.
     */
    public <ObjectT, ColumnT> MapperBuilder<T> map(
            @NotNull String fieldName,
            @NotNull String columnName,
            @NotNull TypeConverter<ObjectT, ColumnT> converter
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
     * Make mapper treat missed columns as they mapped to the field of the same name. If class {@link T} has no field for the missed column,
     * then left the column unmapped.
     *
     * @return {@code this} for chaining.
     */
    public MapperBuilder<T> automap() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Builds mapper.
     *
     * @return Mapper.
     * @throws IllegalStateException if nothing were mapped, or more than one column were mapped to the same field.
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
