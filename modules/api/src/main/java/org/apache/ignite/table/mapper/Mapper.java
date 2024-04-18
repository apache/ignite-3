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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.table.Tuple;

/**
 * Mapper interface defines marshaller methods for mapping class field names to table columns.
 *
 * <p>Note: Only natively supported types, top-level POJOs, and static nested classes are supported. Anonymous, local, inner classes,
 * interfaces, annotation, etc., cause an exception.
 *
 * @param <T> Type of objects the mapper handles.
 * @apiNote Implementation shouldn't use this interface directly; use {@link PojoMapper} or {@link OneColumnMapper}
 *         instead.
 * @see PojoMapper
 * @see OneColumnMapper
 */
public interface Mapper<T> {
    /**
     * Creates a mapper for the specified class. Natively supported types are mapped to a single schema column, otherwise individual
     * object fields are mapped to columns with the same name.
     *
     * <p>Note: Natively supported types can be mapped only to a single key/value column. If a table may have more than one column, the
     * table operation fails with an exception. Use {@link #of(Class, String)} to map to a specific column name.
     *
     * @param type Target type.
     * @return Mapper for key objects that represent a single key column.
     * @throws IllegalArgumentException If {@code type} is not supported.
     */
    static <O> Mapper<O> of(Class<O> type) {
        if (nativelySupported(type)) {
            return new OneColumnMapperImpl<>(type, null, null);
        } else {
            return builder(type).automap().build();
        }
    }

    /**
     * Creates a mapper for a case where an object represents a single column.
     *
     * <p>The mapper can be used as a key, value, or record mapper. However, a single-column record looks like a degraded case.
     *
     * @param type       Parametrized object type the mapper handles.
     * @param columnName Column name to map an object to.
     * @return Mapper for objects that represent a single column.
     * @throws IllegalArgumentException If {@code type} is not supported.
     */
    static <O> Mapper<O> of(Class<O> type, String columnName) {
        return new OneColumnMapperImpl<>(ensureNativelySupported(type), columnName, null);
    }

    /**
     * Creates a mapper for a case where an object represents a single column and an additional transformation is required.
     *
     * <p>The mapper can be used as a key, value, or record mapper. However, a single column record looks as degraded case.
     *
     * @param type       Parametrized object type the mapper handles.
     * @param columnName Column name to map an object to.
     * @param <ObjectT>  Mapper target type.
     * @param <ColumnT>  MUST be a type compatible with the column type.
     * @return Mapper for objects that represent a single column.
     * @throws IllegalArgumentException If {@code type} is not supported.
     */
    static <ObjectT, ColumnT> Mapper<ObjectT> of(
            Class<ObjectT> type,
            String columnName,
            TypeConverter<ObjectT, ColumnT> converter
    ) {
        return new OneColumnMapperImpl<>(Objects.requireNonNull(type), Objects.requireNonNull(columnName),
                Objects.requireNonNull(converter));
    }

    /**
     * Creates a mapper for a case where object's individual fields map to columns by name.
     *
     * <p>The mapper can be used as a key, value, or record mapper.
     *
     * @param type             Parametrized object type the mapper handles.
     * @param fieldName        Object field name.
     * @param columnName       Column name.
     * @param fieldColumnPairs Vararg that accepts (fieldName, columnName) pairs.
     * @return Mapper .
     * @throws IllegalArgumentException If a field name has no matching column name in {@code fieldColumnPairs}, 
     *                                  or if {@code type} is not unsupported.
     */
    static <O> Mapper<O> of(Class<O> type, String fieldName, String columnName, String... fieldColumnPairs) {
        if (fieldColumnPairs.length % 2 != 0) {
            throw new IllegalArgumentException(
                    "Missed a column name, which the field is mapped to: " + fieldColumnPairs[fieldColumnPairs.length - 1]);
        }

        return builder(type).map(Objects.requireNonNull(fieldName), Objects.requireNonNull(columnName), fieldColumnPairs).build();
    }

    /**
     * Adds a manual functional mapping for an object and a row represented by a tuple.
     *
     * @param objectToRow Object to tuple function.
     * @param rowToObject Tuple to object function.
     * @return {@code this} for chaining.
     */
    static <O> Mapper<O> of(Function<O, Tuple> objectToRow, Function<Tuple, O> rowToObject) {
        // TODO: implement custom user mapping https://issues.apache.org/jira/browse/IGNITE-16116
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Creates a mapper builder for objects of a given class.
     *
     * <p>Note: The builder cannot be reused.
     *
     * @param type Parametrized object type the mapper handles. Class MUST have a default constructor.
     * @return Mapper builder.
     * @throws IllegalArgumentException If {@code type} is not supported.
     */
    static <O> MapperBuilder<O> builder(Class<O> type) {
        if (nativelySupported(type)) {
            return new MapperBuilder<>(type, null);
        } else {
            return new MapperBuilder<>(type);
        }
    }

    /**
     * Ensures a class is of natively supported kind and can be used in one-column mapping.
     *
     * @param type Class to validate.
     * @return {@code type} if it is of a natively supported kind.
     * @throws IllegalArgumentException If {@code type} is invalid and cannot be used in one-column mapping.
     */
    static <O> Class<O> ensureNativelySupported(Class<O> type) {
        if (nativelySupported(type)) {
            return type;
        }

        throw new IllegalArgumentException("Class has no native support (type converter required): " + type.getName());
    }

    /**
     * Checks if a class is of natively supported type.
     *
     * @param type Class to check.
     * @return {@code True} if the given type is supported natively and can be mapped to a single column.
     */
    static boolean nativelySupported(Class<?> type) {
        return !Objects.requireNonNull(type).isPrimitive()
                       && (String.class == type
                                   || Boolean.class == type
                                   || UUID.class == type
                                   || BitSet.class == type
                                   || byte[].class == type
                                   || LocalDate.class == type
                                   || LocalTime.class == type
                                   || LocalDateTime.class == type
                                   || Instant.class == type
                                   || Void.class == type
                                   || Number.class
                                              .isAssignableFrom(type)); // Byte, Short, Integer, Long, Float, Double, BigInteger, BigDecimal
    }

    /**
     * Returns a type of the object the mapper handles.
     *
     * @return Mapper target type.
     */
    Class<T> targetType();
}
