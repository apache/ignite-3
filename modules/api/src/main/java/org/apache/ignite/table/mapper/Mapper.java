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

import java.lang.reflect.Modifier;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;

/**
 * Mapper interface defines methods that are required for a marshaller to map class field names to table columns.
 *
 * <p>NB: Anonymous, local, inner classes, and special types are NOT supported, and mapper will not be created for them: e.g. interfaces,
 * annotation, and so on.
 *
 * @param <T> Type of which objects the mapper handles.
 * @apiNote Implementation shouldn't use this interface directly, please, use {@link PojoMapper} or {@link OneColumnMapper} instead.
 * @see PojoMapper
 * @see OneColumnMapper
 */
public interface Mapper<T> {
    /**
     * Shortcut method creates a mapper for class. Natively supported types will be mapped to a single schema column, otherwise individual
     * object fields will be mapped to columns with the same name.
     *
     * <p>Note: Natively supported types can be mapped to a single key/value column, otherwise table operation will ends up with exception.
     * Use {@link #of(Class, String)} instead, to map to a concrete column name.
     *
     * @param cls Target type.
     * @return Mapper for key objects representing a single key column.
     * @throws IllegalArgumentException If {@code cls} is of unsupported kind.
     */
    static <O> Mapper<O> of(Class<O> cls) {
        if (nativelySupported(cls)) {
            return new OneColumnMapperImpl<>(cls, null, null);
        } else {
            return builder(cls).automap().build();
        }
    }

    /**
     * Creates a mapper for the case when an object represents an only one-column.
     *
     * <p>The mapper can be used as key, value, or record mapper. However, single column record looks as degraded case.
     *
     * @param cls        Parametrized type of which objects the mapper will handle.
     * @param columnName Column name to map object to.
     * @return Mapper for objects representing a one column.
     * @throws IllegalArgumentException If {@code cls} is of unsupported kind.
     */
    static <O> Mapper<O> of(Class<O> cls, String columnName) {
        return new OneColumnMapperImpl<>(ensureNativelySupported(cls), columnName, null);
    }

    /**
     * Creates a mapper for the case when an object represents an only one-column and additional transformation is required.
     *
     * <p>The mapper can be used as key, value, or record mapper. However, single column record looks as degraded case.
     *
     * @param cls        Parametrized type of which objects the mapper will handle.
     * @param columnName Column name to map object to.
     * @param <ObjectT>  Mapper target type.
     * @param <ColumnT>  MUST be a type, which compatible with the column type.
     * @return Mapper for objects representing a one column.
     * @throws IllegalArgumentException If {@code cls} is of unsupported kind.
     */
    static <ObjectT, ColumnT> Mapper<ObjectT> of(Class<ObjectT> cls, String columnName, TypeConverter<ObjectT, ColumnT> converter) {
        return new OneColumnMapperImpl<>(cls, columnName, converter);
    }

    /**
     * Shortcut method creates a mapper for a case when object individual fields map to the column by their names.
     *
     * <p>The mapper can be used as key, value, or record mapper.
     *
     * @param cls              Parametrized type of which objects the mapper will handle.
     * @param fieldName        Object field name.
     * @param columnName       Column name.
     * @param fieldColumnPairs Vararg that accepts (fieldName, columnName) pairs.
     * @return Mapper .
     * @throws IllegalArgumentException If a field name has not paired column name in {@code fieldColumnPairs}, or {@code cls} is of
     *                                  unsupported kind.
     */
    static <O> Mapper<O> of(Class<O> cls, String fieldName, String columnName, String... fieldColumnPairs) {
        if (fieldColumnPairs.length % 2 != 0) {
            throw new IllegalArgumentException(
                    "Missed a column name, which the field is mapped to: " + fieldColumnPairs[fieldColumnPairs.length - 1]);
        }

        return builder(cls).map(fieldName, columnName, fieldColumnPairs).build();
    }

    /**
     * Creates a mapper builder for objects of given class.
     *
     * <p>Note: Builder itself can't be reused.
     *
     * @param cls Parametrized type of which objects the mapper will handle. Class MUST have the default constructor,
     * @return Mapper builder.
     * @throws IllegalArgumentException If {@code cls} is of unsupported kind.
     */
    static <O> MapperBuilder<O> builder(Class<O> cls) {
        if (nativelySupported(cls)) {
            return new MapperBuilder<>(cls, null);
        } else {
            return new MapperBuilder<>(cls);
        }
    }

    /**
     * Ensures class is of natively supported kind and can be used in one-column mapping.
     *
     * @param cls Class to validate.
     * @return {@code cls} if it is of natively supported kind.
     * @throws IllegalArgumentException If {@code cls} is invalid and can't be used in one-column mapping.
     */
    static <O> Class<O> ensureNativelySupported(Class<O> cls) {
        if (nativelySupported(cls)) {
            return cls;
        }

        throw new IllegalArgumentException("Class has no native support (use PojoMapper instead): " + cls.getName());
    }

    /**
     * Ensures class is of the supported kind for POJO mapping.
     *
     * @param cls Class to validate.
     * @return {@code cls} if it is valid POJO.
     * @throws IllegalArgumentException If {@code cls} can't be used as POJO for mapping and/or of invalid kind.
     */
    static <O> Class<O> ensureValidPojo(Class<O> cls) {
        if (nativelySupported(cls) || cls.isAnonymousClass() || cls.isLocalClass() || cls.isSynthetic() || cls.isPrimitive() || cls.isEnum()
                || cls.isArray() || cls.isAnnotation() || (cls.isMemberClass() && !Modifier.isStatic(cls.getModifiers()))
                || Modifier.isAbstract(cls.getModifiers())) {
            throw new IllegalArgumentException("Class is of unsupported kind: " + cls.getName());
        }

        try {
            cls.getDeclaredConstructor();

            return cls;
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Class must have default constructor: " + cls.getName());
        }
    }

    /**
     * Checks if class is of natively supported type.
     *
     * @param type Class to check.
     * @return {@code True} if given type is supported natively, and can be mapped to a single column.
     */
    private static boolean nativelySupported(Class<?> type) {
        return !type.isPrimitive()
                && (String.class == type
                || UUID.class == type
                || BitSet.class == type
                || byte[].class == type
                || LocalDate.class == type
                || LocalTime.class == type
                || LocalDateTime.class == type
                || Instant.class == type
                || Number.class.isAssignableFrom(type)); // Byte, Short, Integer, Long, Float, Double, BigInteger, BigDecimal
    }

    /**
     * Returns a type of which object the mapper handles.
     *
     * @return Mapper target type.
     */
    Class<T> targetType();
}