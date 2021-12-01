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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Mapper interface defines methods that are required for a marshaller to map class field names to table columns.
 * <p>
 * NB: Anonymous, local, inner classes, and special types are NOT supported, and mapper will not be created for them: e.g. interfaces,
 * annotation, and so on.
 *
 * @param <T> Type of which objects the mapper handles.
 */
public interface Mapper<T> {
    /**
     * Shortcut method creates a mapper for key objects, which mapped to a single key column in a schema.
     *
     * <p>This mapper supposed to be used only for keys, and only when schema contains single key field. Otherwise, using the mapper for
     * values or multi-column keys will lead to error, and {@link #of(Class, String)} should be used instead.
     *
     * <p>NB: Because of key column set is fixed (no columns can be added/removed after table created), mapping to a single column is
     * trivial and columns name can be omitted.
     *
     * @param cls Target type.
     * @return Mapper for key objects representing a single key column.
     * @throws IllegalArgumentException If {@code cls} is of unsupported kind.
     */
    //TODO: Can this method used to create annotation-based mapping ???
    // Two method with similar signature may look ambiguous for a user.
    // Maybe, rename to "single(Class)" methods for the "one-column" case?
    static <O> Mapper<O> of(Class<O> cls) {
        return new SingleColumnMapper<>(ensureValidKind(cls), null);
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
        return new SingleColumnMapper<>(ensureValidKind(cls), columnName);
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
        throw new UnsupportedOperationException("Not implemented yet.");
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
            throw new IllegalArgumentException("Missed a column name, which the field is mapped to.");
        }

        return builder(cls).map(fieldName, columnName, fieldColumnPairs).build();
    }

    /**
     * Creates a mapper builder for objects of given class.
     *
     * <p>Note: the class MUST have a default constructor.
     * <p>Note: Builder itself can't be reused.
     *
     * @param cls Parametrized type of which objects the mapper will handle.
     * @return Mapper builder.
     * @throws IllegalArgumentException If {@code cls} is of unsupported kind.
     */
    static <O> MapperBuilder<O> builder(Class<O> cls) {
        if (cls.isArray() || cls.isInterface() || Modifier.isAbstract(cls.getModifiers())) {
            throw new IllegalArgumentException("Class is of unsupported kind.");
        }

        return new MapperBuilder<>(ensureDefaultConstructor(ensureValidKind(cls)));
    }

    /**
     * Shortcut method creates a mapper for a case when object individual fields map to the column with the same name.
     *
     * @param cls Parametrized type of which objects the mapper will handle.
     * @return Identity mapper.
     */
    static <O> Mapper<O> identity(Class<O> cls) {
        if (cls.isArray() || cls.isInterface() || Modifier.isAbstract(cls.getModifiers())) {
            throw new IllegalArgumentException("Class is of unsupported kind.");
        }

        // TODO: replace "stream" with "loop".
        return new DefaultColumnMapper<>(ensureValidKind(cls),
                Arrays.stream(cls.getDeclaredFields()).collect(Collectors.toMap(Field::getName, Field::getName)));
    }

    /**
     * Ensures class is of the supported kind.
     *
     * @param cls Class to validate.
     * @return {@code cls} if it is valid.
     * @throws IllegalArgumentException If {@code cls} is invalid and can't be used in mapping.
     */
    private static <O> Class<O> ensureValidKind(Class<O> cls) {
        if (cls.isAnonymousClass() || cls.isLocalClass() || cls.isSynthetic() || cls.isPrimitive() || cls.isEnum() || cls.isAnnotation()
                || (cls.isMemberClass() && !Modifier.isStatic(cls.getModifiers()))) {
            throw new IllegalArgumentException("Class is of unsupported kind.");
        }

        return cls;
    }

    /**
     * Ensures class has default constructor.
     *
     * @param cls Class to validate.
     * @return {@code cls} if it is valid.
     * @throws IllegalArgumentException If {@code cls} can't be used in mapping.
     */
    private static <O> Class<O> ensureDefaultConstructor(Class<O> cls) {
        try {
            cls.getDeclaredConstructor();

            return cls;
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Class must have default constructor.");
        }
    }

    /**
     * Returns a type of which object the mapper handles.
     *
     * @return Mapper target type.
     */
    Class<T> targetType();

    /**
     * Returns a column name if the whole object is mapped to the single column, otherwise, returns {@code null} and individual column
     * mapping (see {@link #fieldForColumn(String)}) should be used.
     *
     * @return Column name that a whole object is mapped to, or {@code null}.
     */
    String mappedColumn();

    /**
     * Return a field name for given column name when POJO individual fields are mapped to columns, otherwise fails.
     *
     * @param columnName Column name.
     * @return Field name or {@code null} if no field mapped to a column.
     * @throws IllegalStateException If a whole object is mapped to a single column.
     */
    @Nullable String fieldForColumn(@NotNull String columnName);
}