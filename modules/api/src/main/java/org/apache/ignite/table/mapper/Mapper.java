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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Mapper interface defines methods that are required for a marshaller to map class field names to table columns.
 *
 * @param <T> Mapped type.
 */
public interface Mapper<T> {
    /**
     * Creates a mapper for a case when object individual fields map to the column with the same names.
     *
     * @param cls Target object class.
     * @return Mapper.
     */
    static <O> Mapper<O> of(Class<O> cls) {
        return identity(cls);
    }

    /**
     * Creates a mapper for a simple case when a whole object maps to a single column.
     *
     * @param columnName Column name.
     * @param cls        Target object class.
     * @return Single column mapper.
     */
    static <O> Mapper<O> of(String columnName, Class<O> cls) {
        return new SingleColumnMapper<>(ensureValidKind(cls), columnName);
    }

    /**
     * Creates a mapper builder for objects of given class.
     *
     * @param cls Target object class.
     * @return Mapper builder.
     * @throws IllegalArgumentException If class is of unsupported kind. E.g. inner, anonymous or local.
     */
    static <O> MapperBuilder<O> builderFor(Class<O> cls) {
        ensureDefaultConsturctor(ensureValidKind(cls));

        return new MapperBuilder<>(cls);
    }

    /**
     * Creates identity mapper which is used for simple types that have native support or objects with field names that match column names.
     *
     * @param cls Target type class.
     * @param <T>         Target type.
     * @return Mapper.
     */
    static <T> Mapper<T> identity(Class<T> cls) {
        return new IdentityMapper<T>(ensureValidKind(cls));
    }

    /**
     * Ensures class is of the supported kind.
     *
     * @param cls Class to validate.
     * @return {@code cls} if it is valid.
     * @throws IllegalArgumentException If {@code cls} is invalid and can't be used in mapping.
     */
    private static <T> Class<T> ensureValidKind(Class<T> cls) {
        if (cls.isAnonymousClass() || cls.isLocalClass() || cls.isSynthetic() || cls.isPrimitive() ||
                (cls.isMemberClass() && !Modifier.isStatic(cls.getModifiers()))) {
            throw new IllegalArgumentException("Class is of unsupported kind.");
        }

        return cls;
    }

    /**
     * Ensures class has default constructor.
     *
     * @param cls Class to validate.
     * @throws IllegalArgumentException If {@code cls} can't be used in mapping.
     */
    static <O> void ensureDefaultConsturctor(Class<O> cls) {
        try {
            cls.getDeclaredConstructor(new Class[0]);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Class must have default constructor.");
        }
    }

    /**
     * Returns a type which objects (or their fields) are mapped with the columns.
     *
     * @return Mapper target type.
     */
    Class<T> targetType();

    /**
     * Returns a column name if the whole object is mapped to the single column, otherwise, returns {@code null} and individual column
     * mapping (see {@link #mappedField(String)) should be used}.
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
    @Nullable String mappedField(@NotNull String columnName);
}