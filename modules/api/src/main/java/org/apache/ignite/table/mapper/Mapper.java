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
 *
 * @param <T> Mapped type.
 */
//TODO: IGNITE-15784 Do we want to support custom user serializer here? Will it be mandatory?
// What about cross-platform compatibility? Is it up to user or no guarantees?
public interface Mapper<T> {
    /**
     * Creates a mapper for a case when object individual fields map to the column with the same names.
     *
     * @param cls Target object class.
     * @return Mapper.
     */
    //TODO: Method has unclear semantic in case of natively supported type, due to ommited column name.
    // As column name is missed, we are trying to map object to the single column,
    // and failed when there is more than one column to map.
    // So, method must be dropped or it's purpose must be clearly described.
    //TODO: IGNITE-15787 Maybe, the method must be used only for annotation mapper purposes.
    static <O> Mapper<O> of(Class<O> cls) {
        if (cls.isArray() || cls.isInterface() || Modifier.isAbstract(cls.getModifiers())) {
            throw new IllegalArgumentException("Class is of unsupported kind.");
        }

        return new DefaultColumnMapper<>(ensureValidKind(cls),
                Arrays.stream(cls.getDeclaredFields()).collect(Collectors.toMap(Field::getName, Field::getName)));
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
     * <p>Note: the class MUST have a default constructor.
     *
     * @param cls Target object class.
     * @return Mapper builder.
     * @throws IllegalArgumentException If class is of unsupported kind. E.g. inner, anonymous or local.
     */
    static <O> MapperBuilder<O> buildFrom(Class<O> cls) {
        if (cls.isArray() || cls.isInterface() || Modifier.isAbstract(cls.getModifiers())) {
            throw new IllegalArgumentException("Class is of unsupported kind.");
        }

        return new MapperBuilder<>(ensureDefaultConstructor(ensureValidKind(cls)));
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
    static <O> Class<O> ensureDefaultConstructor(Class<O> cls) {
        try {
            cls.getDeclaredConstructor();

            return cls;
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
     * mapping (see {@link #mappedField(String)}) should be used.
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