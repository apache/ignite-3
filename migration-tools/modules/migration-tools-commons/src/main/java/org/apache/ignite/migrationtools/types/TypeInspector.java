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

package org.apache.ignite.migrationtools.types;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite3.catalog.ColumnType;
import org.apache.ignite3.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;

/** Utility class that provides methods to identify relevant fields in classes. */
public class TypeInspector {
    private static final Map<Class<?>, ColumnType<?>> COL_TYPE_REF;

    static {
        try {
            COL_TYPE_REF = (Map<Class<?>, ColumnType<?>>) FieldUtils.readDeclaredStaticField(ColumnType.class, "TYPES", true);
            // TODO: IGNITE-25351 Remove
            COL_TYPE_REF.remove(java.util.Date.class);

            var constructor = ColumnType.class.getDeclaredConstructor(Class.class, String.class);
            constructor.setAccessible(true);
            constructor.newInstance(Character.class, "CHAR");
            constructor.newInstance(BitSet.class, "VARBINARY");
            constructor.newInstance(LocalTime.class, "TIME");
            constructor.newInstance(LocalDate.class, "DATE");
            constructor.newInstance(LocalDateTime.class, "TIMESTAMP");
            constructor.newInstance(Instant.class, "TIMESTAMP");
            constructor.newInstance(java.util.Date.class, "TIMESTAMP");
            constructor.newInstance(Enum.class, "VARCHAR");
            // TODO: IGNITE-25351 Remove
            constructor.newInstance(java.sql.Date.class, "DATE");
            constructor.newInstance(java.sql.Time.class, "TIME");
            constructor.newInstance(java.sql.Timestamp.class, "TIMESTAMP");
            // Collections
            constructor.newInstance(Collection.class, "VARBINARY");
            constructor.newInstance(List.class, "VARBINARY");
            constructor.newInstance(Set.class, "VARBINARY");
            // Primitive Arrays
            constructor.newInstance(boolean[].class, "VARBINARY");
            constructor.newInstance(char[].class, "VARBINARY");
            constructor.newInstance(short[].class, "VARBINARY");
            constructor.newInstance(int[].class, "VARBINARY");
            constructor.newInstance(long[].class, "VARBINARY");
            constructor.newInstance(float[].class, "VARBINARY");
            constructor.newInstance(double[].class, "VARBINARY");
            constructor.newInstance(String[].class, "VARBINARY");
        } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Inspects the given class, extracting information about how its fields should be persisted.
     *
     * @param type Class to be inspected.
     * @return List of the inspected fields.
     */
    public static List<InspectedField> inspectType(Class<?> type) {
        Class<?> rootType = ClassUtils.primitiveToWrapper(type);
        String rootTypeName = rootType.getName();

        if (rootType.isArray() || Collection.class.isAssignableFrom(rootType)) {
            return Collections.singletonList(InspectedField.forUnnamed(rootTypeName, InspectedFieldType.ARRAY));
        } else if (isPrimitiveType(rootType)) {
            return Collections.singletonList(InspectedField.forUnnamed(rootTypeName, InspectedFieldType.PRIMITIVE));
        } else {
            Field[] fields = rootType.getDeclaredFields();
            List<InspectedField> ret = new ArrayList<>(fields.length);
            for (Field field : fields) {
                if (shouldPersistField(field)) {
                    @Nullable QuerySqlField annotation = field.getAnnotation(QuerySqlField.class);
                    boolean hasAnnotation = annotation != null;

                    Class<?> origFieldType = field.getType();
                    Class<?> wrappedFieldType = ClassUtils.primitiveToWrapper(origFieldType);

                    boolean nullable = !origFieldType.isPrimitive();

                    InspectedFieldType inspectedFieldType = isPrimitiveType(wrappedFieldType)
                            ? InspectedFieldType.POJO_ATTRIBUTE
                            : InspectedFieldType.NESTED_POJO_ATTRIBUTE;

                    InspectedField inspectedField = InspectedField.forNamed(
                            field.getName(),
                            wrappedFieldType.getName(),
                            inspectedFieldType,
                            nullable,
                            hasAnnotation
                    );

                    ret.add(inspectedField);
                }
            }

            return ret;
        }
    }

    private static boolean isPrimitiveType(Class<?> type) {
        return type.isEnum() || Mapper.nativelySupported(type) || COL_TYPE_REF.containsKey(type);
    }

    private static boolean shouldPersistField(Field field) {
        var mods = field.getModifiers();
        return !Modifier.isStatic(mods) && !Modifier.isTransient(mods);
    }
}
