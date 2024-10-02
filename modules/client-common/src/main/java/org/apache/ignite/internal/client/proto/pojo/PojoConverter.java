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

package org.apache.ignite.internal.client.proto.pojo;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.util.Collection;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.mapper.PojoMapper;

/** Converts POJO to Tuple and back. */
public class PojoConverter {

    /**
     * Converts POJO to Tuple. Supports public and private non-static fields.
     *
     * @param obj POJO to convert.
     * @return Tuple with columns corresponding to supported fields of the POJO.
     * @throws PojoConversionException If conversion failed.
     */
    public static Tuple toTuple(Object obj) throws PojoConversionException {
        Class<?> clazz = obj.getClass();

        // TODO https://issues.apache.org/jira/browse/IGNITE-23092
        if (clazz.getSuperclass() != Object.class) {
            throw new PojoConversionException("Can't convert subclasses");
        }

        PojoMapper<?> mapper;
        try {
            mapper = (PojoMapper<?>) Mapper.of(clazz);
        } catch (IllegalArgumentException e) {
            throw new PojoConversionException("Class " + clazz.getName() + " doesn't contain any marshallable fields", e);
        }

        Tuple tuple = Tuple.create();

        Collection<String> fields = mapper.fields();
        for (String fieldName : fields) {
            try {
                // Name needs to be quoted to keep the case
                String columnName = "\"" + fieldName + "\"";
                Field field = clazz.getDeclaredField(fieldName);
                // TODO https://issues.apache.org/jira/browse/IGNITE-23261
                MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(clazz, MethodHandles.lookup());
                VarHandle varHandle = lookup.unreflectVarHandle(field);
                tuple.set(columnName, varHandle.get(obj));
            } catch (IllegalAccessException e) {
                throw new PojoConversionException("Cannot access field `" + fieldName + "`", e);
            } catch (NoSuchFieldException e) {
                // This shouldn't ever happen since we're using mapper which takes the fields from the same class
                throw new PojoConversionException("Field `" + fieldName + "` was not found", e);
            }
        }

        return tuple;
    }

    /**
     * Sets POJO fields from the Tuple. Supports public and private non-final non-static fields.
     *
     * @param obj POJO to fill.
     * @param tuple Tuple to get the values from.
     * @throws PojoConversionException If conversion failed.
     */
    public static void fromTuple(Object obj, Tuple tuple) {
        Class<?> clazz = obj.getClass();

        for (int i = 0; i < tuple.columnCount(); i++) {
            String columnName = tuple.columnName(i);
            String fieldName = columnName.substring(1, columnName.length() - 1);
            Field field = getField(clazz, fieldName);
            Object value = tuple.value(i);
            try {
                // TODO https://issues.apache.org/jira/browse/IGNITE-23261
                MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(clazz, MethodHandles.lookup());
                VarHandle varHandle = lookup.unreflectVarHandle(field);
                varHandle.set(obj, value);
            } catch (UnsupportedOperationException e) {
                throw new PojoConversionException("Field for the column `" + fieldName + "` is final", e);
            } catch (ClassCastException e) {
                throw new PojoConversionException("Incompatible types: Field `" + fieldName + "` has a type " + field.getType()
                        + " while deserializing type " + value.getClass(), e);
            } catch (IllegalAccessException e) {
                throw new PojoConversionException("Field for the column `" + fieldName + "` is not accessible", e);
            }
        }
    }

    private static Field getField(Class<?> clazz, String fieldName) {
        try {
            return clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            throw new PojoConversionException("Field `" + fieldName + "` was not found", e);
        }
    }

}
