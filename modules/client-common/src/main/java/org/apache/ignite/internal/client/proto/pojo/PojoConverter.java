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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/** Converts POJO to Tuple and back. */
public class PojoConverter {

    /**
     * Converts POJO to Tuple. Supports public non-static fields, public non-static getter methods starting with "get" (or "is" for boolean
     * type) followed by the capital letter.
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

        Map<String, Getter> getters = new HashMap<>();

        for (Field field : clazz.getDeclaredFields()) {
            if (isSupportedType(field.getType())) {
                int modifiers = field.getModifiers();
                if (Modifier.isPublic(modifiers) && !Modifier.isStatic(modifiers)) {
                    getters.put(field.getName(), () -> field.get(obj));
                }
            }
        }

        for (Method method : clazz.getDeclaredMethods()) {
            if (isSupportedType(method.getReturnType()) && method.getParameterCount() == 0) {
                String getterName = getterName(method);
                if (getterName != null) {
                    int modifiers = method.getModifiers();
                    if (Modifier.isPublic(modifiers) && !Modifier.isStatic(modifiers)) {
                        getters.put(getterName, () -> method.invoke(obj));
                    }
                }
            }
        }

        Tuple tuple = Tuple.create();

        for (Entry<String, Getter> getter : getters.entrySet()) {
            try {
                // Name needs to be quoted to keep the case
                String columnName = "\"" + getter.getKey() + "\"";
                tuple.set(columnName, getter.getValue().get());
            } catch (InvocationTargetException e) {
                throw new PojoConversionException("Getter for field `" + getter.getKey() + "` has thrown an exception", e);
            } catch (IllegalAccessException ignored) {
                // Skip inaccessible fields
            }
        }

        if (tuple.columnCount() > 0) {
            return tuple;
        }

        throw new PojoConversionException("Class " + clazz.getName() + " doesn't contain any marshallable fields");
    }

    /**
     * Sets POJO fields from the Tuple. Supports public non-final non-static fields, public non-static setter methods starting with "set"
     * followed by the capital letter.
     *
     * @param obj POJO to fill.
     * @param tuple Tuple to get the values from.
     * @throws PojoConversionException If conversion failed.
     */
    public static void fromTuple(Object obj, Tuple tuple) {
        Class<?> clazz = obj.getClass();

        Map<String, Setter> setters = new HashMap<>();

        for (Field field : clazz.getDeclaredFields()) {
            if (isSupportedType(field.getType())) {
                int modifiers = field.getModifiers();
                if (Modifier.isPublic(modifiers) && !Modifier.isStatic(modifiers) && !Modifier.isFinal(modifiers)) {
                    setters.put(field.getName(), value -> field.set(obj, value));
                }
            }
        }

        for (Method method : clazz.getDeclaredMethods()) {
            if (method.getParameterCount() == 1 && isSupportedType(method.getParameterTypes()[0])) {
                String setterName = setterName(method);
                if (setterName != null) {
                    int modifiers = method.getModifiers();
                    if (Modifier.isPublic(modifiers) && !Modifier.isStatic(modifiers)) {
                        setters.put(setterName, value -> method.invoke(obj, value));
                    }
                }
            }
        }

        for (int i = 0; i < tuple.columnCount(); i++) {
            String columnName = tuple.columnName(i);
            String fieldName = columnName.substring(1, columnName.length() - 1);
            if (!setters.containsKey(fieldName)) {
                throw new PojoConversionException("No setter found for the column `" + fieldName + "`");
            }
            try {
                setters.get(fieldName).set(tuple.value(i));
            } catch (InvocationTargetException e) {
                throw new PojoConversionException("Setter for field `" + fieldName + "` has thrown an exception", e);
            } catch (IllegalAccessException e) {
                throw new PojoConversionException("Setter for the column `" + fieldName + "` is not accessible", e);
            }
        }
    }

    private static @Nullable String getterName(Method method) {
        String methodName = method.getName();
        if ((method.getReturnType() == Boolean.class || method.getReturnType() == Boolean.TYPE)
                && methodName.startsWith("is") && methodName.length() > 2 && Character.isUpperCase(methodName.charAt(2))) {
            return decapitalize(methodName.substring(2));
        }
        if (methodName.startsWith("get") && methodName.length() > 3 && Character.isUpperCase(methodName.charAt(3))) {
            return decapitalize(methodName.substring(3));
        }
        return null;
    }

    private static @Nullable String setterName(Method method) {
        String methodName = method.getName();
        if (methodName.startsWith("set") && methodName.length() > 3 && Character.isUpperCase(methodName.charAt(3))) {
            return decapitalize(methodName.substring(3));
        }
        return null;
    }

    private static String decapitalize(String str) {
        return str.substring(0, 1).toLowerCase() + str.substring(1);
    }

    @FunctionalInterface
    private interface Getter {
        @Nullable Object get() throws IllegalAccessException, InvocationTargetException;
    }

    @FunctionalInterface
    private interface Setter {
        void set(@Nullable Object t) throws IllegalAccessException, InvocationTargetException;
    }

    private static boolean isSupportedType(Class<?> fieldClass) {
        return fieldClass.isPrimitive()
                || Number.class.isAssignableFrom(fieldClass)
                || fieldClass == Boolean.class
                || fieldClass.isArray() && fieldClass.getComponentType() == Byte.TYPE
                || fieldClass == String.class || fieldClass == BigDecimal.class || fieldClass == LocalDate.class
                || fieldClass == LocalTime.class || fieldClass == LocalDateTime.class || fieldClass == Instant.class
                || fieldClass == UUID.class || fieldClass == Period.class || fieldClass == Duration.class;
    }
}
