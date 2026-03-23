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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/** Enables access to {@link RecordSupport} without reflection to provides utility methods to support records in Java 11. */
public final class RecordSupportInternal {
    private RecordSupportInternal() {
        // No-op.
    }

    /**
     * Java 11 compatible, reflection based alternative, that returns true if and only if this class is a record class.
     *
     * <p>Without reflection:
     *
     * {@snippet lang="java" :
     * Class<?> clazz;
     * clazz.isRecord();
     * }
     */
    public static boolean isRecord(Class<?> clazz) throws IllegalAccessException {
        return RecordSupport.isRecord(clazz);
    }

    /**
     * Java 11 compatible, reflection based alternative to find the record canonical constructor.
     *
     * <p>Without reflection:
     *
     * {@snippet lang="java" :
     * static <T extends Record> Constructor<T> getCanonicalConstructor(Class<T> cls)
     *     throws NoSuchMethodException {
     *   Class<?>[] paramTypes =
     *     Arrays.stream(cls.getRecordComponents())
     *           .map(RecordComponent::getType)
     *           .toArray(Class<?>[]::new);
     *   return cls.getDeclaredConstructor(paramTypes);
     * }}
     */
    public static <T> Constructor<T> getCanonicalConstructor(Class<T> clazz)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, ClassNotFoundException {
        Method getRecordComponentsMtd = Class.class.getDeclaredMethod("getRecordComponents");

        Object[] recordComponents = (Object[]) getRecordComponentsMtd.invoke(clazz);
        if (recordComponents == null) {
            throw new IllegalArgumentException("'" + clazz.getName() + "' does not seem to be a record.");
        }

        Method getTypeMtd = Class.forName("java.lang.reflect.RecordComponent")
                .getDeclaredMethod("getType");

        Class<?>[] types = new Class<?>[recordComponents.length];
        for (int i = 0; i < recordComponents.length; i++) {
            types[i] = (Class<?>) getTypeMtd.invoke(recordComponents[i]);
        }

        return clazz.getDeclaredConstructor(types);
    }
}
