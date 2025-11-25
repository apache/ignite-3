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

package org.apache.ignite.internal.marshaller;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.Nullable;

class Creator<T> {
    private static final IgniteLogger LOG = Loggers.forClass(Creator.class);

    final Class<T> clazz;
    private final FieldAccessor[] accessors;
    private @Nullable Annotated defaultConstructor;
    private @Nullable Annotated creatorConstructor;

    Creator(Class<T> clazz, FieldAccessor[] accessors) {
        this.clazz = clazz;
        this.accessors = accessors;
        findCreators(clazz);
    }

    @FunctionalInterface
    interface Annotated {
        Object createFrom(FieldAccessor[] accessors, MarshallerReader reader);
    }

    Object createFrom(MarshallerReader reader) {
        if (creatorConstructor != null) {
            return creatorConstructor.createFrom(accessors, reader);
        }
        if (defaultConstructor != null) {
            return defaultConstructor.createFrom(accessors, reader);
        }

        throw new IllegalArgumentException("Could not find default (no-args) or canonical (record) constructor for " + clazz.getName());
    }

    private void findCreators(Class<T> clazz) {
        Constructor<?> defaultCtor = null;
        Constructor<?> creatorCtor = null;

        Constructor<?>[] ctors = clazz.getDeclaredConstructors();
        for (Constructor<?> ctor : ctors) {
            if (ctor.isSynthetic()) {
                continue;
            }
            if (ctor.getParameterCount() == 0) {
                defaultCtor = ctor;
                continue;
            }

            if (isCanonicalConstructor(clazz, ctor)) {
                creatorCtor = ctor;
            }
        }

        if (defaultCtor != null) {
            this.defaultConstructor = new AnnotatedFieldsWithDefaultConstructor(defaultCtor);
        }
        if (creatorCtor != null) {
            this.creatorConstructor = new AnnotatedConstructor(creatorCtor);
        }
        if (this.defaultConstructor == null && this.creatorConstructor == null) {
            throw new IllegalArgumentException("Could not find default (no-args) or canonical (record) constructor for " + clazz);
        }
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
    private static boolean isCanonicalConstructor(Class<?> clazz, Constructor<?> ctor) {
        if (Runtime.version().version().get(0) < 14) {
            return false;
        }
        try {
            Method getRecordComponentsMtd = Class.class.getDeclaredMethod("getRecordComponents");

            Object[] recordComponents = (Object[]) getRecordComponentsMtd.invoke(clazz);

            if (recordComponents == null || recordComponents.length != ctor.getParameterCount()) {
                return false;
            }

            Method getTypeMtd = Class.forName("java.lang.reflect.RecordComponent")
                    .getDeclaredMethod("getType");

            Object[] types = new Object[recordComponents.length];
            for (int i = 0; i < recordComponents.length; i++) {
                types[i] = getTypeMtd.invoke(recordComponents[i]);
            }

            if (Arrays.equals(types, ctor.getParameterTypes())) {
                return true;
            }
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Canonical constructor introspection failed", e);
            }
            return false;
        }

        return false;
    }
}
