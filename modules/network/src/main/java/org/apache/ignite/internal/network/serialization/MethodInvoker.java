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

package org.apache.ignite.internal.network.serialization;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Invokes a method using either the corresponding {@link MethodHandle} or the good old {@link Method} if it's impossible
 * to obtain a MethodHandle.
 * The impossibility to obtain a MethodHandle can occur due to the method of interest being declared inside java.lang.invoke package.
 */
class MethodInvoker {
    @Nullable
    private final MethodHandle methodHandle;
    @Nullable
    private final Method method;

    MethodInvoker(String methodName, Class<?> declaringClass, MethodType methodType, MethodType castType, Class<?>... parameterTypes)
            throws ReflectiveOperationException {
        MethodHandle handle = methodHandleOrNull(methodName, declaringClass, methodType, castType);

        if (handle != null) {
            methodHandle = handle;
            method = null;
        } else {
            methodHandle = null;
            method = findMethod(methodName, declaringClass, parameterTypes);
        }
    }

    @Nullable
    private MethodHandle methodHandleOrNull(String methodName, Class<?> declaringClass, MethodType methodType, MethodType castType)
            throws ReflectiveOperationException {
        try {
            return findMethodHandle(methodName, declaringClass, methodType, castType);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private MethodHandle findMethodHandle(String methodName, Class<?> declaringClass, MethodType methodType, MethodType castType)
            throws ReflectiveOperationException {
        MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(declaringClass, MethodHandles.lookup());
        return lookup.findVirtual(declaringClass, methodName, methodType)
                .asType(castType);
    }

    private Method findMethod(String methodName, Class<?> declaringClass, Class<?>[] parameterTypes)
            throws ReflectiveOperationException {
        Method method = declaringClass.getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method;
    }

    boolean hasMethodHandle() {
        return methodHandle != null;
    }

    MethodHandle methodHandle() {
        return Objects.requireNonNull(methodHandle);
    }

    @Nullable
    Object invokeWithMethod(Object target) {
        Objects.requireNonNull(method);

        // avoid empty array allocation
        Object[] args = null;

        try {
            return method.invoke(target, args);
        } catch (ReflectiveOperationException e) {
            throw new ReflectionException("Cannot invoke a method", e);
        }
    }

    @Nullable
    Object invokeWithMethod(Object target, Object... parameters) {
        Objects.requireNonNull(method);

        try {
            return method.invoke(target, parameters);
        } catch (ReflectiveOperationException e) {
            throw new ReflectionException("Cannot invoke a method", e);
        }
    }
}
