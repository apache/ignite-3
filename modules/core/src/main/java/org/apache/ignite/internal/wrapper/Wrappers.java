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

package org.apache.ignite.internal.wrapper;

import org.jetbrains.annotations.Nullable;

/**
 * Utils for unwrapping {@link Wrapper} instances.
 */
public class Wrappers {
    /**
     * Unwraps an object (that is not {@code null}). Either invokes {@link Wrapper#unwrap(Class)} if it's a Wrapper, or tries to cast
     * directly otherwise.
     *
     * @param object Object to unwrap.
     * @param classToUnwrap Class which is to be unwrapped.
     */
    public static <T> T unwrap(Object object, Class<T> classToUnwrap) {
        assert object != null : "Object to unwrap is null";

        if (object instanceof Wrapper) {
            return ((Wrapper) object).unwrap(classToUnwrap);
        }

        return classToUnwrap.cast(object);
    }

    /**
     * Unwraps an object or returns {@code null} if it's {@code null}.
     *
     * @param object Object to unwrap.
     * @param classToUnwrap Class which is to be unwrapped.
     * @see #unwrap(Object, Class)
     */
    public static @Nullable <T> T unwrapNullable(@Nullable Object object, Class<T> classToUnwrap) {
        if (object == null) {
            return null;
        }

        return unwrap(object, classToUnwrap);
    }
}
