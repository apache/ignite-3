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

package org.apache.ignite.internal.compute.util;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import org.jetbrains.annotations.Nullable;

/** Utility methods for generics. */
public class GenericUtils {

    private GenericUtils() {
    }

    /**
     * Calculates lookup class' generic types of the target class.
     * E.g. if {@code class MyList extends List<String>} is a target and {@code List<T>} is a lookup,
     * then the result is {@code {String.class}}.
     * <br>
     * Note that this method only works for declared classes, so it doesn't work in this case:
     * <pre><code>
     *  ArrayList&lt;String&gt; tmp = new ArrayList&lt;&gt;();
     *  getTypeArguments(tmp.getClass(), List.class);
     * </code></pre>
     *
     * @param target Target class.
     * @param lookup Class that target class extends or implements.
     * @return An array of types or {@code null} if failed to calculate.
     */
    @Nullable
    public static Type[] getTypeArguments(Class<?> target, Class<?> lookup) {
        if (!lookup.isAssignableFrom(target)) {
            return null;
        }

        return getTypeArguments(target, lookup, null);
    }

    /**
     * Recursively searches generic types through interfaces and superclasses of the target class.
     *
     * @param target Target class.
     * @param lookup Lookup class.
     * @param args Current arguments that are passed to the next level of the inheritance.
     * @return Array of types.
     */
    private static Type[] getTypeArguments(Class<?> target, Class<?> lookup, Type[] args) {
        if (target.equals(lookup)) {
            return args;
        }

        Type[] genericInterfaces = target.getGenericInterfaces();

        for (Type genericInterface : genericInterfaces) {
            Type[] types = getTypeArguments(genericInterface, lookup, args);

            if (types != null) {
                return types;
            }
        }

        return getTypeArguments(target.getGenericSuperclass(), lookup, args);
    }

    /**
     * Handles abstract type in search for the actual generic types.
     *
     * @param type Type.
     * @param lookup Lookup class.
     * @param args Current generic arguments.
     * @return Array of types.
     */
    private static Type[] getTypeArguments(Type type, Class<?> lookup, Type[] args) {
        if (type == null) {
            return null;
        }

        Type[] types = null;

        if (type instanceof Class<?>) {
            types = getTypeArguments((Class<?>) type, lookup, args);
        } else if (type instanceof ParameterizedType) {
            types = getTypeArguments((ParameterizedType) type, lookup, args);
        }

        return types;
    }

    /**
     * Tries to substitute type variables and continues searching for generic types of the target class.
     *
     * @param type Parameterized type.
     * @param lookup Lookup class.
     * @param args Current generic arguments.
     * @return Array of types.
     */
    private static Type[] getTypeArguments(ParameterizedType type, Class<?> lookup, Type[] args) {
        Type[] localArgs = type.getActualTypeArguments();

        if (args != null) {
            System.arraycopy(args, 0, localArgs, 0, args.length);
        }

        Class<?> rawType = (Class<?>) type.getRawType();
        return getTypeArguments(rawType, lookup, localArgs);
    }
}
