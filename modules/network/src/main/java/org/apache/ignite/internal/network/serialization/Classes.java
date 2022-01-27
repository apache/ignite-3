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

import java.io.Externalizable;
import java.io.Serializable;

/**
 * Utilities to work with classes.
 */
public class Classes {
    /**
     * Returns {@code true} if the given class implements {@link Serializable}.
     *
     * @param objectClass class to check
     * @return {@code true} if the given class implements {@link Serializable}
     */
    public static boolean isSerializable(Class<?> objectClass) {
        return Serializable.class.isAssignableFrom(objectClass);
    }

    /**
     * Returns {@code true} if the given class is defined by a lambda expression.
     *
     * @param objectClass class to check
     * @return {@code true} if the given class is defined by a lambda expression
     */
    public static boolean isLambda(Class<?> objectClass) {
        return !objectClass.isPrimitive() && !objectClass.isArray()
                && !objectClass.isAnonymousClass() && !objectClass.isLocalClass()
                && objectClass.isSynthetic()
                && classCannotBeLoadedByName(objectClass);
    }

    private static boolean classCannotBeLoadedByName(Class<?> objectClass) {
        try {
            Class.forName(objectClass.getName());
            return false;
        } catch (ClassNotFoundException e) {
            return true;
        }
    }

    /**
     * Returns {@code true} if the given class implements {@link Externalizable}.
     *
     * @param objectClass class to check
     * @return {@code true} if the given class implements {@link Externalizable}
     */
    public static boolean isExternalizable(Class<?> objectClass) {
        return Externalizable.class.isAssignableFrom(objectClass);
    }

    private Classes() {
    }
}
