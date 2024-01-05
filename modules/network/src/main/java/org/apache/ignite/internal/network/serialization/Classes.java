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

package org.apache.ignite.internal.network.serialization;

import java.io.Externalizable;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.jetbrains.annotations.Nullable;

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

    /**
     * Returns {@code true} if the given class is an enum or represents an anonymous enum constant, but is not exactly {@link Enum}.
     *
     * @param objectClass class to check
     * @return {@code true} if the given class is an enum or represents an anonymous enum constant
     */
    public static boolean isRuntimeEnum(Class<?> objectClass) {
        return Enum.class.isAssignableFrom(objectClass) && objectClass != Enum.class;
    }

    /**
     * Returns enum class as it appears in the source code. If the class is an anonymous subclass os an enum, we return its superclass,
     * otherwise we just return its class.
     *
     * @param enumClass enum class
     * @return enum class as it appears in the source code
     */
    public static Class<?> enumClassAsInSourceCode(Class<?> enumClass) {
        assert enumClass != null;
        assert Enum.class.isAssignableFrom(enumClass);

        if (!enumClass.isEnum()) {
            // this is needed for enums where members are represented with anonymous classes
            enumClass = enumClass.getSuperclass();
        }
        return enumClass;
    }

    /**
     * Returns {@code true} if a field (or array item) of the described class can only host (at runtime) instances of this type
     * (and not subtypes), so the runtime marshalling type is known upfront. This is also true for enums, even though technically
     * their values might have subtypes; but we serialize them using their names, so we still treat the type as known upfront.
     *
     * @return {@code true} if a field (or array item) of the described class can only host (at runtime) instances of the concrete type
     *     that is known upfront
     */
    public static boolean isRuntimeTypeKnownUpfront(Class<?> clazz) {
        if (clazz.isArray()) {
            return isRuntimeTypeKnownUpfront(clazz.getComponentType());
        }

        if (clazz == String.class) {
            // A String may be represented with more than one built-in type, so we don't know the type upfront.
            return false;
        }

        return clazz.isPrimitive() || Modifier.isFinal(clazz.getModifiers()) || isRuntimeEnum(clazz);
    }

    /**
     * Returns whether the declared class unambigously defines the class of the object that will be written when serializing an instance of
     * the original class. This means that the runtime type is known upfront and that it does not have a writeReplace()
     * method (as it can return any replacement).
     *
     * @param clazz Class to check.
     * @see #isRuntimeTypeKnownUpfront(Class)
     */
    public static boolean isSerializationTypeKnownUpfront(Class<?> clazz) {
        return isRuntimeTypeKnownUpfront(clazz) && !typeCanBeReplacedDuringSerialization(clazz);
    }

    private static boolean typeCanBeReplacedDuringSerialization(Class<?> clazz) {
        return Serializable.class.isAssignableFrom(clazz)
                && !Externalizable.class.isAssignableFrom(clazz)
                && hasWriteReplace(clazz);
    }

    /**
     * Returns whether the given class defines a writeReplace() method.
     *
     * @param clazz Class to check.
     */
    static boolean hasWriteReplace(Class<?> clazz) {
        return getWriteReplace(clazz) != null;
    }

    /**
     * Gets a method with the signature
     * {@code ANY-ACCESS-MODIFIER Object writeReplace() throws ObjectStreamException}.
     *
     * @param clazz Class.
     * @return Method.
     */
    @Nullable
    private static Method getWriteReplace(Class<?> clazz) {
        try {
            return clazz.getDeclaredMethod("writeReplace");
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    private Classes() {
    }
}
