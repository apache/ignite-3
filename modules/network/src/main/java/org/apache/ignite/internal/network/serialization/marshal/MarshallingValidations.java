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

package org.apache.ignite.internal.network.serialization.marshal;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.ignite.internal.network.serialization.Classes;
import org.jetbrains.annotations.Nullable;

/**
 * Validations that are run before marshalling objects.
 */
class MarshallingValidations {
    private final Map<Class<?>, Marshallability> marshallabilityMap = new ConcurrentHashMap<>();

    private final Function<Class<?>, Marshallability> marshallabilityFunction = this::marshallability;

    void throwIfMarshallingNotSupported(@Nullable Object object) {
        if (object == null) {
            return;
        }

        Class<?> objectClass = object.getClass();

        Marshallability marshallability = marshallabilityMap.computeIfAbsent(objectClass, marshallabilityFunction);

        switch (marshallability) {
            case INNER_CLASS:
                throw new MarshallingNotSupportedException("Non-static inner class instances are not supported for marshalling: "
                        + objectClass);
            case CAPTUREING_CLOSURE:
                throw new MarshallingNotSupportedException("Capturing nested class instances are not supported for marshalling: " + object);
            case NON_SERIALIZABLE_LAMBDA:
                throw new MarshallingNotSupportedException("Non-serializable lambda instances are not supported for marshalling: "
                        + object);
            default:
                // do nothing
        }
    }

    private Marshallability marshallability(Class<?> objectClass) {
        if (Enum.class.isAssignableFrom(objectClass)) {
            return Marshallability.OK;
        }

        if (isInnerClass(objectClass)) {
            return Marshallability.INNER_CLASS;
        }

        if (isCapturingClosure(objectClass)) {
            return Marshallability.CAPTUREING_CLOSURE;
        }

        if (isNonSerializableLambda(objectClass)) {
            return Marshallability.NON_SERIALIZABLE_LAMBDA;
        }

        return Marshallability.OK;
    }

    private boolean isInnerClass(Class<?> objectClass) {
        return !Modifier.isStatic(objectClass.getModifiers()) && objectClass.getDeclaringClass() != null;
    }

    private boolean isCapturingClosure(Class<?> objectClass) {
        for (Field field : objectClass.getDeclaredFields()) {
            if ((field.isSynthetic() && field.getName().equals("this$0"))
                    || field.getName().startsWith("arg$")) {
                return true;
            }
        }

        return false;
    }

    private boolean isNonSerializableLambda(Class<?> objectClass) {
        return Classes.isLambda(objectClass) && !Classes.isSerializable(objectClass);
    }

    private enum Marshallability {
        OK,
        INNER_CLASS,
        CAPTUREING_CLOSURE,
        NON_SERIALIZABLE_LAMBDA
    }
}
