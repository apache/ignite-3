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

package org.apache.ignite.internal.network.serialization.marshal;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.internal.network.serialization.Classes;
import org.jetbrains.annotations.Nullable;

/**
 * Validations that are run before marshalling objects.
 */
class MarshallingValidations {
    private final Map<Class<?>, YesNo> whetherInnerClasses = new HashMap<>();
    private final Map<Class<?>, YesNo> whetherCapturingClosures = new HashMap<>();
    private final Map<Class<?>, YesNo> whetherNonSerializableLambdas = new HashMap<>();

    private final Function<Class<?>, YesNo> isInnerClassFunction = MarshallingValidations.this::isInnerClass;
    private final Function<Class<?>, YesNo> isCapturingClosureFunction = this::isCapturingClosure;
    private final Function<Class<?>, YesNo> isNonSerializableLambdaFunction = this::isNonSerializableLambda;

    void throwIfMarshallingNotSupported(@Nullable Object object) {
        if (object == null) {
            return;
        }
        if (Enum.class.isAssignableFrom(object.getClass())) {
            return;
        }

        Class<?> objectClass = object.getClass();

        if (cachingIsInnerClass(objectClass)) {
            throw new MarshallingNotSupportedException("Non-static inner class instances are not supported for marshalling: "
                    + objectClass);
        }
        if (cachingIsCapturingClosure(objectClass)) {
            throw new MarshallingNotSupportedException("Capturing nested class instances are not supported for marshalling: " + object);
        }
        if (cachingIsNonSerializableLambds(objectClass)) {
            throw new MarshallingNotSupportedException("Non-serializable lambda instances are not supported for marshalling: " + object);
        }
    }

    private boolean cachingIsInnerClass(Class<?> objectClass) {
        return whetherInnerClasses.computeIfAbsent(objectClass, isInnerClassFunction) == YesNo.YES;
    }

    private boolean cachingIsCapturingClosure(Class<?> objectClass) {
        return whetherCapturingClosures.computeIfAbsent(objectClass, isCapturingClosureFunction) == YesNo.YES;
    }

    private boolean cachingIsNonSerializableLambds(Class<?> objectClass) {
        return whetherNonSerializableLambdas.computeIfAbsent(objectClass, isNonSerializableLambdaFunction) == YesNo.YES;
    }

    private YesNo isInnerClass(Class<?> objectClass) {
        boolean innerClass = objectClass.getDeclaringClass() != null && !Modifier.isStatic(objectClass.getModifiers());
        return yesNo(innerClass);
    }

    private YesNo yesNo(boolean yes) {
        return yes ? YesNo.YES : YesNo.NO;
    }

    private YesNo isCapturingClosure(Class<?> objectClass) {
        for (Field field : objectClass.getDeclaredFields()) {
            if ((field.isSynthetic() && field.getName().equals("this$0"))
                    || field.getName().startsWith("arg$")) {
                return YesNo.YES;
            }
        }

        return YesNo.NO;
    }

    private YesNo isNonSerializableLambda(Class<?> objectClass) {
        boolean nonSerializableLambda = Classes.isLambda(objectClass) && !Classes.isSerializable(objectClass);
        return yesNo(nonSerializableLambda);
    }

    private enum YesNo {
        YES, NO
    }
}
