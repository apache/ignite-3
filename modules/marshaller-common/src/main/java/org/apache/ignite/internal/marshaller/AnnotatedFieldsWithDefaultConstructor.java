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

import static java.lang.invoke.MethodType.methodType;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import org.apache.ignite.lang.MarshallerException;

class AnnotatedFieldsWithDefaultConstructor implements Creator.Annotated {
    private final MethodHandle mhNoArgs;

    AnnotatedFieldsWithDefaultConstructor(Constructor<?> defaultCtor) {
        assert defaultCtor.getParameterCount() == 0;
        this.mhNoArgs = unreflect(defaultCtor);
    }

    @Override
    public Object createFrom(FieldAccessor[] accessors, MarshallerReader reader) {
        try {
            Object instance = mhNoArgs.invokeExact();

            for (int fldIdx = 0; fldIdx < accessors.length; fldIdx++) {
                accessors[fldIdx].read(reader, instance);
            }

            return instance;
        } catch (MarshallerException e) {
            throw e;
        } catch (Throwable e) {
            throw new IllegalArgumentException("Failed to instantiate class", e);
        }
    }

    private static MethodHandle unreflect(Constructor<?> defaultCtor) {
        try {
            defaultCtor.setAccessible(true);
            return MethodHandles.lookup()
                    .unreflectConstructor(defaultCtor)
                    .asType(methodType(Object.class));
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Failed to create method handle from constructor", e);
        }
    }
}
