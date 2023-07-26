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

package org.apache.ignite.serialization.descriptor;

import static org.apache.ignite.serialization.descriptor.ReflectionUtils.canonicalize;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public abstract class GenericClassDescriptor<T> implements TypeDescriptor {
    private final Type type;

    protected GenericClassDescriptor() {
        this.type = getTypeTokenTypeArgument();
    }

    private Type getTypeTokenTypeArgument() {
        Type superclass = getClass().getGenericSuperclass();
        if (superclass instanceof ParameterizedType) {
            ParameterizedType parameterized = (ParameterizedType) superclass;
            if (parameterized.getRawType() == GenericClassDescriptor.class) {
                return canonicalize(parameterized.getActualTypeArguments()[0]);
            }
        }

        throw new IllegalStateException("Must only create direct subclasses of GenericClassDescriptor");
    }

    @Override
    public Type type() {
        return type;
    }
}
