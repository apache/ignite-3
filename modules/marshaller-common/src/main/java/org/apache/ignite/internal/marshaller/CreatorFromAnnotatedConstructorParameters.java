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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.lang.util.IgniteNameUtils.parseIdentifier;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.catalog.annotations.Column;
import org.apache.ignite.lang.MarshallerException;
import org.jetbrains.annotations.NotNull;

class CreatorFromAnnotatedConstructorParameters implements Creator {
    private final MethodHandle mhFixedArity;
    private final List<String> argsColumnNames;

    CreatorFromAnnotatedConstructorParameters(Constructor<?> ctor) {
        assert ctor.getParameterCount() > 0;
        this.argsColumnNames = argsNames(ctor);
        this.mhFixedArity = unreflect(ctor);
    }

    @Override
    public Object createInstance(FieldAccessor[] accessors, MarshallerReader reader) {
        try {
            NamedArguments args = readArgs(accessors, reader);
            return mhFixedArity.invokeWithArguments(args.values);
        } catch (MarshallerException e) {
            throw e;
        } catch (Throwable e) {
            throw new IllegalArgumentException("Failed to instantiate class", e);
        }
    }

    private static List<String> argsNames(Constructor<?> ctor) {
        Parameter[] creatorParams = ctor.getParameters();

        if (Arrays.stream(creatorParams).anyMatch(p -> !p.isAnnotationPresent(Column.class))) {
            throw new IllegalArgumentException("All constructor parameters must be annotated with @Column");
        }

        return Arrays.stream(creatorParams)
                .map(p -> p.getAnnotation(Column.class))
                .map(column -> {
                    String columnName = column.value();
                    if (columnName.isBlank()) {
                        throw new IllegalArgumentException("@Column name can not be blank in constructor declaration");
                    }
                    return parseIdentifier(columnName);
                })
                .collect(toList());
    }

    private static MethodHandle unreflect(Constructor<?> ctor) {
        try {
            ctor.setAccessible(true);
            return MethodHandles.lookup()
                    .unreflectConstructor(ctor)
                    .asFixedArity();
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Failed to create method handle from constructor", e);
        }
    }

    private @NotNull NamedArguments readArgs(FieldAccessor[] accessors, MarshallerReader reader) {
        NamedArguments args = new NamedArguments(argsColumnNames);

        for (int fldIdx = 0; fldIdx < accessors.length; fldIdx++) {
            FieldAccessor a = accessors[fldIdx];
            Object val = a.read(reader); // pojo field order doesn't matter for current reader impl
            args.add(a.getColumnName(), val);
        }
        return args;
    }

    private static class NamedArguments {
        private final List<String> names;
        private final Object[] values;

        private NamedArguments(List<String> names) {
            this.names = names;
            this.values = new Object[names.size()];
        }

        void add(String name, Object value) {
            int idx = names.indexOf(name);
            if (idx < 0) {
                return;
            }
            values[idx] = value;
        }
    }
}
