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

import static org.apache.ignite.table.mapper.RecordSupportInternal.getCanonicalConstructor;
import static org.apache.ignite.table.mapper.RecordSupportInternal.isRecord;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Creates object during unmarshalling. It's class either may have default constructor with fields annotated with {@code @Column}
 * or record canonical constructor with parameters annotated with {@code @Column}.
 */
@FunctionalInterface
interface Creator {

    Object createInstance(FieldAccessor[] accessors, MarshallerReader reader);

    static Creator of(Class<?> clazz) {
        try {
            if (isRecord(clazz)) {
                Constructor<?> canonicalCtor = getCanonicalConstructor(clazz);
                return new CreatorFromAnnotatedConstructorParameters(canonicalCtor);
            } else {
                Constructor<?> defaultCtor = clazz.getDeclaredConstructor();
                return new CreatorFromAnnotatedFieldsWithDefaultConstructor(defaultCtor);
            }
        } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException | IllegalAccessException ex) {
            throw new IllegalArgumentException("Could not find default (no-args) or canonical (record) constructor for " + clazz, ex);
        }
    }
}
