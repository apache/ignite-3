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

package org.apache.ignite.internal.sql.engine.util;

import org.apache.ignite.internal.type.NativeTypes;

/**
 * A wrapper interface for non-comparable {@link NativeTypes native types} that implements {@link Comparable}.
 *
 * @param <T> a native type wrapper.
 */
public interface NativeTypeWrapper<T extends NativeTypeWrapper<T>> extends Comparable<T> {

    /** Returns a value of a native type. */
    Object get();

    /**
     * If the given value is an instance of a native type wrapper, this method returns a value of a native type.
     * Otherwise it returns an input value.
     */
    static Object unwrap(Object value) {
        if (value instanceof NativeTypeWrapper) {
            return ((NativeTypeWrapper<?>) value).get();
        } else {
            return value;
        }
    }
}
