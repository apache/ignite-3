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

package org.apache.ignite.internal.sql.engine;

import org.jetbrains.annotations.Nullable;

/**
 * Schema-aware converter for sql row.
 *
 * @param <T> Column value type.
 * @param <R> Result type.
 */
@FunctionalInterface
public interface SchemaAwareConverter<T, R> {
    /**
     * Applies this function to the given arguments.
     *
     * @param index Index of column to convert.
     * @param value Column value to be converted.
     * @return Converted value.
     */
    @Nullable R convert(int index, @Nullable T value);
}
