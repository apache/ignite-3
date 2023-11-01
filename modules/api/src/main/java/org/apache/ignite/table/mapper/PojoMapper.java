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

package org.apache.ignite.table.mapper;

import java.util.Collection;
import org.jetbrains.annotations.Nullable;

/**
 * Maps fields of objects of type {@link T} to columns by name. Every mapped field must be of
 * a natively supported type. Alternatively, a converter must be provided.
 *
 * @param <T> Target type.
 * @see OneColumnMapper
 */
public interface PojoMapper<T> extends Mapper<T> {
    /**
     * Returns a field name for a given column name when POJO individual fields are mapped to columns, otherwise fails.
     *
     * @param columnName Column name.
     * @return Field name or {@code null} if no field is mapped to the column.
     * @throws IllegalStateException If the entire object is mapped to a single column.
     */
    @Nullable String fieldForColumn(String columnName);

    /**
     * Returns all fields that should be mapped to columns.
     *
     * @return Field names.
     */
    Collection<String> fields();

    /**
     * Returns a type converter for a given column.
     *
     * @return Type converter or {@code null} if not set.
     */
    <FieldT, ColumnT> TypeConverter<FieldT, ColumnT> converterForColumn(String columnName);
}
