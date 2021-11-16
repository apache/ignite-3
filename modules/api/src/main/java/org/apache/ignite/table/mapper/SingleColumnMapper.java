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

package org.apache.ignite.table.mapper;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Simple mapper implementation that maps a whole object (of the target type) to a single column.
 *
 * @param <T> Target type.
 */
class SingleColumnMapper<T> implements Mapper<T> {
    /** Target type. */
    private final Class<T> targetType;

    /** Column name. */
    private final String mappedColumn;

    SingleColumnMapper(Class<T> targetType, @NotNull String mappedColumn) {
        this.targetType = targetType;
        this.mappedColumn = mappedColumn;
    }

    /** {@inheritDoc} */
    @Override
    public Class<T> targetType() {
        return targetType;
    }

    /** {@inheritDoc} */
    @Override
    public String mappedColumn() {
        return mappedColumn;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String mappedField(@NotNull String columnName) {
        throw new UnsupportedOperationException("Not intended for individual fields mapping.");
    }
}
