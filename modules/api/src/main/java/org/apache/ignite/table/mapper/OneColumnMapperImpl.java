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

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Maps an entire object of the {@link ObjectT} type to a single column.
 *
 * @param <ObjectT> Target type.
 */
class OneColumnMapperImpl<ObjectT> implements OneColumnMapper<ObjectT> {

    /** Cache of one column mappers. */
    private static final ConcurrentHashMap<CacheKey, OneColumnMapper<?>> CACHE = new ConcurrentHashMap<>();

    /** Target type. */
    private final Class<ObjectT> targetType;

    /** Column name. */
    private final String mappedColumn;

    /** Converter. */
    private final TypeConverter<ObjectT, ?> converter;

    /**
     * Constructor.
     *
     * @param targetType Target type.
     * @param mappedColumn Mapped column.
     * @param converter Column converter.
     */
    private OneColumnMapperImpl(Class<ObjectT> targetType, @Nullable String mappedColumn, @Nullable TypeConverter<ObjectT, ?> converter) {
        this.targetType = targetType;
        this.mappedColumn = IgniteNameUtils.parseSimpleName(mappedColumn);
        this.converter = converter;
    }

    /**
     * Returns an instance of a {@link OneColumnMapper} for the given target type.
     *
     * @param targetType Target type.
     * @param mappedColumn Mapped column.
     * @param converter Column converter.
     * @retur nMapper.
     */
    static <ObjectT> OneColumnMapper<ObjectT> getInstance(
            Class<ObjectT> targetType,
            @Nullable String mappedColumn,
            @Nullable TypeConverter<ObjectT, ?> converter) {

        if (converter == null) {
            return (OneColumnMapper<ObjectT>) CACHE.computeIfAbsent(new CacheKey(targetType, mappedColumn), (k) -> {
                return new OneColumnMapperImpl<>((Class<Object>) k.type, k.column, null);
            });
        } else {
            return new OneColumnMapperImpl<>(targetType, mappedColumn, converter);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Class<ObjectT> targetType() {
        return targetType;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String mappedColumn() {
        return mappedColumn;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable TypeConverter<ObjectT, ?> converter() {
        return converter;
    }

    private static final class CacheKey {

        private final Class<?> type;

        private final String column;

        CacheKey(Class<?> type, @Nullable String column) {
            this.type = type;
            this.column = column;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(type, cacheKey.type) && Objects.equals(column, cacheKey.column);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, column);
        }
    }
}
