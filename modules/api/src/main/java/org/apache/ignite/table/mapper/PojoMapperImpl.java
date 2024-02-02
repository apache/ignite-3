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
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps object fields to columns by name.
 *
 * @param <T> Target type.
 */
class PojoMapperImpl<T> implements PojoMapper<T> {

    /** Cache of pojo mappers. */
    private static final ConcurrentHashMap<CacheKey, PojoMapper<?>> CACHE = new ConcurrentHashMap<>();

    /** Target type. */
    private final Class<T> targetType;

    /** Column-to-field name mapping. */
    private final Map<String, String> mapping;

    private final Map<String, TypeConverter<?, ?>> converters;

    /**
     * Creates a mapper for a given type.
     *
     * @param targetType Target type.
     * @param mapping    Column-to-field name mapping.
     * @param converters Column converters.
     */
    private PojoMapperImpl(Class<T> targetType, Map<String, String> mapping, Map<String, TypeConverter<?, ?>> converters) {
        this.converters = converters;
        if (Objects.requireNonNull(mapping).isEmpty()) {
            throw new IllegalArgumentException("Empty mapping isn't allowed.");
        }

        this.targetType = targetType;
        this.mapping = mapping;
    }

    /**
     * Returns an instance of a {@link PojoMapper} for the given target type.
     *
     * @param targetType Target type.
     * @param columnToFields Column-to-field name mapping.
     * @param converters Column converters.
     * @return Mapper.
     */
    static <T> PojoMapper<T> getInstance(
            Class<T> targetType,
            Map<String, String> columnToFields,
            Map<String, TypeConverter<?, ?>> converters) {

        if (converters.isEmpty()) {
            CacheKey cacheKey = new CacheKey(targetType, columnToFields);

            return (PojoMapper<T>) CACHE.computeIfAbsent(cacheKey, (k) -> {
                return new PojoMapperImpl<>((Class<Object>) k.type, k.columnToFields, Collections.emptyMap());
            });
        } else {
            return new PojoMapperImpl<>(targetType, columnToFields, converters);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Class<T> targetType() {
        return targetType;
    }

    /** {@inheritDoc} */
    @Override
    public String fieldForColumn(String columnName) {
        return mapping.get(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<String> fields() {
        return mapping.values();
    }

    /** {@inheritDoc} */
    @Override
    public <FieldT, ColumnT> TypeConverter<FieldT, ColumnT> converterForColumn(String columnName) {
        return (TypeConverter<FieldT, ColumnT>) converters.get(columnName);
    }

    private static final class CacheKey {

        private final Class<?> type;

        private final Map<String, String> columnToFields;

        CacheKey(Class<?> type,  Map<String, String> columnToFields) {
            this.type = type;
            this.columnToFields = columnToFields;
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
            return Objects.equals(type, cacheKey.type) && Objects.equals(columnToFields, cacheKey.columnToFields);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, columnToFields);
        }
    }
}
