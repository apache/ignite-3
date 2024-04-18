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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;
import org.apache.ignite.table.mapper.Mapper;

/** Implementation of {@link MarshallersProvider}. */
public class ReflectionMarshallersProvider implements MarshallersProvider {
    /** Marshaller cache size for schema based marshallers (schema version, mapper, flags). */
    private static final int KV_CACHE_SIZE = 64;

    /** Marshaller cache size for column based marshallers (columns, mapper, flags). */
    private static final int PROJECTION_CACHE_SIZE = 64;

    /** Marshaller cache by schema. Cached by schema version. */
    private final MarshallerCache marshallerCache;

    /** Marshaller cache by an arbitrary columns. Cached by columns. */
    private final MarshallerCache projectionMarshallerCache;

    /** Constructor. */
    public ReflectionMarshallersProvider() {
        this.marshallerCache = new MarshallerCache(KV_CACHE_SIZE);
        this.projectionMarshallerCache = new MarshallerCache(PROJECTION_CACHE_SIZE);
    }

    /** {@inheritDoc} */
    @Override
    public Marshaller getKeysMarshaller(
            MarshallerSchema schema,
            Mapper<?> mapper,
            boolean requireAllFields,
            boolean allowUnmappedFields
    ) {

        MarshallerCacheKey key = new MarshallerCacheKey(
                schema.schemaVersion(), MarshallerType.KEY_ONLY, schema.keys(), mapper, requireAllFields, allowUnmappedFields
        );

        return marshallerCache.getOrAdd(key, k -> {
            return Marshaller.createMarshaller(schema.keys(), key.mapper, key.requireAllFields, key.allowUnmappedFields);
        });
    }

    /** {@inheritDoc} */
    @Override
    public Marshaller getValuesMarshaller(
            MarshallerSchema schema,
            Mapper<?> mapper,
            boolean requireAllFields,
            boolean allowUnmappedFields) {

        MarshallerCacheKey key = new MarshallerCacheKey(
                schema.schemaVersion(), MarshallerType.VALUE_ONLY, schema.values(), mapper, requireAllFields, allowUnmappedFields
        );

        return marshallerCache.getOrAdd(key, k -> {
            return Marshaller.createMarshaller(schema.values(), key.mapper, key.requireAllFields, key.allowUnmappedFields);
        });
    }

    /** {@inheritDoc} */
    @Override
    public Marshaller getRowMarshaller(
            MarshallerSchema schema,
            Mapper<?> mapper,
            boolean requireAllFields,
            boolean allowUnmappedFields
    ) {

        MarshallerCacheKey key = new MarshallerCacheKey(
                schema.schemaVersion(), MarshallerType.FULL_ROW, schema.row(), mapper, requireAllFields, allowUnmappedFields
        );

        return marshallerCache.getOrAdd(key, k -> {
            return Marshaller.createMarshaller(schema.row(), key.mapper, key.requireAllFields, key.allowUnmappedFields);
        });
    }

    /** {@inheritDoc} */
    @Override
    public Marshaller getMarshaller(
            MarshallerColumn[] columns,
            Mapper<?> mapper,
            boolean requireAllFields,
            boolean allowUnmappedFields
    ) {

        MarshallerCacheKey key = new MarshallerCacheKey(columns, mapper, requireAllFields, allowUnmappedFields);

        return projectionMarshallerCache.getOrAdd(key, k -> {
            return Marshaller.createMarshaller(k.columns, k.mapper, k.requireAllFields, k.allowUnmappedFields);
        });
    }

    private static class MarshallerCache {

        private final Cache<MarshallerCacheKey, Marshaller> cache;

        MarshallerCache(int maximumSize) {
            cache = Caffeine.newBuilder()
                    .maximumSize(maximumSize)
                    .build();
        }

        Marshaller getOrAdd(MarshallerCacheKey key, Function<MarshallerCacheKey, Marshaller> func) {
            return cache.get(key, func);
        }
    }

    private enum MarshallerType {
        KEY_ONLY,
        VALUE_ONLY,
        FULL_ROW
    }

    private static final class MarshallerCacheKey {
        private final int schemaVersion;

        private final Mapper<?> mapper;

        private final MarshallerColumn[] columns;

        private final MarshallerType type;

        private final boolean requireAllFields;

        private final boolean allowUnmappedFields;

        MarshallerCacheKey(
                int schemaVersion,
                MarshallerType type,
                MarshallerColumn[] columns,
                Mapper<?> mapper,
                boolean requireAllFields,
                boolean allowUnmappedFields
        ) {
            this.schemaVersion = schemaVersion;
            this.columns = columns;
            this.type = type;
            this.mapper = mapper;
            this.requireAllFields = requireAllFields;
            this.allowUnmappedFields = allowUnmappedFields;
        }

        MarshallerCacheKey(
                MarshallerColumn[] columns,
                Mapper<?> mapper,
                boolean requireAllFields,
                boolean allowUnmappedFields
        ) {
            this.schemaVersion = -1;
            this.columns = columns;
            this.mapper = mapper;
            this.type = null;
            this.requireAllFields = requireAllFields;
            this.allowUnmappedFields = allowUnmappedFields;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MarshallerCacheKey that = (MarshallerCacheKey) o;
            return schemaVersion == that.schemaVersion && requireAllFields == that.requireAllFields
                    && allowUnmappedFields == that.allowUnmappedFields && Objects.equals(mapper, that.mapper) && Arrays.equals(
                    columns, that.columns) && type == that.type;
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(schemaVersion, mapper, type, requireAllFields, allowUnmappedFields);
            result = 31 * result + Arrays.hashCode(columns);
            return result;
        }
    }
}
