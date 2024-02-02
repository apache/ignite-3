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

import static org.apache.ignite.internal.marshaller.FieldAccessor.createIdentityAccessor;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.internal.marshaller.FieldAccessor.IdentityAccessor;
import org.apache.ignite.internal.util.Factory;
import org.apache.ignite.internal.util.ObjectFactory;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.mapper.OneColumnMapper;
import org.apache.ignite.table.mapper.PojoMapper;
import org.apache.ignite.table.mapper.TypeConverter;
import org.jetbrains.annotations.Nullable;

/**
 * Marshaller.
 */
public abstract class Marshaller {

    /** Marshaller for key columns of a particular schema. Cached by schema version. */
    private static final MarshallerCache KEYS_MARSHALLER_CACHE = new MarshallerCache();

    /** Marshaller for value columns of a particular schema. Cached by schema version. */
    private static final MarshallerCache VALUES_MARSHALLER_CACHE = new MarshallerCache();

    /** Marshaller for all columns of a particular schema. Cached by schema version. */
    private static final MarshallerCache ROW_MARSHALLER_CACHE = new MarshallerCache();

    /** Marshaller for an arbitrary columns. Cached by columns. */
    private static final MarshallerCache PROJECTION_MARSHALLER_CACHE = new MarshallerCache();

    /**
     * Returns a marshaller for key columns of the given schema.
     *
     * @param schema Schema.
     * @param mapper Mapper.
     * @param requireAllFields If specified class should contain fields for all columns.
     * @param allowUnmappedFields Whether specified class can contain fields that are not mapped to columns.
     * @return Marshaller.
     */
    public static Marshaller getKeysMarshaller(
            MarshallerSchema schema,
            Mapper<?> mapper,
            boolean requireAllFields,
            boolean allowUnmappedFields) {

        MarshallerCacheKey key = new MarshallerCacheKey(schema.schemaVersion(), mapper, requireAllFields, allowUnmappedFields);

        return KEYS_MARSHALLER_CACHE.put(key, k -> {
            return createMarshaller(schema.keys(), key.mapper, key.requireAllFields, key.allowUnmappedFields);
        });
    }

    /**
     * Returns a marshaller for value columns of the given schema.
     *
     * @param schema Schema.
     * @param mapper Mapper.
     * @param requireAllFields If specified class should contain fields for all columns.
     * @param allowUnmappedFields Whether specified class can contain fields that are not mapped to columns.
     * @return Marshaller.
     */
    public static Marshaller getValuesMarshaller(
            MarshallerSchema schema,
            Mapper<?> mapper,
            boolean requireAllFields,
            boolean allowUnmappedFields) {

        MarshallerCacheKey key = new MarshallerCacheKey(schema.schemaVersion(), mapper, requireAllFields, allowUnmappedFields);

        return VALUES_MARSHALLER_CACHE.put(key, k -> {
            return createMarshaller(schema.values(), key.mapper, key.requireAllFields, key.allowUnmappedFields);
        });
    }

    /**
     * Returns a marshaller that includes both key and value columns of the given schema.
     *
     * @param schema Schema.
     * @param mapper Mapper.
     * @param requireAllFields If specified class should contain fields for all columns.
     * @param allowUnmappedFields Whether specified class can contain fields that are not mapped to columns.
     * @return Marshaller.
     */
    public static Marshaller getRowMarshaller(
            MarshallerSchema schema,
            Mapper<?> mapper,
            boolean requireAllFields,
            boolean allowUnmappedFields) {

        MarshallerCacheKey key = new MarshallerCacheKey(schema.schemaVersion(), mapper, requireAllFields, allowUnmappedFields);

        return ROW_MARSHALLER_CACHE.put(key, k -> {
            return createMarshaller(schema.row(), key.mapper, key.requireAllFields, key.allowUnmappedFields);
        });
    }

    /**
     * Returns a marshaller for the given columns.
     *
     * @param columns Columns.
     * @param mapper Mapper.
     * @param requireAllFields If specified class should contain fields for all columns.
     * @param allowUnmappedFields Whether specified class can contain fields that are not mapped to columns.
     * @return Marshaller.
     */
    public static Marshaller getMarshaller(
            MarshallerColumn[] columns,
            Mapper<?> mapper,
            boolean requireAllFields,
            boolean allowUnmappedFields) {

        MarshallerCacheKey key = new MarshallerCacheKey(columns, mapper, requireAllFields, allowUnmappedFields);

        return PROJECTION_MARSHALLER_CACHE.put(key, k -> {
            return createMarshaller(k.columns, k.mapper, k.requireAllFields, k.allowUnmappedFields);
        });
    }

    /**
     * Creates a marshaller for class.
     *
     * @param cols Columns.
     * @param mapper Mapper.
     * @param requireAllFields If specified class should contain fields for all columns.
     * @param allowUnmappedFields Whether specified class can contain fields that are not mapped to columns.
     * @return Marshaller.
     */
    private static Marshaller createMarshaller(
            MarshallerColumn[] cols,
            Mapper<?> mapper,
            boolean requireAllFields,
            boolean allowUnmappedFields) {
        if (mapper.targetType() == Void.class) {
            return new NoOpMarshaller();
        }

        if (mapper.targetType().isPrimitive()) {
            throw new IllegalArgumentException("Mappers for primitive types are not supported: " + mapper.targetType());
        }

        if (mapper instanceof OneColumnMapper) {
            return simpleMarshaller(cols, (OneColumnMapper<?>) mapper);
        } else if (mapper instanceof PojoMapper) {
            return pojoMarshaller(cols, (PojoMapper<?>) mapper, requireAllFields, allowUnmappedFields);
        } else {
            throw new IllegalArgumentException("Mapper of unsupported type: " + mapper.getClass());
        }
    }

    /**
     * Creates a marshaller for class.
     *
     * @param cols   Columns.
     * @param mapper Mapper.
     * @return Marshaller.
     */
    private static SimpleMarshaller simpleMarshaller(MarshallerColumn[] cols, OneColumnMapper<?> mapper) {
        int colIdx = findColumnIndex(cols, mapper.mappedColumn());

        return new SimpleMarshaller(createIdentityAccessor(cols[colIdx], colIdx, mapper.converter()));
    }

    private static int findColumnIndex(MarshallerColumn[] cols, @Nullable String name) {
        if (name == null) {
            if (cols.length != 1) {
                throw new IllegalArgumentException(String.format(
                        "Failed to map object to a single column: schema contains %d columns but no mapped columns were provided",
                        cols.length
                ));
            }

            return 0;
        }

        return IntStream.range(0, cols.length)
                .filter(i -> cols[i].name().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format(
                        "Failed to map object to a single column: mappedColumn '%s' is not present in the schema",
                        name
                )));
    }

    /**
     * Creates a pojo marshaller for class.
     *
     * @param cols                Columns.
     * @param mapper              Mapper.
     * @param requireAllFields    If specified class should contain fields for all columns.
     * @param allowUnmappedFields Whether specified class can contain fields that are not mapped to columns.
     * @return Pojo marshaller.
     */
    private static PojoMarshaller pojoMarshaller(
            MarshallerColumn[] cols,
            PojoMapper<?> mapper,
            boolean requireAllFields,
            boolean allowUnmappedFields) {
        FieldAccessor[] fieldAccessors = new FieldAccessor[cols.length];
        int usedFields = 0;

        // Build handlers.
        for (int i = 0; i < cols.length; i++) {
            MarshallerColumn col = cols[i];

            String columnName = col.name();

            String fieldName = mapper.fieldForColumn(columnName);

            if (fieldName == null) {
                if (requireAllFields) {
                    throw new IllegalArgumentException(String.format("No mapped object field found for column '%s'", columnName));
                }

                fieldAccessors[i] = FieldAccessor.noopAccessor(col);
            } else {
                usedFields++;

                TypeConverter<Object, Object> converter = mapper.converterForColumn(columnName);

                fieldAccessors[i] = FieldAccessor.create(mapper.targetType(), fieldName, col, i, converter);
            }
        }

        if (!allowUnmappedFields) {
            Collection<String> fields = mapper.fields();

            if (fields.size() > usedFields) {
                Set<String> fieldSet = new TreeSet<>(fields);
                for (MarshallerColumn col : cols) {
                    String fieldName = mapper.fieldForColumn(col.name());
                    fieldSet.remove(fieldName);
                }

                throw new IllegalArgumentException(
                        String.format("Fields %s of type %s are not mapped to columns", fieldSet, mapper.targetType().getName()),
                        new UnmappedColumnsException()
                );
            }
        }

        return new PojoMarshaller(new ObjectFactory<>(mapper.targetType()), fieldAccessors);
    }

    /**
     * Reads object field value.
     *
     * @param obj    Object to read from.
     * @param fldIdx Field index.
     * @return Field value.
     */
    public abstract @Nullable Object value(Object obj, int fldIdx);

    /**
     * Reads object from a row.
     *
     * @param reader Row reader.
     * @param target Optional target object. When not specified, a new object will be created.
     * @return Object.
     * @throws MarshallerException If failed.
     */
    public abstract Object readObject(MarshallerReader reader, @Nullable Object target) throws MarshallerException;

    /**
     * Write an object to a row.
     *
     * @param obj    Object.
     * @param writer Row writer.
     * @throws MarshallerException If failed.
     */
    public abstract void writeObject(@Nullable Object obj, MarshallerWriter writer) throws MarshallerException;

    /**
     * Marshaller for objects of natively supported types.
     */
    private static class SimpleMarshaller extends Marshaller {
        /** Identity accessor. */
        private final IdentityAccessor fieldAccessor;

        /**
         * Creates a marshaller for objects of natively supported type.
         *
         * @param fieldAccessor Identity field accessor for objects of natively supported type.
         */
        SimpleMarshaller(IdentityAccessor fieldAccessor) {
            this.fieldAccessor = fieldAccessor;
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable
        Object value(Object obj, int fldIdx) {
            assert fldIdx == 0;

            return fieldAccessor.value(obj);
        }

        /** {@inheritDoc} */
        @Override
        public Object readObject(MarshallerReader reader, Object target) {
            return fieldAccessor.read(reader);
        }

        /** {@inheritDoc} */
        @Override
        public void writeObject(@Nullable Object obj, MarshallerWriter writer) throws MarshallerException {
            fieldAccessor.write(writer, obj);
        }
    }

    /**
     * Marshaller for POJOs.
     */
    private static class PojoMarshaller extends Marshaller {
        /** Field accessors for mapped columns. Array has same size and order as columns. */
        private final FieldAccessor[] fieldAccessors;

        /** Object factory. */
        private final Factory<?> factory;

        /**
         * Creates a marshaller for POJOs.
         *
         * @param factory        Object factory.
         * @param fieldAccessors Object field accessors for mapped columns.
         */
        PojoMarshaller(Factory<?> factory, FieldAccessor[] fieldAccessors) {
            this.fieldAccessors = fieldAccessors;
            this.factory = Objects.requireNonNull(factory);
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable Object value(Object obj, int fldIdx) {
            return fieldAccessors[fldIdx].value(obj);
        }

        /** {@inheritDoc} */
        @Override
        public Object readObject(MarshallerReader reader, Object target) throws MarshallerException {
            Object obj = target == null ? factory.create() : target;

            for (int fldIdx = 0; fldIdx < fieldAccessors.length; fldIdx++) {
                fieldAccessors[fldIdx].read(reader, obj);
            }

            return obj;
        }

        /** {@inheritDoc} */
        @Override
        public void writeObject(@Nullable Object obj, MarshallerWriter writer) throws MarshallerException {
            for (int fldIdx = 0; fldIdx < fieldAccessors.length; fldIdx++) {
                fieldAccessors[fldIdx].write(writer, obj);
            }
        }
    }

    private static class NoOpMarshaller extends Marshaller {
        @Override
        public @Nullable Object value(Object obj, int fldIdx) {
            return null;
        }

        @Override
        public Object readObject(MarshallerReader reader, @Nullable Object target) {
            return null;
        }

        @Override
        public void writeObject(Object obj, MarshallerWriter writer) {
        }
    }

    private static class MarshallerCache {

        private final Cache<MarshallerCacheKey, Marshaller> cache = Caffeine.newBuilder()
                .maximumSize(1024)
                .build();

        Marshaller put(MarshallerCacheKey key, Function<MarshallerCacheKey, Marshaller> func) {
            return cache.get(key, func);
        }
    }

    private static final class MarshallerCacheKey {
        private final int schemaVersion;

        private final Mapper<?> mapper;

        private final MarshallerColumn[] columns;

        private final boolean requireAllFields;

        private final boolean allowUnmappedFields;

        MarshallerCacheKey(int schemaVersion, Mapper<?> mapper, boolean requireAllFields, boolean allowUnmappedFields) {
            this.schemaVersion = schemaVersion;
            this.columns = null;
            this.mapper = mapper;
            this.requireAllFields = requireAllFields;
            this.allowUnmappedFields = allowUnmappedFields;
        }

        MarshallerCacheKey(MarshallerColumn[] columns, Mapper<?> mapper, boolean requireAllFields, boolean allowUnmappedFields) {
            this.schemaVersion = -1;
            this.columns = columns;
            this.mapper = mapper;
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
                    columns, that.columns);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(schemaVersion, mapper, requireAllFields, allowUnmappedFields);
            result = 31 * result + Arrays.hashCode(columns);
            return result;
        }
    }
}
