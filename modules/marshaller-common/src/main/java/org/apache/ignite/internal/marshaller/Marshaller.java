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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.marshaller.FieldAccessor.createIdentityAccessor;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import org.apache.ignite.internal.marshaller.FieldAccessor.IdentityAccessor;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.mapper.OneColumnMapper;
import org.apache.ignite.table.mapper.PojoMapper;
import org.apache.ignite.table.mapper.TypeConverter;
import org.jetbrains.annotations.Nullable;

/**
 * Marshaller.
 */
public abstract class Marshaller {
    /**
     * Creates a marshaller for class.
     *
     * @param cols Columns.
     * @param mapper Mapper.
     * @param requireAllFields If specified class should contain fields for all columns.
     * @param allowUnmappedFields Whether specified class can contain fields that are not mapped to columns.
     * @return Marshaller.
     */
    static Marshaller createMarshaller(
            MarshallerColumn[] cols,
            Mapper<?> mapper,
            boolean requireAllFields,
            boolean allowUnmappedFields) {
        if (mapper.targetType() == Void.class) {
            return new NoOpMarshaller();
        }

        if (mapper.targetType().isPrimitive()) {
            throw new MarshallerException(format("Mappers for primitive types are not supported: {}", mapper.targetType()));
        }

        if (mapper instanceof OneColumnMapper) {
            return simpleMarshaller(cols, (OneColumnMapper<?>) mapper);
        } else if (mapper instanceof PojoMapper) {
            return pojoMarshaller(cols, (PojoMapper<?>) mapper, requireAllFields, allowUnmappedFields);
        } else {
            throw new MarshallerException(format("Mapper of unsupported type: {}", mapper.getClass()));
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
        MarshallerColumn column = findColumnIndex(cols, mapper.mappedColumn());

        return new SimpleMarshaller(createIdentityAccessor(column, column.schemaIndex(), mapper.converter()));
    }

    private static MarshallerColumn findColumnIndex(MarshallerColumn[] cols, @Nullable String name) {
        if (name == null) {
            if (cols.length != 1) {
                throw new MarshallerException(format(
                        "Failed to map object to a single column: schema contains {} columns but no mapped columns were provided",
                        cols.length
                ));
            }

            return cols[0];
        }

        for (MarshallerColumn column : cols) {
            if (column.name().equals(name)) {
                return column;
            }
        }

        throw new MarshallerException(format(
                "Failed to map object to a single column: mappedColumn '{}' is not present in the schema",
                name
        ));
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
    private static Marshaller pojoMarshaller(
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

            Field field = mapper.declaredFieldForColumn(columnName);

            if (field == null) {
                if (requireAllFields) {
                    throw new MarshallerException(format("No mapped object field found for column '{}'", columnName));
                }

                fieldAccessors[i] = FieldAccessor.noopAccessor(col);
            } else {
                usedFields++;

                TypeConverter<Object, Object> converter = mapper.converterForColumn(columnName);
                fieldAccessors[i] = FieldAccessor.create(mapper.targetType(), field, col, i, converter);
            }

            fieldAccessors[i]
                    .withColumnName(columnName);
        }

        if (!allowUnmappedFields) {
            Collection<String> fields = mapper.fields();

            if (fields.size() > usedFields) {
                Set<String> fieldSet = new TreeSet<>(fields);
                for (MarshallerColumn col : cols) {
                    String fieldName = mapper.fieldForColumn(col.name());
                    if (fieldName == null) {
                        assert !requireAllFields;
                        continue;
                    }
                    fieldSet.remove(fieldName);
                }

                throw new MarshallerException(
                        format("Fields {} of type {} are not mapped to columns", fieldSet, mapper.targetType().getName()),
                        new UnmappedColumnsException()
                );
            }
        }

        return new PojoMarshaller(mapper.targetType(), fieldAccessors);
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
     * @return Object.
     * @throws MarshallerException If failed.
     */
    public abstract Object readObject(MarshallerReader reader, Object target) throws MarshallerException;

    /**
     * Write an object to a row.
     *
     * @param obj    Object.
     * @param writer Row writer.
     * @throws MarshallerException If failed.
     */
    public abstract void writeObject(@Nullable Object obj, MarshallerWriter writer) throws MarshallerException;

    /**
     * Write the specified field of an object to a row.
     *
     * @param obj Object.
     * @param writer Row writer.
     * @param fldIdx Field index.
     * @throws MarshallerException If failed.
     */
    public abstract void writeField(@Nullable Object obj, MarshallerWriter writer, int fldIdx) throws MarshallerException;

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

        /** {@inheritDoc} */
        @Override
        public void writeField(Object obj, MarshallerWriter writer, int fldIdx) throws MarshallerException {
            assert fldIdx == 0;

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
        private final Creator creator;

        /**
         * Creates a marshaller for POJOs.
         *
         * @param targetType Object target type.
         * @param fieldAccessors Object field accessors for mapped columns.
         */
        PojoMarshaller(Class<?> targetType, FieldAccessor[] fieldAccessors) {
            this.fieldAccessors = fieldAccessors;
            this.creator = Creator.of(targetType);
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable Object value(Object obj, int fldIdx) {
            return fieldAccessors[fldIdx].value(obj);
        }

        /** {@inheritDoc} */
        @Override
        public Object readObject(MarshallerReader reader, Object target) throws MarshallerException {
            // target is always null, but this exact API is used by migration tools
            return creator.createInstance(fieldAccessors, reader);
        }

        /** {@inheritDoc} */
        @Override
        public void writeObject(@Nullable Object obj, MarshallerWriter writer) throws MarshallerException {
            for (int fldIdx = 0; fldIdx < fieldAccessors.length; fldIdx++) {
                fieldAccessors[fldIdx].write(writer, obj);
            }
        }

        /** {@inheritDoc} */
        @Override
        public void writeField(@Nullable Object obj, MarshallerWriter writer, int fldIdx) throws MarshallerException {
            fieldAccessors[fldIdx].write(writer, obj);
        }
    }

    private static class NoOpMarshaller extends Marshaller {
        @Override
        public @Nullable Object value(Object obj, int fldIdx) {
            return null;
        }

        @Override
        public Object readObject(MarshallerReader reader, Object target) {
            return null;
        }

        @Override
        public void writeObject(Object obj, MarshallerWriter writer) {
        }

        @Override
        public void writeField(Object obj, MarshallerWriter writer, int fldIdx) throws MarshallerException {

        }
    }
}
