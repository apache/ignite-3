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

package org.apache.ignite.internal.schema.marshaller.reflection;

import java.util.Arrays;
import java.util.Objects;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.SchemaMismatchException;
import org.apache.ignite.internal.schema.marshaller.BinaryMode;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.marshaller.MarshallerUtil;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.util.Factory;
import org.apache.ignite.internal.util.ObjectFactory;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Marshaller.
 */
public abstract class Marshaller {
    /**
     * Creates a marshaller for class.
     *
     * @param cols   Columns.
     * @param mapper Mapper.
     * @return Marshaller.
     */
    public static <T> Marshaller createMarshaller(Column[] cols, @NotNull Mapper<T> mapper) {
        final BinaryMode mode = MarshallerUtil.mode(mapper.targetType());

        if (mode != BinaryMode.POJO || mapper.mappedColumn() != null) {
            Column col = (mapper.mappedColumn() == null && cols.length == 1) ? cols[0] :
                    Arrays.stream(cols).filter(c -> c.name().equals(mapper.mappedColumn())).findFirst().orElseThrow(() ->
                            new SchemaMismatchException("Failed to map object to a single column: mappedColumn=" + mapper.mappedColumn()));

            if (mode.typeSpec() != col.type().spec()) {
                throw new SchemaMismatchException(
                        String.format("Object can't be mapped to a column of incompatible type: columnType=%s, mappedType=%s",
                                col.type().spec(), mapper.targetType().getName()));
            }

            assert !mapper.targetType().isPrimitive() : "Non-nullable types are not allowed.";

            return new SimpleMarshaller(ColumnBinding.createIdentityBinding(col, mapper.targetType()));
        }

        ColumnBinding[] fieldAccessors = new ColumnBinding[cols.length];

        // Build handlers.
        for (int i = 0; i < cols.length; i++) {
            final Column col = cols[i];

            String fieldName = mapper.mappedField(col.name());

            // TODO: IGNITE-15785 validate key marshaller has no NoopAccessors.
            fieldAccessors[i] = (fieldName == null) ? ColumnBinding.noopAccessor(col) :
                    ColumnBinding.createFieldBinding(col, mapper.targetType(), fieldName);
        }

        return new ObjectMarshaller(new ObjectFactory<>(mapper.targetType()), fieldAccessors);
    }

    /**
     * Creates a marshaller for class.
     *
     * @param cols Columns.
     * @param cls  Type.
     * @return Marshaller.
     */
    //TODO: IGNITE-15907 drop
    @Deprecated
    public static Marshaller createMarshaller(Columns cols, Class<? extends Object> cls) {
        final BinaryMode mode = MarshallerUtil.mode(cls);

        if (mode != BinaryMode.POJO) {
            final Column col = cols.column(0);

            assert cols.length() == 1;
            assert mode.typeSpec() == col.type().spec() : "Target type is not compatible.";
            assert !cls.isPrimitive() : "Non-nullable types are not allowed.";

            return new SimpleMarshaller(ColumnBinding.createIdentityBinding(col, cls));
        }

        ColumnBinding[] fieldAccessors = new ColumnBinding[cols.length()];

        // Build accessors
        for (int i = 0; i < cols.length(); i++) {
            final Column col = cols.column(i);

            fieldAccessors[i] = ColumnBinding.createFieldBinding(col, cls, col.name());
        }

        return new ObjectMarshaller(new ObjectFactory<>(cls), fieldAccessors);
    }

    /**
     * Reads object field value.
     *
     * @param obj    Object to read from.
     * @param fldIdx Field index.
     * @return Field value.
     * @throws MarshallerException If failed to read object field.
     */
    public abstract @Nullable Object value(Object obj, int fldIdx) throws MarshallerException;

    /**
     * Reads object from a row.
     *
     * @param reader Row reader.
     * @return Object.
     * @throws MarshallerException If failed to unmarshall given row to an object.
     */
    public abstract Object readObject(Row reader) throws MarshallerException;

    /**
     * Write an object to a row.
     *
     * @param obj    Object.
     * @param writer Row writer.
     * @throws MarshallerException If failed to marshall given object to a row.
     */
    public abstract void writeObject(Object obj, RowAssembler writer) throws MarshallerException;

    /**
     * Marshaller for key/value objects of natively supported types. The case when a whole object maps to a single column.
     */
    static class SimpleMarshaller extends Marshaller {
        /** Identity accessor. */
        private final ColumnBinding fieldAccessor;

        /**
         * Creates a marshaller for objects of natively supported type.
         *
         * @param FieldAccessor Identity field accessor for objects of natively supported type.
         */
        SimpleMarshaller(ColumnBinding FieldAccessor) {
            this.fieldAccessor = FieldAccessor;
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable Object value(Object obj, int fldIdx) throws MarshallerException {
            assert fldIdx == 0;

            return fieldAccessor.value(obj);
        }

        /** {@inheritDoc} */
        @Override
        public Object readObject(Row reader) throws MarshallerException {
            try {
                return fieldAccessor.columnValue(reader);
            } catch (Throwable e) {
                throw new MarshallerException("Failed to read column: colIdx" + fieldAccessor.colIdx, e);
            }
        }


        /** {@inheritDoc} */
        @Override
        public void writeObject(Object obj, RowAssembler writer) throws MarshallerException {
            try {
                fieldAccessor.write(writer, obj);
            } catch (Throwable e) {
                throw new MarshallerException("Failed to write column: colIdx" + fieldAccessor.colIdx, e);
            }
        }
    }

    /**
     * Marshaller for POJOs/ The case when an object fields map to the columns.
     */
    static class ObjectMarshaller extends Marshaller {
        /** Field accessors for mapped columns. Array has same size and order as columns. */
        private final ColumnBinding[] fieldAccessors;

        /** Object factory. */
        private final Factory<?> factory;

        /**
         * Creates a marshaller for POJOs.
         *
         * @param factory        Object factory.
         * @param fieldAccessors Object field accessors for mapped columns.
         */
        @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
        ObjectMarshaller(Factory<?> factory, ColumnBinding[] fieldAccessors) {
            this.fieldAccessors = fieldAccessors;
            this.factory = Objects.requireNonNull(factory);
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable Object value(Object obj, int fldIdx) throws MarshallerException {
            return fieldAccessors[fldIdx].value(obj);
        }

        /** {@inheritDoc} */
        @Override
        public Object readObject(Row reader) throws MarshallerException {
            try {
                final Object obj = factory.create();

                for (int fldIdx = 0; fldIdx < fieldAccessors.length; fldIdx++) {
                    fieldAccessors[fldIdx].read(reader, obj);
                }

                return obj;
            } catch (MarshallerException e) {
                throw e;
            } catch (Throwable e) {
                throw new MarshallerException("Failed to read row. ", e);
            }
        }

        /** {@inheritDoc} */
        @Override
        public void writeObject(Object obj, RowAssembler writer) throws MarshallerException {
            try {
                for (int fldIdx = 0; fldIdx < fieldAccessors.length; fldIdx++) {
                    fieldAccessors[fldIdx].write(writer, obj);
                }
            } catch (MarshallerException e) {
                throw e;
            } catch (Throwable e) {
                throw new MarshallerException("Failed to write row. ", e);
            }
        }
    }
}
