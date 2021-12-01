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
     * @param cols             Columns.
     * @param mapper           Mapper.
     * @param requireAllFields If specified class should contain fields for all columns.
     * @return Marshaller.
     */
    public static <T> Marshaller createMarshaller(Column[] cols, @NotNull Mapper<T> mapper, boolean requireAllFields) {
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

        ColumnBinding[] columnBindings = new ColumnBinding[cols.length];
        // Build handlers.

        for (int i = 0; i < cols.length; i++) {
            final Column col = cols[i];

            String fieldName = mapper.fieldForColumn(col.name());

            if (requireAllFields && fieldName == null) {
                throw new IllegalArgumentException("No field found for column " + col.name());
            }

            columnBindings[i] = (fieldName == null) ? ColumnBinding.unmappedFieldBinding(col) :
                    ColumnBinding.createFieldBinding(col, mapper.targetType(), fieldName);
        }

        return new ObjectMarshaller(new ObjectFactory<>(mapper.targetType()), columnBindings);
    }

    /**
     * Creates a marshaller for class.
     *
     * @param cols             Columns.
     * @param cls              Type.
     * @param requireAllFields If specified class should contain fields for all columns.
     * @return Marshaller.
     */
    //TODO: IGNITE-15907 drop
    @Deprecated
    public static Marshaller createMarshaller(Columns cols, Class<?> cls, boolean requireAllFields) {
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

            if (requireAllFields) {
                try {
                    cls.getDeclaredField(col.name());
                } catch (NoSuchFieldException e) {
                    throw new IllegalArgumentException("No field found for column " + col.name());
                }
            }

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
    public abstract @Nullable
    Object value(Object obj, int fldIdx) throws MarshallerException;

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
        /** Individual column binding. */
        private final ColumnBinding columnBinding;

        /**
         * Creates a marshaller for objects of natively supported type.
         *
         * @param columnBinding Identity field binding for objects of natively supported type.
         */
        SimpleMarshaller(ColumnBinding columnBinding) {
            this.columnBinding = columnBinding;
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable
        Object value(Object obj, int fldIdx) throws MarshallerException {
            assert fldIdx == 0;

            return columnBinding.value(obj);
        }

        /** {@inheritDoc} */
        @Override
        public Object readObject(Row reader) throws MarshallerException {
            try {
                return columnBinding.columnValue(reader);
            } catch (Throwable e) {
                throw new MarshallerException("Failed to read column: colIdx" + columnBinding.colIdx, e);
            }
        }

        /** {@inheritDoc} */
        @Override
        public void writeObject(Object obj, RowAssembler writer) throws MarshallerException {
            try {
                columnBinding.write(writer, obj);
            } catch (Throwable e) {
                throw new MarshallerException("Failed to write column: colIdx" + columnBinding.colIdx, e);
            }
        }
    }

    /**
     * Marshaller for POJOs/ The case when an object fields map to the columns.
     */
    static class ObjectMarshaller extends Marshaller {
        /** Column bindings. Array has same size and order as columns. */
        private final ColumnBinding[] columnBindings;
        /** Object factory. */
        private final Factory<?> factory;

        /**
         * Creates a marshaller for POJOs.
         *
         * @param factory        Object factory.
         * @param columnBindings Column bindings.
         */
        @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
        ObjectMarshaller(Factory<?> factory, ColumnBinding[] columnBindings) {
            this.columnBindings = columnBindings;
            this.factory = Objects.requireNonNull(factory);
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable
        Object value(Object obj, int fldIdx) throws MarshallerException {
            return columnBindings[fldIdx].value(obj);
        }

        /** {@inheritDoc} */
        @Override
        public Object readObject(Row reader) throws MarshallerException {
            try {
                final Object obj = factory.create();

                for (int fldIdx = 0; fldIdx < columnBindings.length; fldIdx++) {
                    columnBindings[fldIdx].read(reader, obj);
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
                for (int fldIdx = 0; fldIdx < columnBindings.length; fldIdx++) {
                    columnBindings[fldIdx].write(writer, obj);
                }
            } catch (MarshallerException e) {
                throw e;
            } catch (Throwable e) {
                throw new MarshallerException("Failed to write row. ", e);
            }
        }
    }
}
