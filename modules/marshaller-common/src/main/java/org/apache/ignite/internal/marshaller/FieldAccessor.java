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

import static org.apache.ignite.internal.marshaller.ValidationUtils.validateColumnType;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.table.mapper.TypeConverter;
import org.jetbrains.annotations.Nullable;

/**
 * Field accessor to speedup access.
 */
abstract class FieldAccessor {
    /** Mode. */
    private final BinaryMode mode;

    /**
     * Mapped column position in the schema.
     */
    private final int colIdx;

    /** Scale. */
    private final int scale;

    static FieldAccessor noopAccessor(MarshallerColumn col) {
        return new UnmappedFieldAccessor(col);
    }

    /**
     * Create accessor for the field.
     *
     * @param type Object class.
     * @param fldName Object field name.
     * @param col A column the field is mapped to.
     * @param colIdx Column index in the schema.
     * @return Accessor.
     */
    static FieldAccessor create(
            Class<?> type,
            String fldName,
            MarshallerColumn col,
            int colIdx,
            @Nullable TypeConverter<?, ?> typeConverter
    ) {
        try {
            Field field = type.getDeclaredField(fldName);

            if (typeConverter == null) {
                validateColumnType(col, field.getType());
            }

            BinaryMode fieldAccessMode = BinaryMode.forClass(field.getType());
            MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(type, MethodHandles.lookup());

            VarHandle varHandle = lookup.unreflectVarHandle(field);

            assert fieldAccessMode != null : "Invalid fieldAccessMode for type: " + field.getType();

            switch (fieldAccessMode) {
                case P_BOOLEAN:
                    return new BooleanPrimitiveAccessor(varHandle, colIdx);

                case P_BYTE:
                    return new BytePrimitiveAccessor(varHandle, colIdx);

                case P_SHORT:
                    return new ShortPrimitiveAccessor(varHandle, colIdx);

                case P_INT:
                    return new IntPrimitiveAccessor(varHandle, colIdx);

                case P_LONG:
                    return new LongPrimitiveAccessor(varHandle, colIdx);

                case P_FLOAT:
                    return new FloatPrimitiveAccessor(varHandle, colIdx);

                case P_DOUBLE:
                    return new DoublePrimitiveAccessor(varHandle, colIdx);

                case BOOLEAN:
                case BYTE:
                case SHORT:
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case STRING:
                case UUID:
                case BYTE_ARR:
                case DECIMAL:
                case TIME:
                case DATE:
                case DATETIME:
                case TIMESTAMP:
                case POJO:
                    return new ReferenceFieldAccessor(varHandle, colIdx, col.type(), col.scale(), typeConverter);

                default:
                    assert false : "Invalid field access mode " + fieldAccessMode;
            }

            throw new IllegalArgumentException("Failed to create accessor for field [name=" + field.getName() + ']');
        } catch (NoSuchFieldException | SecurityException | IllegalAccessException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    /**
     * Create accessor for the field.
     *
     * @param col Column.
     * @param colIdx Column index.
     * @return Accessor.
     */
    static IdentityAccessor createIdentityAccessor(MarshallerColumn col, int colIdx, @Nullable TypeConverter<?, ?> converter) {
        switch (col.type()) {
            //  Marshaller read/write object contract methods allowed boxed types only.
            case P_BOOLEAN:
            case P_BYTE:
            case P_SHORT:
            case P_INT:
            case P_LONG:
            case P_FLOAT:
            case P_DOUBLE:
                throw new IllegalArgumentException("Primitive key/value types are not possible by API contract.");

            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case UUID:
            case BYTE_ARR:
            case DECIMAL:
            case TIME:
            case DATE:
            case DATETIME:
            case TIMESTAMP:
            case POJO:
                return new IdentityAccessor(colIdx, col.type(), col.scale(), converter);

            default:
                assert false : "Invalid mode " + col.type();
        }

        throw new IllegalArgumentException("Failed to create accessor for column [name=" + col.name() + ']');
    }

    /**
     * Constructor.
     *
     * @param colIdx Column index.
     * @param mode Read/write mode.
     * @param scale Scale.
     */
    FieldAccessor(int colIdx, BinaryMode mode, int scale) {
        assert colIdx >= 0;

        this.colIdx = colIdx;
        this.mode = mode;
        this.scale = scale;
    }

    /**
     * Constructor.
     *
     * @param colIdx Column index.
     * @param mode Read/write mode.
     */
    FieldAccessor(int colIdx, BinaryMode mode) {
        this(colIdx, mode, 0);
    }

    /**
     * Reads value object from row.
     *
     * @param reader Reader.
     * @return Read value object.
     */
    Object readRefValue(MarshallerReader reader) {
        assert reader != null;
        assert colIdx >= 0;

        switch (mode) {
            case BOOLEAN:
                return reader.readBooleanBoxed();

            case BYTE:
                return reader.readByteBoxed();

            case SHORT:
                return reader.readShortBoxed();

            case INT:
                return reader.readIntBoxed();

            case LONG:
                return reader.readLongBoxed();

            case FLOAT:
                return reader.readFloatBoxed();

            case DOUBLE:
                return reader.readDoubleBoxed();

            case STRING:
                return reader.readString();

            case UUID:
                return reader.readUuid();

            case BYTE_ARR:
                return reader.readBytes();

            case DECIMAL:
                return reader.readBigDecimal(scale);

            case DATE:
                return reader.readDate();

            case TIME:
                return reader.readTime();

            case TIMESTAMP:
                return reader.readTimestamp();

            case DATETIME:
                return reader.readDateTime();

            case POJO:
                return reader.readBytes();

            default:
                throw new IllegalArgumentException("Invalid mode: " + mode);
        }
    }

    /**
     * Writes reference value to row.
     *
     * @param val Value object.
     * @param writer Writer.
     */
    void writeRefObject(Object val, MarshallerWriter writer) {
        assert writer != null;

        if (val == null) {
            writer.writeNull();

            return;
        }

        switch (mode) {
            case BOOLEAN:
                writer.writeBoolean((Boolean) val);

                break;

            case BYTE:
                writer.writeByte((Byte) val);

                break;

            case SHORT:
                writer.writeShort((Short) val);

                break;

            case INT:
                writer.writeInt((Integer) val);

                break;

            case LONG:
                writer.writeLong((Long) val);

                break;

            case FLOAT:
                writer.writeFloat((Float) val);

                break;

            case DOUBLE:
                writer.writeDouble((Double) val);

                break;

            case STRING:
                writer.writeString((String) val);

                break;

            case UUID:
                writer.writeUuid((UUID) val);

                break;

            case BYTE_ARR:
                writer.writeBytes((byte[]) val);

                break;

            case DECIMAL:
                writer.writeBigDecimal((BigDecimal) val, scale);

                break;

            case DATE:
                writer.writeDate((LocalDate) val);

                break;

            case TIME:
                writer.writeTime((LocalTime) val);

                break;

            case TIMESTAMP:
                writer.writeTimestamp((Instant) val);

                break;

            case DATETIME:
                writer.writeDateTime((LocalDateTime) val);

                break;

            case POJO:
                writer.writeBytes((byte[]) val);

                break;

            default:
                throw new IllegalArgumentException("Invalid mode: " + mode);
        }
    }

    /**
     * Write object field value to row.
     *
     * @param writer Row writer.
     * @param obj Source object.
     * @throws MarshallerException If failed.
     */
    final void write(MarshallerWriter writer, @Nullable Object obj) throws MarshallerException {
        try {
            write0(writer, obj);
        } catch (Exception ex) {
            throw new MarshallerException(ex.getMessage(), ex);
        }
    }

    /**
     * Write object field value to row.
     *
     * @param writer Row writer.
     * @param obj Source object.
     * @throws Exception If write failed.
     */
    abstract void write0(MarshallerWriter writer, @Nullable Object obj) throws Exception;

    /**
     * Reads value fom row to object field.
     *
     * @param reader MarshallerReader reader.
     * @param obj Target object.
     * @throws MarshallerException If failed.
     */
    final void read(MarshallerReader reader, Object obj) throws MarshallerException {
        try {
            read0(reader, obj);
        } catch (Exception ex) {
            throw new MarshallerException(ex.getMessage(), ex);
        }
    }

    /**
     * Reads value fom row to object field.
     *
     * @param reader MarshallerReader reader.
     * @param obj Target object.
     * @throws Exception If failed.
     */
    abstract void read0(MarshallerReader reader, Object obj) throws Exception;

    /**
     * Reads object field value.
     *
     * @param obj Object.
     * @return Field value of given object.
     */
    abstract Object value(Object obj);

    /**
     * Stubbed accessor for unused columns writes default column value, and ignore value on read access.
     */
    private static class UnmappedFieldAccessor extends FieldAccessor {
        /** Column. */
        private final MarshallerColumn col;

        /**
         * Constructor.
         *
         * @param col Column.
         */
        UnmappedFieldAccessor(MarshallerColumn col) {
            super(0, null);
            this.col = col;
        }

        @Override
        void read0(MarshallerReader reader, Object obj) {
            reader.skipValue();
        }

        @Override
        void write0(MarshallerWriter writer, Object obj) {
            writer.writeAbsentValue();
        }

        @Override
        Object value(Object obj) {
            return col.defaultValue();
        }
    }

    /**
     * Accessor for a field of primitive {@code byte} type.
     */
    public static class IdentityAccessor extends FieldAccessor {
        @Nullable
        private final TypeConverter<Object, Object> converter;

        /**
         * Constructor.
         *
         * @param colIdx Column index.
         * @param mode Read/write mode.
         */
        IdentityAccessor(int colIdx, BinaryMode mode, int scale, @Nullable TypeConverter<?, ?> converter) {
            super(colIdx, mode, scale);

            this.converter = (TypeConverter<Object, Object>) converter;
        }

        @Override
        void write0(MarshallerWriter writer, Object obj) {
            try {
                obj = converter == null ? obj : converter.toColumnType(obj);
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }

            writeRefObject(obj, writer);
        }

        @Override
        void read0(MarshallerReader reader, Object obj) {
            throw new UnsupportedOperationException("Called identity accessor for object field.");
        }

        /**
         * Read an object from a row.
         *
         * @param reader MarshallerReader reader.
         * @return Object.
         */
        Object read(MarshallerReader reader) {
            Object obj = readRefValue(reader);

            try {
                return converter == null ? obj : converter.toObjectType(obj);
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        Object value(Object obj) {
            return obj;
        }
    }

    private abstract static class VarHandleAccessor extends FieldAccessor {
        private final VarHandle varHandle;

        VarHandleAccessor(int colIdx, BinaryMode mode, VarHandle varHandle) {
            super(colIdx, mode);

            this.varHandle = varHandle;
        }

        VarHandleAccessor(int colIdx, BinaryMode mode, int scale, VarHandle varHandle) {
            super(colIdx, mode, scale);

            this.varHandle = varHandle;
        }

        <T> T get(Object obj) {
            return (T) varHandle.get(obj);
        }

        void set(Object obj, Object val) {
            varHandle.set(obj, val);
        }

        @Override
        Object value(Object obj) {
            return get(Objects.requireNonNull(obj));
        }
    }

    /**
     * Accessor for a field of primitive {@code boolean} type.
     */
    private static class BooleanPrimitiveAccessor extends VarHandleAccessor {
        /**
         * Constructor.
         *
         * @param varHandle VarHandle.
         * @param colIdx Column index.
         */
        BooleanPrimitiveAccessor(VarHandle varHandle, int colIdx) {
            super(colIdx, BinaryMode.P_BOOLEAN, varHandle);
        }

        @Override
        void write0(MarshallerWriter writer, Object obj) {
            boolean val = get(obj);

            writer.writeBoolean(val);
        }

        @Override
        void read0(MarshallerReader reader, Object obj) {
            boolean val = reader.readBoolean();

            set(obj, val);
        }
    }

    /**
     * Accessor for a field of primitive {@code byte} type.
     */
    private static class BytePrimitiveAccessor extends VarHandleAccessor {
        /**
         * Constructor.
         *
         * @param varHandle VarHandle.
         * @param colIdx Column index.
         */
        BytePrimitiveAccessor(VarHandle varHandle, int colIdx) {
            super(colIdx, BinaryMode.P_BYTE, varHandle);
        }

        @Override
        void write0(MarshallerWriter writer, Object obj) {
            byte val = get(obj);

            writer.writeByte(val);
        }

        @Override
        void read0(MarshallerReader reader, Object obj) {
            byte val = reader.readByte();

            set(obj, val);
        }
    }

    /**
     * Accessor for a field of primitive {@code short} type.
     */
    private static class ShortPrimitiveAccessor extends VarHandleAccessor {
        /**
         * Constructor.
         *
         * @param varHandle VarHandle.
         * @param colIdx Column index.
         */
        ShortPrimitiveAccessor(VarHandle varHandle, int colIdx) {
            super(colIdx, BinaryMode.P_SHORT, varHandle);
        }

        @Override
        void write0(MarshallerWriter writer, Object obj) {
            short val = get(obj);

            writer.writeShort(val);
        }

        @Override
        void read0(MarshallerReader reader, Object obj) {
            short val = reader.readShort();

            set(obj, val);
        }
    }

    /**
     * Accessor for a field of primitive {@code int} type.
     */
    private static class IntPrimitiveAccessor extends VarHandleAccessor {
        /**
         * Constructor.
         *
         * @param varHandle VarHandle.
         * @param colIdx Column index.
         */
        IntPrimitiveAccessor(VarHandle varHandle, int colIdx) {
            super(colIdx, BinaryMode.P_INT, varHandle);
        }

        @Override
        void write0(MarshallerWriter writer, Object obj) {
            int val = get(obj);

            writer.writeInt(val);
        }

        @Override
        void read0(MarshallerReader reader, Object obj) {
            int val = reader.readInt();

            set(obj, val);
        }
    }

    /**
     * Accessor for a field of primitive {@code long} type.
     */
    private static class LongPrimitiveAccessor extends VarHandleAccessor {
        /**
         * Constructor.
         *
         * @param varHandle VarHandle.
         * @param colIdx Column index.
         */
        LongPrimitiveAccessor(VarHandle varHandle, int colIdx) {
            super(colIdx, BinaryMode.P_LONG, varHandle);
        }

        @Override
        void write0(MarshallerWriter writer, Object obj) {
            long val = get(obj);

            writer.writeLong(val);
        }

        @Override
        void read0(MarshallerReader reader, Object obj) {
            long val = reader.readLong();

            set(obj, val);
        }
    }

    /**
     * Accessor for a field of primitive {@code float} type.
     */
    private static class FloatPrimitiveAccessor extends VarHandleAccessor {
        /**
         * Constructor.
         *
         * @param varHandle VarHandle.
         * @param colIdx Column index.
         */
        FloatPrimitiveAccessor(VarHandle varHandle, int colIdx) {
            super(colIdx, BinaryMode.P_FLOAT, varHandle);
        }

        @Override
        void write0(MarshallerWriter writer, Object obj) {
            float val = get(obj);

            writer.writeFloat(val);
        }

        @Override
        void read0(MarshallerReader reader, Object obj) {
            float val = reader.readFloat();

            set(obj, val);
        }
    }

    /**
     * Accessor for a field of primitive {@code double} type.
     */
    private static class DoublePrimitiveAccessor extends VarHandleAccessor {
        /**
         * Constructor.
         *
         * @param varHandle VarHandle.
         * @param colIdx Column index.
         */
        DoublePrimitiveAccessor(VarHandle varHandle, int colIdx) {
            super(colIdx, BinaryMode.P_DOUBLE, varHandle);
        }

        @Override
        void write0(MarshallerWriter writer, Object obj) {
            double val = get(obj);

            writer.writeDouble(val);
        }

        @Override
        void read0(MarshallerReader reader, Object obj) {
            double val = reader.readDouble();

            set(obj, val);
        }
    }

    /**
     * Accessor for a field of reference type.
     */
    private static class ReferenceFieldAccessor extends VarHandleAccessor {
        @Nullable
        private final TypeConverter<Object, Object> typeConverter;

        /**
         * Constructor.
         *
         * @param varHandle VarHandle.
         * @param colIdx Column index.
         * @param mode Read/write mode.
         */
        ReferenceFieldAccessor(VarHandle varHandle, int colIdx, BinaryMode mode, int scale, @Nullable TypeConverter<?, ?> typeConverter) {
            super(colIdx, mode, scale, varHandle);

            this.typeConverter = (TypeConverter<Object, Object>) typeConverter;
        }

        @Override
        void write0(MarshallerWriter writer, @Nullable Object obj) {
            assert writer != null;

            if (obj == null) {
                writer.writeNull();

                return;
            }

            Object val = get(obj);

            if (val == null) {
                writer.writeNull();

                return;
            }

            try {
                writeRefObject(typeConverter == null ? val : typeConverter.toColumnType(val), writer);
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        void read0(MarshallerReader reader, Object obj) {
            Object val = readRefValue(reader);

            try {
                set(obj, typeConverter == null ? val : typeConverter.toObjectType(val));
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        Object value(Object obj) {
            Object value = get(Objects.requireNonNull(obj));

            try {
                return typeConverter == null ? value : typeConverter.toColumnType(value);
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }
    }
}
