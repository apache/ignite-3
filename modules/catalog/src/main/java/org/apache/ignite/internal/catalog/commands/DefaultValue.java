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

package org.apache.ignite.internal.catalog.commands;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.BitSet;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Definition of value provider to use as default.
 */
@SuppressWarnings("PublicInnerClass")
public abstract class DefaultValue {

    /**
     * Defines value provider as functional provider.
     *
     * @param name Name of the function to invoke to generate the value
     * @return Default value definition.
     */
    public static DefaultValue functionCall(String name) {
        return new FunctionCall(Objects.requireNonNull(name, "name"));
    }

    /**
     * Defines value provider as a constant value provider.
     *
     * @param value A value to use as default.
     * @return Default value definition.
     */
    public static DefaultValue constant(@Nullable Object value) {
        return new ConstantValue(value);
    }

    /** Types of the defaults. */
    public enum Type {
        /** Default is specified as a constant. */
        CONSTANT(0),

        /** Default is specified as a call to a function. */
        FUNCTION_CALL(1);

        /** Represents absent of default value ({@code null}). */
        private static final int NO_DEFAULT = -1;

        /** type id used by serialization. */
        private final int typeId;

        Type(int typeId) {
            this.typeId = typeId;
        }
    }

    protected final Type type;

    private DefaultValue(Type type) {
        this.type = type;
    }

    /** Returns type of the default value. */
    public Type type() {
        return type;
    }

    /** Serializes this default value or {@code null}. */
    public abstract void writeTo(IgniteDataOutput os) throws IOException;

    /** Reads default value or {@code null}. */
    public static @Nullable DefaultValue readFrom(IgniteDataInput in) throws IOException {
        int typeId = in.readByte();
        if (typeId == Type.NO_DEFAULT) {
            return null;
        } else if (typeId == Type.CONSTANT.typeId) {
            Object val = readValue(in);
            return new ConstantValue(val);
        } else if (typeId == Type.FUNCTION_CALL.typeId) {
            String functionName = in.readUTF();
            return new FunctionCall(functionName);
        } else {
            throw new IllegalArgumentException("Unexpected type: " + typeId);
        }
    }

    /** Defines default value provider as a function call. */
    public static class FunctionCall extends DefaultValue {

        private final String functionName;

        private FunctionCall(String functionName) {
            super(Type.FUNCTION_CALL);
            this.functionName = functionName;
        }

        /** Returns name of the function to use as value generator. */
        public String functionName() {
            return functionName;
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FunctionCall that = (FunctionCall) o;

            return Objects.equals(functionName, that.functionName);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Objects.hash(type, functionName);
        }

        /** {@inheritDoc} */
        @Override
        public void writeTo(IgniteDataOutput os) throws IOException {
            os.writeByte(type.typeId);
            os.writeUTF(functionName);
        }
    }

    /** Defines default value provider as a constant. */
    public static class ConstantValue extends DefaultValue {
        private final ColumnType columnType;

        private final @Nullable Object value;

        private ConstantValue(@Nullable Object value) {
            super(Type.CONSTANT);

            NativeType nativeType = NativeTypes.fromObject(value);

            if (nativeType == null) {
                columnType = ColumnType.NULL;
            } else {
                columnType = nativeType.spec().asColumnType();
            }
            this.value = value;
        }

        /** Returns value to use as default. */
        public @Nullable Object value() {
            return value;
        }

        /** {@inheritDoc} */
        @Override
        public void writeTo(IgniteDataOutput out) throws IOException {
            out.writeByte(type.typeId);
            writeValue(columnType, value, out);
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ConstantValue that = (ConstantValue) o;

            return Objects.equals(value, that.value);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Objects.hash(type, value);
        }
    }

    /**
     * Writes the given default value into output.
     * @param val Default value or null.
     * @param out Output.
     * @throws IOException if thrown.
     */
    public static void writeTo(@Nullable DefaultValue val, IgniteDataOutput out) throws IOException {
        if (val == null) {
            out.writeByte(Type.NO_DEFAULT);
        } else {
            val.writeTo(out);
        }
    }

    private static void writeValue(ColumnType columnType, @Nullable Object value, IgniteDataOutput out) throws IOException {
        out.writeByte(columnType.id());
        if (value == null) {
            return;
        }

        switch (columnType) {
            case NULL:
                break;
            case BOOLEAN:
                out.writeBoolean((Boolean) value);
                break;
            case INT8:
                out.writeByte((Byte) value);
                break;
            case INT16:
                out.writeShort((Short) value);
                break;
            case INT32:
                out.writeInt((Integer) value);
                break;
            case INT64:
                out.writeLong((Long) value);
                break;
            case FLOAT:
                out.writeFloat((Float) value);
                break;
            case DOUBLE:
                out.writeDouble((Double) value);
                break;
            case DECIMAL:
                out.writeBigDecimal((BigDecimal) value);
                break;
            case DATE:
                out.writeLocalDate((LocalDate) value);
                break;
            case TIME:
                out.writeLocalTime((LocalTime) value);
                break;
            case DATETIME:
                out.writeLocalDateTime((LocalDateTime) value);
                break;
            case TIMESTAMP:
                out.writeInstant((Instant) value);
                break;
            case UUID:
                out.writeUuid((UUID) value);
                break;
            case BITMASK:
                out.writeBitSet((BitSet) value);
                break;
            case STRING:
                out.writeUTF((String) value);
                break;
            case BYTE_ARRAY:
                byte[] bytes = (byte[]) value;
                out.writeInt(bytes.length);
                out.writeByteArray((byte[]) value);
                break;
            case PERIOD:
                out.writePeriod((Period) value);
                break;
            case DURATION:
                out.writeDuration((Duration) value);
                break;
            case NUMBER:
                out.writeBigInteger((BigInteger) value);
                break;
            default:
                throw new IllegalArgumentException("Unexpected column type: " + columnType);
        }
    }

    private static @Nullable Object readValue(IgniteDataInput in) throws IOException {
        int typeId = in.readByte();
        ColumnType columnType = ColumnType.getById(typeId);
        switch (columnType) {
            case NULL:
                return null;
            case BOOLEAN:
                return in.readBoolean();
            case INT8:
                return in.readByte();
            case INT16:
                return in.readShort();
            case INT32:
                return in.readInt();
            case INT64:
                return in.readLong();
            case FLOAT:
                return in.readFloat();
            case DOUBLE:
                return in.readDouble();
            case DECIMAL:
                return in.readBigDecimal();
            case DATE:
                return in.readLocalDate();
            case TIME:
                return in.readLocalTime();
            case DATETIME:
                return in.readLocalDateTime();
            case TIMESTAMP:
                return in.readInstant();
            case UUID:
                return in.readUuid();
            case BITMASK:
                return in.readBitSet();
            case STRING:
                return in.readUTF();
            case BYTE_ARRAY:
                int bytesLength = in.readInt();
                return in.readByteArray(bytesLength);
            case PERIOD:
                return in.readPeriod();
            case DURATION:
                return in.readDuration();
            case NUMBER:
                return in.readBigInteger();
            default:
                throw new IllegalArgumentException("Unexpected column type: " + columnType);
        }
    }
}
