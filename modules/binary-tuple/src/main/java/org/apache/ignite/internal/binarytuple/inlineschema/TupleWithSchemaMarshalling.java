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

package org.apache.ignite.internal.binarytuple.inlineschema;

import static org.apache.ignite.lang.ErrorGroups.Client.PROTOCOL_ERR;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.UUID;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.marshalling.UnmarshallingException;
import org.apache.ignite.marshalling.UnsupportedObjectTypeMarshallingException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/** Tuple with schema marshalling. */
public final class TupleWithSchemaMarshalling {
    private static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    /**
     * Marshal tuple in the following format (LITTLE_ENDIAN).
     *
     * <pre>
     * marshalledTuple := | size | valuePosition | binaryTupleWithSchema |
     *
     * size            := int32
     * valuePosition   := int32
     * binaryTupleWithSchema := | schemaBinaryTuple | valueBinaryTuple |
     * schemaBinaryTuple := | column1 | ... | columnN |
     * column              := | columnName | columnType |
     * columnName          := string
     * columnType          := int32
     * valueBinaryTuple := | value1 | ... | valueN |.
     * </pre>
     */
    public static byte @Nullable [] marshal(@Nullable Tuple tuple) {
        if (tuple == null) {
            return null;
        }

        // Allocate all the memory we need upfront.
        int size = tuple.columnCount();
        Object[] values = new Object[size];
        String[] columns = new String[size];
        ColumnType[] types = new ColumnType[size];

        // Fill in the values, column names, and types.
        for (int i = 0; i < size; i++) {
            var value = tuple.value(i);
            values[i] = value;
            columns[i] = tuple.columnName(i);
            types[i] = inferType(value);
        }

        ByteBuffer schemaBf = schemaBuilder(columns, types).build();
        ByteBuffer valueBf = valueBuilder(columns, types, values).build();

        byte[] schemaArr = getByteArray(schemaBf);
        byte[] valueArr = getByteArray(valueBf);
        // Size: int32 (tuple size), int32 (value offset), schema, value.
        byte[] result = new byte[4 + 4 + schemaArr.length + valueArr.length];
        ByteBuffer buff = ByteBuffer.wrap(result).order(BYTE_ORDER);

        // Put the size of the schema in the first 4 bytes.
        buff.putInt(0, size);

        // Put the value offset in the second 4 bytes.
        int offset = schemaArr.length + 8;
        buff.putInt(4, offset);

        System.arraycopy(schemaArr, 0, result, 8, schemaArr.length);
        System.arraycopy(valueArr, 0, result, schemaArr.length + 8, valueArr.length);

        return result;
    }

    /** Get byte array from ByteBuffer without capacity overhead, only meaningful bytes are returned. */
    private static byte[] getByteArray(ByteBuffer buff) {
        int offset = buff.arrayOffset();
        int limit = buff.limit();
        byte[] result = new byte[limit - offset];

        buff.get(result, offset, limit);
        return result;
    }

    /**
     * Unmarshal tuple (LITTLE_ENDIAN).
     *
     * @param raw byte[] bytes that are marshaled by {@link #marshal(Tuple)}.
     */
    public static @Nullable Tuple unmarshal(byte @Nullable [] raw) {
        if (raw == null) {
            return null;
        }
        if (raw.length < 8) {
            throw new UnmarshallingException("byte[] length can not be less than 8");
        }

        // Read first int32.
        ByteBuffer buff = ByteBuffer.wrap(raw).order(BYTE_ORDER);
        int size = buff.getInt(0);
        if (size < 0) {
            throw new UnmarshallingException("Size of the tuple can not be less than zero");
        }

        // Read second int32.
        int valueOffset = buff.getInt(4);
        if (valueOffset < 0) {
            throw new UnmarshallingException("valueOffset can not be less than zero");
        }
        if (valueOffset > raw.length) {
            throw new UnmarshallingException(
                    "valueOffset can not be greater than byte[] length, valueOffset: "
                            + valueOffset + ", length: " + raw.length
            );
        }

        ByteBuffer schemaBuff = buff
                .position(8).limit(valueOffset)
                .slice().order(BYTE_ORDER);
        ByteBuffer valueBuff = buff
                .position(valueOffset).limit(raw.length)
                .slice().order(BYTE_ORDER);

        BinaryTupleReader schemaReader = new BinaryTupleReader(size * 2, schemaBuff);
        BinaryTupleReader valueReader = new BinaryTupleReader(size, valueBuff);

        Tuple tup = Tuple.create(size);
        
        for (int i = 0; i < size; i++) {
            String colName = schemaReader.stringValue(i * 2);
            int colTypeId = schemaReader.intValue(i * 2 + 1);

            setColumnValue(valueReader, tup, colName, colTypeId, i);
        }

        return tup;
    }

    private static BinaryTupleBuilder schemaBuilder(String[] columns, ColumnType[] types) {
        BinaryTupleBuilder builder = new BinaryTupleBuilder(columns.length * 2);

        for (int i = 0; i < columns.length; i++) {
            builder.appendString(columns[i]);
            builder.appendInt(types[i].id());
        }

        return builder;
    }

    private static BinaryTupleBuilder valueBuilder(String[] columnNames, ColumnType[] types, Object[] values) {
        BinaryTupleBuilder builder = new BinaryTupleBuilder(values.length);

        for (int i = 0; i < values.length; i++) {
            ColumnType type = types[i];
            Object v = values[i];

            appender(type, columnNames[i]).accept(builder, v);
        }

        return builder;
    }

    private static BiConsumer<BinaryTupleBuilder, Object> appender(ColumnType type, String name) {
        try {
            switch (type) {
                case NULL:
                    return (b, v) -> b.appendNull();
                case BOOLEAN:
                    return (b, v) -> b.appendBoolean((Boolean) v);
                case INT8:
                    return (b, v) -> b.appendByte((Byte) v);
                case INT16:
                    return (b, v) -> b.appendShort((Short) v);
                case INT32:
                    return (b, v) -> b.appendInt((Integer) v);
                case INT64:
                    return (b, v) -> b.appendLong((Long) v);
                case FLOAT:
                    return (b, v) -> b.appendFloat((Float) v);
                case DOUBLE:
                    return (b, v) -> b.appendDouble((Double) v);
                case STRING:
                    return (b, v) -> b.appendString((String) v);
                case DECIMAL:
                    return (b, v) -> {
                        BigDecimal d = (BigDecimal) v;
                        b.appendDecimal(d, d.scale());
                    };
                case DATE:
                    return (b, v) -> b.appendDate((LocalDate) v);
                case TIME:
                    return (b, v) -> b.appendTime((LocalTime) v);
                case DATETIME:
                    return (b, v) -> b.appendDateTime((LocalDateTime) v);
                case TIMESTAMP:
                    return (b, v) -> b.appendTimestamp((Instant) v);
                case UUID:
                    return (b, v) -> b.appendUuid((UUID) v);
                case BYTE_ARRAY:
                    return (b, v) -> b.appendBytes((byte[]) v);
                case PERIOD:
                    return (b, v) -> b.appendPeriod((Period) v);
                case DURATION:
                    return (b, v) -> b.appendDuration((Duration) v);
                default:
                    throw new IllegalArgumentException("Unsupported type: " + type);
            }
        } catch (ClassCastException e) {
            throw new IgniteException(PROTOCOL_ERR, "Column's type mismatch ["
                    + "column=" + name
                    + ", expectedType=" + type.javaClass(),
                    e
            );
        }
    }

    private static ColumnType inferType(@Nullable Object value) {
        if (value == null) {
            return ColumnType.NULL;
        }
        if (value instanceof Boolean) {
            return ColumnType.BOOLEAN;
        }
        if (value instanceof Byte) {
            return ColumnType.INT8;
        }
        if (value instanceof Short) {
            return ColumnType.INT16;
        }
        if (value instanceof Integer) {
            return ColumnType.INT32;
        }
        if (value instanceof Long) {
            return ColumnType.INT64;
        }
        if (value instanceof Float) {
            return ColumnType.FLOAT;
        }
        if (value instanceof Double) {
            return ColumnType.DOUBLE;
        }
        if (value instanceof String) {
            return ColumnType.STRING;
        }
        if (value instanceof BigDecimal) {
            return ColumnType.DECIMAL;
        }
        if (value instanceof LocalDate) {
            return ColumnType.DATE;
        }
        if (value instanceof LocalTime) {
            return ColumnType.TIME;
        }
        if (value instanceof LocalDateTime) {
            return ColumnType.DATETIME;
        }
        if (value instanceof Instant) {
            return ColumnType.TIMESTAMP;
        }
        if (value instanceof UUID) {
            return ColumnType.UUID;
        }
        if (value instanceof byte[]) {
            return ColumnType.BYTE_ARRAY;
        }
        if (value instanceof Period) {
            return ColumnType.PERIOD;
        }
        if (value instanceof Duration) {
            return ColumnType.DURATION;
        }
        throw new UnsupportedObjectTypeMarshallingException("Tuple field is of unsupported type: " + value.getClass());
    }


    private static void setColumnValue(BinaryTupleReader reader, Tuple tuple, String colName, int colTypeId, int i) {
        switch (ColumnType.getById(colTypeId)) {
            case NULL:
                tuple.set(colName, null);
                break;
            case BOOLEAN:
                tuple.set(colName, reader.booleanValue(i));
                break;
            case INT8:
                tuple.set(colName, reader.byteValue(i));
                break;
            case INT16:
                tuple.set(colName, reader.shortValue(i));
                break;
            case INT32:
                tuple.set(colName, reader.intValue(i));
                break;
            case INT64:
                tuple.set(colName, reader.longValue(i));
                break;
            case FLOAT:
                tuple.set(colName, reader.floatValue(i));
                break;
            case DOUBLE:
                tuple.set(colName, reader.doubleValue(i));
                break;
            case STRING:
                tuple.set(colName, reader.stringValue(i));
                break;
            case DECIMAL:
                BigDecimal decimal = reader.decimalValue(i, Integer.MIN_VALUE);
                tuple.set(colName, decimal);
                break;
            case DATE:
                tuple.set(colName, reader.dateValue(i));
                break;
            case TIME:
                tuple.set(colName, reader.timeValue(i));
                break;
            case DATETIME:
                tuple.set(colName, reader.dateTimeValue(i));
                break;
            case TIMESTAMP:
                tuple.set(colName, reader.timestampValue(i));
                break;
            case UUID:
                tuple.set(colName, reader.uuidValue(i));
                break;
            case BYTE_ARRAY:
                tuple.set(colName, reader.bytesValue(i));
                break;
            case PERIOD:
                tuple.set(colName, reader.periodValue(i));
                break;
            case DURATION:
                tuple.set(colName, reader.durationValue(i));
                break;
            default:
                throw new IllegalArgumentException("Unsupported type: " + colTypeId);
        }
    }
}
