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
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.marshalling.UnmarshallingException;
import org.apache.ignite.marshalling.UnsupportedObjectTypeMarshallingException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/** Tuple with schema marshalling. */
public final class TupleWithSchemaMarshalling {
    private static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    public static final int TYPE_ID_TUPLE = -1;

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
    public static byte [] marshal(Tuple tuple) {
        // Allocate all the memory we need upfront.
        int size = tuple.columnCount();
        Object[] values = new Object[size];
        String[] columns = new String[size];
        int[] colTypeIds = new int[size];

        // Fill in the values, column names, and types.
        for (int i = 0; i < size; i++) {
            var value = tuple.value(i);
            values[i] = value;
            columns[i] = tuple.columnName(i);
            colTypeIds[i] = getColumnTypeId(value);
        }

        ByteBuffer schemaBuff = schemaBuilder(columns, colTypeIds).build();
        ByteBuffer valueBuff = valueBuilder(columns, values).build();

        int schemaBuffLen = schemaBuff.remaining();
        int valueBuffLen = valueBuff.remaining();

        // Size: int32 (tuple size), int32 (value offset), schema, value.
        byte[] result = new byte[4 + 4 + schemaBuffLen + valueBuffLen];
        ByteBuffer buff = ByteBuffer.wrap(result).order(BYTE_ORDER);

        // Put the size of the schema in the first 4 bytes.
        buff.putInt(size);

        // Put the value offset in the second 4 bytes.
        int offset = schemaBuffLen + 8;

        buff.putInt(offset);

        buff.put(schemaBuff);
        buff.put(valueBuff);

        return result;
    }

    /**
     * Unmarshal tuple (LITTLE_ENDIAN).
     *
     * @param raw bytes that are marshaled by {@link #marshal(Tuple)}.
     */
    public static Tuple unmarshal(byte[] raw) {
        return unmarshal(ByteBuffer.wrap(raw));
    }

    /**
     * Unmarshal tuple (LITTLE_ENDIAN).
     *
     * @param raw bytes that are marshaled by {@link #marshal(Tuple)}.
     */
    public static Tuple unmarshal(ByteBuffer raw) {
        int dataLen = raw.remaining();
        if (dataLen < 8) {
            throw new UnmarshallingException("Data length can not be less than 8");
        }

        // Read first int32.
        ByteBuffer buff = raw.order(BYTE_ORDER);
        int size = buff.getInt(0);
        if (size < 0) {
            throw new UnmarshallingException("Size of the tuple can not be less than zero");
        }

        // Read second int32.
        int valueOffset = buff.getInt(4);
        if (valueOffset < 0) {
            throw new UnmarshallingException("valueOffset can not be less than zero");
        }
        if (valueOffset > dataLen) {
            throw new UnmarshallingException(
                    "valueOffset can not be greater than data length, valueOffset: "
                            + valueOffset + ", length: " + dataLen
            );
        }

        ByteBuffer schemaBuff = buff
                .position(8).limit(valueOffset)
                .slice().order(BYTE_ORDER);
        ByteBuffer valueBuff = buff
                .position(valueOffset).limit(dataLen)
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

    private static BinaryTupleBuilder schemaBuilder(String[] columns, int[] colTypeIds) {
        BinaryTupleBuilder builder = new BinaryTupleBuilder(columns.length * 2);

        for (int i = 0; i < columns.length; i++) {
            builder.appendString(columns[i]);
            builder.appendInt(colTypeIds[i]);
        }

        return builder;
    }

    private static BinaryTupleBuilder valueBuilder(String[] columnNames, Object[] values) {
        BinaryTupleBuilder builder = new BinaryTupleBuilder(values.length);

        for (int i = 0; i < values.length; i++) {
            append(columnNames[i], builder, values[i]);
        }

        return builder;
    }

    private static void append(String name, BinaryTupleBuilder builder, Object value) {
        if (value instanceof Tuple) {
            builder.appendBytes(marshal((Tuple) value));
            return;
        }

        ColumnType type = inferType(value);
        try {
            switch (type) {
                case NULL:
                    builder.appendNull();
                    return;
                case BOOLEAN:
                    builder.appendBoolean((Boolean) value);
                    return;
                case INT8:
                    builder.appendByte((Byte) value);
                    return;
                case INT16:
                    builder.appendShort((Short) value);
                    return;
                case INT32:
                    builder.appendInt((Integer) value);
                    return;
                case INT64:
                    builder.appendLong((Long) value);
                    return;
                case FLOAT:
                    builder.appendFloat((Float) value);
                    return;
                case DOUBLE:
                    builder.appendDouble((Double) value);
                    return;
                case STRING:
                    builder.appendString((String) value);
                    return;
                case DECIMAL:
                    BigDecimal d = (BigDecimal) value;
                    builder.appendDecimal(d, d.scale());
                    return;
                case DATE:
                    builder.appendDate((LocalDate) value);
                    return;
                case TIME:
                    builder.appendTime((LocalTime) value);
                    return;
                case DATETIME:
                    builder.appendDateTime((LocalDateTime) value);
                    return;
                case TIMESTAMP:
                    builder.appendTimestamp((Instant) value);
                    return;
                case UUID:
                    builder.appendUuid((UUID) value);
                    return;
                case BYTE_ARRAY:
                    builder.appendBytes((byte[]) value);
                    return;
                case PERIOD:
                    builder.appendPeriod((Period) value);
                    return;
                case DURATION:
                    builder.appendDuration((Duration) value);
                    return;
                default:
                    throw new IllegalArgumentException("Unsupported type: " + type);
            }
        } catch (ClassCastException e) {
            throw new UnsupportedObjectTypeMarshallingException(
                    "Column's type mismatch [column=" + name + ", expectedType=" + type.javaClass() + "]", e
            );
        }
    }

    private static int getColumnTypeId(@Nullable Object value) {
        if (value instanceof Tuple) {
            return TYPE_ID_TUPLE;
        } else {
            return inferType(value).id();
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
        if (colTypeId == TYPE_ID_TUPLE) {
            byte[] nestedTupleBytes = reader.bytesValue(i);
            Tuple nestedTuple = unmarshal(nestedTupleBytes);
            tuple.set(colName, nestedTuple);
            return;
        }

        ColumnType colType = ColumnType.getById(colTypeId);
        switch (colType) {
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
                throw new IllegalArgumentException("Unsupported type: " + colType);
        }
    }
}
