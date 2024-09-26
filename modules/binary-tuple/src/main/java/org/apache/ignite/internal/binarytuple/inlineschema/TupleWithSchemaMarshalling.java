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


import static org.apache.ignite.lang.ErrorGroups.Marshalling.UNSUPPORTED_OBJECT_TYPE_ERR;

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
        boolean[] nestedTupleFlags = new boolean[size];

        // Fill in the values, column names, and types.
        for (int i = 0; i < size; i++) {
            var value = tuple.value(i);
            values[i] = value;
            columns[i] = tuple.columnName(i);
            types[i] = inferType(value);
            nestedTupleFlags[i] = value instanceof Tuple;
        }

        ByteBuffer schemaBuff = schemaBuilder(columns, types, nestedTupleFlags).build();
        ByteBuffer valueBuff = valueBuilder(columns, types, values, nestedTupleFlags).build();

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

        BinaryTupleReader schemaReader = new BinaryTupleReader(size * 3, schemaBuff);
        BinaryTupleReader valueReader = new BinaryTupleReader(size, valueBuff);

        Tuple tup = Tuple.create(size);

        for (int i = 0; i < size; i++) {
            String colName = schemaReader.stringValue(i * 3);
            int colTypeId = schemaReader.intValue(i * 3 + 1);
            boolean nestedTupleFlag = schemaReader.booleanValue(i * 3 + 2);

            setColumnValue(valueReader, tup, colName, ColumnType.getById(colTypeId), i, nestedTupleFlag);
        }

        return tup;
    }

    private static BinaryTupleBuilder schemaBuilder(String[] columns, ColumnType[] types, boolean[] nestedTupleFlags) {
        BinaryTupleBuilder builder = new BinaryTupleBuilder(columns.length * 3);

        for (int i = 0; i < columns.length; i++) {
            builder.appendString(columns[i]);
            builder.appendInt(types[i].id());
            builder.appendBoolean(nestedTupleFlags[i]);
        }

        return builder;
    }

    private static BinaryTupleBuilder valueBuilder(String[] columnNames, ColumnType[] types, Object[] values, boolean[] nestedTupleFlags) {
        BinaryTupleBuilder builder = new BinaryTupleBuilder(values.length);

        for (int i = 0; i < values.length; i++) {
            ColumnType type = types[i];
            Object v = values[i];
            boolean nestedTupleFlag = nestedTupleFlags[i];

            append(type, columnNames[i], builder, v, nestedTupleFlag);
        }

        return builder;
    }

    private static void append(ColumnType type, String name, BinaryTupleBuilder builder, Object value, boolean nestedTupleFlag) {
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
                    byte[] b = nestedTupleFlag ? marshal((Tuple) value) : (byte[]) value;
                    builder.appendBytes(b);
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
            throw new IgniteException(UNSUPPORTED_OBJECT_TYPE_ERR, "Column's type mismatch ["
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
        if (value instanceof Tuple) {
            return ColumnType.BYTE_ARRAY;
        }
        throw new UnsupportedObjectTypeMarshallingException("Tuple field is of unsupported type: " + value.getClass());
    }


    private static void setColumnValue(BinaryTupleReader reader, Tuple tuple, String colName, ColumnType colType, int i,
            boolean nestedTupleFlag) {
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
                if (nestedTupleFlag) {
                    byte[] nestedTupleBytes = reader.bytesValue(i);
                    Tuple nestedTuple = unmarshal(nestedTupleBytes);
                    tuple.set(colName, nestedTuple);
                    break;
                }
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
