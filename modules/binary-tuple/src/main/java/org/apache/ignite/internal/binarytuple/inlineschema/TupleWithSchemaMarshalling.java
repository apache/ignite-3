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
import java.util.function.BiConsumer;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/** Tuple with schema marshalling. */
public final class TupleWithSchemaMarshalling {
    private static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    /**
     * Marshal tuple is the following format (LITTLE_ENDIAN):
     * marshalledTuple       := | size | valuePosition | binaryTupleWithSchema |
     * size                  := int32
     * valuePosition := int32
     * binaryTupleWithSchema := | schemaBinaryTuple | valueBinaryTuple |
     * schemaBinaryTuple := | column1 | ... | columnN |
     * column                := | columnName | columnType |
     * columnName            := string
     * columnType            := int32
     * valueBinaryTuple := | value1 | ... | valueN |.
     */
    public static byte @Nullable [] marshal(Tuple tup) {
        // Allocate all the memory we need upfront.
        int size = tup.columnCount();
        Object[] values = new Object[size];
        String[] columns = new String[size];
        ColumnType[] types = new ColumnType[size];

        // Fill in the values, column names, and types.
        for (int i = 0; i < size; i++) {
            var value = tup.value(i);
            values[i] = value;
            columns[i] = tup.columnName(i);
            types[i] = inferType(value);
        }

        BinaryTupleBuilder schemaBuilder = schemaBuilder(columns, types);
        BinaryTupleBuilder valueBuilder = valueBuilder(values, types);

        byte[] schemaBt = schemaBuilder.build().array();
        byte[] valueBt = valueBuilder.build().array();
        // Size: int32 (tuple size), int32 (value offset), schema, value.
        byte[] result = new byte[4 + 4 + schemaBt.length + valueBt.length];
        ByteBuffer buff = ByteBuffer.wrap(result).order(BYTE_ORDER);

        // Put the size of the schema in the first 4 bytes.
        buff.putInt(0, size);

        // Put the value offset in the second 4 bytes.
        int offset = schemaBt.length + 8;
        buff.putInt(4, offset);

        System.arraycopy(schemaBt, 0, result, 8, schemaBt.length);
        System.arraycopy(valueBt, 0, result, schemaBt.length + 8, valueBt.length);

        return result;
    }

    /**
     * Unmarshal tuple (LITTLE_ENDIAN).
     *
     * @param raw byte[] bytes that are marshaled by {@link #marshal(Tuple)}.
     */
    public static @Nullable Tuple unmarshal(byte @Nullable [] raw) {
        // Read first int32.
        ByteBuffer buff = ByteBuffer.wrap(raw).order(BYTE_ORDER);
        int size = buff.getInt(0);

        // Read second int32.
        int valueOffset = buff.getInt(4);

        String[] columns = new String[size];
        ColumnType[] types = new ColumnType[size];

        ByteBuffer schemaBuff = buff
                .position(8).limit(valueOffset)
                .slice().order(BYTE_ORDER);
        ByteBuffer valueBuff = buff
                .position(valueOffset).limit(raw.length)
                .slice().order(BYTE_ORDER);

        BinaryTupleReader schemaReader = new BinaryTupleReader(size * 2, schemaBuff);
        BinaryTupleReader valueReader = new BinaryTupleReader(size, valueBuff);

        Tuple tup = Tuple.create(size);

        int readerInd = 0;
        int i = 0;
        while (i < size) {
            String colName = schemaReader.stringValue(readerInd++);
            int colTypeId = schemaReader.intValue(readerInd++);

            columns[i] = colName;
            types[i] = ColumnType.getById(colTypeId);

            i += 1;
        }

        int k = 0;
        while (k < size) {
            setColumnValue(valueReader, tup, columns[k], types[k].id(), k);
            k += 1;
        }

        return tup;
    }

    private static BinaryTupleBuilder schemaBuilder(String[] columns, ColumnType[] types) {
        BinaryTupleBuilder builder = new BinaryTupleBuilder(columns.length * 2, columns.length * 2);

        for (int i = 0; i < columns.length; i++) {
            builder.appendString(columns[i]);
            builder.appendInt(types[i].id());
        }

        return builder;
    }

    private static BinaryTupleBuilder valueBuilder(Object[] values, ColumnType[] types) {
        BinaryTupleBuilder builder = new BinaryTupleBuilder(values.length, values.length);

        for (int i = 0; i < values.length; i++) {
            ColumnType type = types[i];
            Object v = values[i];

            appender(type).accept(builder, v);
        }

        return builder;
    }

    private static BiConsumer<BinaryTupleBuilder, Object> appender(ColumnType type) {
        switch (type) {
            case NULL:
                return (b, v) -> b.appendNull();
            case BOOLEAN:
                return (b, v) -> b.appendBoolean(checkType(Boolean.class, v));
            case INT8:
                return (b, v) -> b.appendByte(checkType(Byte.class, v));
            case INT16:
                return (b, v) -> b.appendShort(checkType(Short.class, v));
            case INT32:
                return (b, v) -> b.appendInt(checkType(Integer.class, v));
            case INT64:
                return (b, v) -> b.appendLong(checkType(Long.class, v));
            case FLOAT:
                return (b, v) -> b.appendFloat(checkType(Float.class, v));
            case DOUBLE:
                return (b, v) -> b.appendDouble(checkType(Double.class, v));
            case STRING:
                return (b, v) -> b.appendString(checkType(String.class, v));
            case DECIMAL:
                return (b, v) -> b.appendDecimal(checkType(BigDecimal.class, v), ((BigDecimal) v).scale());
            case DATE:
                return (b, v) -> b.appendDate(checkType(LocalDate.class, v));
            case TIME:
                return (b, v) -> b.appendTime(checkType(LocalTime.class, v));
            case DATETIME:
                return (b, v) -> b.appendDateTime(checkType(LocalDateTime.class, v));
            case TIMESTAMP:
                return (b, v) -> b.appendTimestamp(checkType(Instant.class, v));
            case UUID:
                return (b, v) -> b.appendUuid(checkType(UUID.class, v));
            case BYTE_ARRAY:
                return (b, v) -> b.appendBytes(checkType(byte[].class, v));
            case PERIOD:
                return (b, v) -> b.appendPeriod(checkType(Period.class, v));
            case DURATION:
                return (b, v) -> b.appendDuration(checkType(Duration.class, v));
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    private static <T> T checkType(Class<T> cl, Object o) {
        if (o.getClass().isAssignableFrom(cl)) {
            return (T) o;
        }

        throw new IllegalArgumentException("Unsupported type: " + o.getClass());
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
        throw new IllegalArgumentException("Unsupported type: " + value.getClass());
    }


    private static void setColumnValue(BinaryTupleReader reader, Tuple tup, String colName, int colTypeId, int i) {
        switch (ColumnType.getById(colTypeId)) {
            case NULL:
                tup.set(colName, null);
                break;
            case BOOLEAN:
                tup.set(colName, reader.booleanValue(i));
                break;
            case INT8:
                tup.set(colName, reader.byteValue(i));
                break;
            case INT16:
                tup.set(colName, reader.shortValue(i));
                break;
            case INT32:
                tup.set(colName, reader.intValue(i));
                break;
            case INT64:
                tup.set(colName, reader.longValue(i));
                break;
            case FLOAT:
                tup.set(colName, reader.floatValue(i));
                break;
            case DOUBLE:
                tup.set(colName, reader.doubleValue(i));
                break;
            case STRING:
                tup.set(colName, reader.stringValue(i));
                break;
            case DECIMAL:
                BigDecimal decimal = reader.decimalValue(i, Integer.MIN_VALUE);
                tup.set(colName, decimal);
                break;
            case DATE:
                tup.set(colName, reader.dateValue(i));
                break;
            case TIME:
                tup.set(colName, reader.timeValue(i));
                break;
            case DATETIME:
                tup.set(colName, reader.dateTimeValue(i));
                break;
            case TIMESTAMP:
                tup.set(colName, reader.timestampValue(i));
                break;
            case UUID:
                tup.set(colName, reader.uuidValue(i));
                break;
            case BYTE_ARRAY:
                tup.set(colName, reader.bytesValue(i));
                break;
            case PERIOD:
                tup.set(colName, reader.periodValue(i));
                break;
            case DURATION:
                tup.set(colName, reader.durationValue(i));
                break;
            default:
                throw new IllegalArgumentException("Unsupported type: " + colTypeId);
        }
    }
}
