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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

public class TupleMarshalling {

    //    binarytuplewithschema := |schema, payload|
//
//    schema := |inlineschema or schemaid|
//    schemaid := |long|
//    inlineschema := |col, [col]|
//    col := |colname, coltypeid|
//    colname := |string|
//    coltypeid := |int|
//
//    payload := |byte[]|
    public static byte @Nullable [] marshal(Tuple tup) {
        int i = 0;
        int size = tup.columnCount();
        ArrayList<Object> values = new ArrayList<>(size);
        ArrayList<String> columns = new ArrayList<>(size);
        ColumnType[] types = new ColumnType[size];

        for (var value : tup) {
            values.add(value);
            columns.add(tup.columnName(i));
            types[i] = inferType(value);
            i++;
        }

        BinaryTupleBuilder builder = new BinaryTupleBuilder(values.size() * 3, values.size() * 3);

        for (int j = 0; j < columns.size(); j++) {
            builder.appendString(columns.get(j));
            builder.appendInt(types[j].id());
        }

        for (int j = 0; j < values.size(); j++) {
            switch (types[j]) {
                case NULL:
                    builder.appendNull();
                    break;
                case BOOLEAN:
                    builder.appendBoolean((Boolean) values.get(j));
                    break;
                case INT8:
                    builder.appendByte((Byte) values.get(j));
                    break;
                case INT16:
                    builder.appendShort((Short) values.get(j));
                    break;
                case INT32:
                    builder.appendInt((Integer) values.get(j));
                    break;
                case INT64:
                    builder.appendLong((Long) values.get(j));
                    break;
                case FLOAT:
                    builder.appendFloat((Float) values.get(j));
                    break;
                case DOUBLE:
                    builder.appendDouble((Double) values.get(j));
                    break;
                case STRING:
                    builder.appendString((String) values.get(j));
                    break;
                case DECIMAL:
                    BigDecimal d = (BigDecimal) values.get(j);
                    builder.appendDecimal(d, d.scale());
                    break;
                case DATE:
                    builder.appendDate((LocalDate) values.get(j));
                    break;
                case TIME:
                    builder.appendTime((LocalTime) values.get(j));
                    break;
                case DATETIME:
                    builder.appendDateTime((LocalDateTime) values.get(j));
                    break;
                case TIMESTAMP:
                    builder.appendTimestamp((Instant) values.get(j));
                    break;
                case UUID:
                    builder.appendUuid((UUID) values.get(j));
                    break;
                case BYTE_ARRAY:
                    builder.appendBytes((byte[]) values.get(j));
                    break;
                case PERIOD:
                    builder.appendPeriod((Period) values.get(j));
                    break;
                case DURATION:
                    builder.appendDuration((Duration) values.get(j));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported type: " + types[j]);
            }
        }

        byte[] arr = builder.build().array();
        byte[] result = new byte[arr.length + 1];
        result[0] = (byte) size;
        System.arraycopy(arr, 0, result, 1, arr.length);

        return result;
    }

    private static ColumnType inferType(Object value) {
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

    public static @Nullable Tuple unmarshal(byte @Nullable [] raw) {
        byte size = raw[0];

        ArrayList<String> columns = new ArrayList<>(size);
        ColumnType[] types = new ColumnType[size];

        byte[] arr = new byte[raw.length - 1];
        System.arraycopy(raw, 1, arr, 0, arr.length);
        BinaryTupleReader reader = new BinaryTupleReader(size * 3, arr);
        Tuple tup = Tuple.create();

        int i = 0;
        int j = 0;
        while (i < size) {
            String colName = reader.stringValue(j++);
            int colTypeId = reader.intValue(j++);
            columns.add(colName);
            types[i++] = ColumnType.getById(colTypeId);
        }

        int offset = size * 2;
        int k = 0;
        while (k < size) {
            setColumnValue(reader, tup, columns.get(k), types[k].id(), k + offset);
            k += 1;
        }

        return tup;
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
