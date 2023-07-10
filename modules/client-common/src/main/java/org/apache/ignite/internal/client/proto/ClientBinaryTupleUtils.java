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

package org.apache.ignite.internal.client.proto;

import static org.apache.ignite.lang.ErrorGroups.Client.PROTOCOL_ERR;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.ColumnType;

/**
 * Client binary tuple utils.
 */
public class ClientBinaryTupleUtils {

    /**
     * Reads an object from binary tuple at the specified index.
     *
     * @param reader Binary tuple reader.
     * @param index  Starting index in the binary tuple.
     * @return Object.
     */
    public static Object readObject(BinaryTupleReader reader, int index) {
        if (reader.hasNullValue(index)) {
            return null;
        }

        int typeCode = reader.intValue(index);
        ColumnType type = ColumnTypeConverter.fromOrdinalOrThrow(typeCode);
        int valIdx = index + 2;

        switch (type) {
            case INT8:
                return reader.byteValue(valIdx);

            case INT16:
                return reader.shortValue(valIdx);

            case INT32:
                return reader.intValue(valIdx);

            case INT64:
                return reader.longValue(valIdx);

            case FLOAT:
                return reader.floatValue(valIdx);

            case DOUBLE:
                return reader.doubleValue(valIdx);

            case DECIMAL:
                return reader.decimalValue(valIdx, reader.intValue(index + 1));

            case UUID:
                return reader.uuidValue(valIdx);

            case STRING:
                return reader.stringValue(valIdx);

            case BYTE_ARRAY:
                return reader.bytesValue(valIdx);

            case BITMASK:
                return reader.bitmaskValue(valIdx);

            case DATE:
                return reader.dateValue(valIdx);

            case TIME:
                return reader.timeValue(valIdx);

            case DATETIME:
                return reader.dateTimeValue(valIdx);

            case TIMESTAMP:
                return reader.timestampValue(valIdx);

            case NUMBER:
                return reader.numberValue(valIdx);

            case BOOLEAN:
                return reader.byteValue(valIdx) != 0;

            case DURATION:
                return reader.durationValue(valIdx);

            case PERIOD:
                return reader.periodValue(valIdx);

            default:
                throw unsupportedTypeException(typeCode);
        }
    }

    /**
     * Writes an object with type info to the binary tuple.
     *
     * @param builder Builder.
     * @param obj Object.
     */
    public static void appendObject(BinaryTupleBuilder builder, Object obj) {
        if (obj == null) {
            builder.appendNull(); // Type.
            builder.appendNull(); // Scale.
            builder.appendNull(); // Value.
        } else if (obj instanceof Boolean) {
            appendTypeAndScale(builder, ColumnType.BOOLEAN);
            builder.appendBoolean((Boolean) obj);
        } else if (obj instanceof Byte) {
            appendTypeAndScale(builder, ColumnType.INT8);
            builder.appendByte((Byte) obj);
        } else if (obj instanceof Short) {
            appendTypeAndScale(builder, ColumnType.INT16);
            builder.appendShort((Short) obj);
        } else if (obj instanceof Integer) {
            appendTypeAndScale(builder, ColumnType.INT32);
            builder.appendInt((Integer) obj);
        } else if (obj instanceof Long) {
            appendTypeAndScale(builder, ColumnType.INT64);
            builder.appendLong((Long) obj);
        } else if (obj instanceof Float) {
            appendTypeAndScale(builder, ColumnType.FLOAT);
            builder.appendFloat((Float) obj);
        } else if (obj instanceof Double) {
            appendTypeAndScale(builder, ColumnType.DOUBLE);
            builder.appendDouble((Double) obj);
        } else if (obj instanceof BigDecimal) {
            BigDecimal bigDecimal = (BigDecimal) obj;
            appendTypeAndScale(builder, ColumnType.DECIMAL, bigDecimal.scale());
            builder.appendDecimal(bigDecimal, bigDecimal.scale());
        } else if (obj instanceof java.util.UUID) {
            appendTypeAndScale(builder, ColumnType.UUID);
            builder.appendUuid((UUID) obj);
        } else if (obj instanceof String) {
            appendTypeAndScale(builder, ColumnType.STRING);
            builder.appendString((String) obj);
        } else if (obj instanceof byte[]) {
            appendTypeAndScale(builder, ColumnType.BYTE_ARRAY);
            builder.appendBytes((byte[]) obj);
        } else if (obj instanceof BitSet) {
            appendTypeAndScale(builder, ColumnType.BITMASK);
            builder.appendBitmask((BitSet) obj);
        } else if (obj instanceof LocalDate) {
            appendTypeAndScale(builder, ColumnType.DATE);
            builder.appendDate((LocalDate) obj);
        } else if (obj instanceof LocalTime) {
            appendTypeAndScale(builder, ColumnType.TIME);
            builder.appendTime((LocalTime) obj);
        } else if (obj instanceof LocalDateTime) {
            appendTypeAndScale(builder, ColumnType.DATETIME);
            builder.appendDateTime((LocalDateTime) obj);
        } else if (obj instanceof Instant) {
            appendTypeAndScale(builder, ColumnType.TIMESTAMP);
            builder.appendTimestamp((Instant) obj);
        } else if (obj instanceof BigInteger) {
            appendTypeAndScale(builder, ColumnType.NUMBER);
            builder.appendNumber((BigInteger) obj);
        } else if (obj instanceof Duration) {
            appendTypeAndScale(builder, ColumnType.DURATION);
            builder.appendDuration((Duration) obj);
        } else if (obj instanceof Period) {
            appendTypeAndScale(builder, ColumnType.PERIOD);
            builder.appendPeriod((Period) obj);
        } else {
            throw unsupportedTypeException(obj.getClass());
        }
    }

    private static void appendTypeAndScale(BinaryTupleBuilder builder, ColumnType type, int scale) {
        builder.appendInt(type.ordinal());
        builder.appendInt(scale);
    }

    private static void appendTypeAndScale(BinaryTupleBuilder builder, ColumnType type) {
        builder.appendInt(type.ordinal());
        builder.appendInt(0);
    }

    private static IgniteException unsupportedTypeException(int dataType) {
        return new IgniteException(PROTOCOL_ERR, "Unsupported type: " + dataType);
    }

    private static IgniteException unsupportedTypeException(Class<?> cls) {
        return new IgniteException(PROTOCOL_ERR, "Unsupported type: " + cls);
    }
}
