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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Client binary tuple utils.
 */
public class ClientBinaryTupleUtils {
    /**
     * Reads an object from binary tuple at the specified index.
     *
     * @param reader Binary tuple reader.
     * @param index Starting index in the binary tuple.
     * @return Object.
     */
    static @Nullable Object readObject(BinaryTupleReader reader, int index) {
        if (reader.hasNullValue(index)) {
            return null;
        }

        int typeId = reader.intValue(index);
        ColumnType type = ColumnTypeConverter.fromIdOrThrow(typeId);
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
                throw unsupportedTypeException(typeId);
        }
    }

    static Function<Integer, Object> readerForType(BinaryTupleReader binTuple, ColumnType type) {
        switch (type) {
            case INT8:
                return binTuple::byteValue;

            case INT16:
                return binTuple::shortValue;

            case INT32:
                return binTuple::intValue;

            case INT64:
                return binTuple::longValue;

            case FLOAT:
                return binTuple::floatValue;

            case DOUBLE:
                return binTuple::doubleValue;

            case DECIMAL:
                return idx -> binTuple.decimalValue(idx, -1);

            case UUID:
                return binTuple::uuidValue;

            case STRING:
                return binTuple::stringValue;

            case BYTE_ARRAY:
                return binTuple::bytesValue;

            case BITMASK:
                return binTuple::bitmaskValue;

            case DATE:
                return binTuple::dateValue;

            case TIME:
                return binTuple::timeValue;

            case DATETIME:
                return binTuple::dateTimeValue;

            case TIMESTAMP:
                return binTuple::timestampValue;

            case NUMBER:
                return binTuple::numberValue;

            case BOOLEAN:
                return idx -> binTuple.byteValue(idx) != 0;

            case DURATION:
                return binTuple::durationValue;

            case PERIOD:
                return binTuple::periodValue;

            default:
                throw unsupportedTypeException(type.id());
        }
    }

    /**
     * Writes an object with type info to the binary tuple.
     *
     * @param builder Builder.
     * @param obj Object.
     */
    public static <T> void appendObject(BinaryTupleBuilder builder, @Nullable T obj, @Nullable Marshaller<T, byte[]> marshaller) {
        if (obj == null) {
            builder.appendNull(); // Type.
            builder.appendNull(); // Scale.
            builder.appendNull(); // Value.
        } else if (marshaller != null) {
            appendTypeAndScale(builder, ColumnType.BYTE_ARRAY);
            builder.appendBytes(marshaller.marshal(obj));
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
        } else if (obj instanceof UUID) {
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

    /**
     * Packs an array of objects in BinaryTuple format.
     *
     * @param builder Target builder.
     * @param items Items.
     */
    static <T> void appendCollectionToBinaryTuple(BinaryTupleBuilder builder, Collection<T> items) {
        assert items != null : "items can't be null";
        assert !items.isEmpty() : "items can't be empty";
        assert builder != null : "builder can't be null";

        T firstItem = items.iterator().next();
        Objects.requireNonNull(firstItem);
        Class<?> type = firstItem.getClass();

        Consumer<T> appender = appendTypeAndGetAppender(builder, firstItem);
        builder.appendInt(items.size());

        for (T item : items) {
            Objects.requireNonNull(item);
            if (!type.equals(item.getClass())) {
                throw new IllegalArgumentException(
                        "All items must have the same type. First item: " + type + ", current item: " + item.getClass());
            }

            appender.accept(item);
        }
    }

    static <R> List<R> readCollectionFromBinaryTuple(BinaryTupleReader reader, int readerIndex) {
        int typeId = reader.intValue(readerIndex++);
        ColumnType type = ColumnTypeConverter.fromIdOrThrow(typeId);
        Function<Integer, Object> itemReader = readerForType(reader, type);
        int itemsCount = reader.intValue(readerIndex++);

        List<R> items = new ArrayList<>(itemsCount);
        for (int i = 0; i < itemsCount; i++) {
            items.add((R) itemReader.apply(readerIndex++));
        }

        return items;
    }

    /**
     * Writes type id to the specified packer and returns a consumer that writes the value to the binary tuple.
     *
     * @param builder Builder.
     * @param obj Object.
     */
    private static <T> Consumer<T> appendTypeAndGetAppender(BinaryTupleBuilder builder, Object obj) {
        assert obj != null : "Object is null";

        if (obj instanceof Boolean) {
            builder.appendInt(ColumnType.BOOLEAN.id());
            return (T v) -> builder.appendBoolean((Boolean) v);
        } else if (obj instanceof Byte) {
            builder.appendInt(ColumnType.INT8.id());
            return (T v) -> builder.appendByte((Byte) v);
        } else if (obj instanceof Short) {
            builder.appendInt(ColumnType.INT16.id());
            return (T v) -> builder.appendShort((Short) v);
        } else if (obj instanceof Integer) {
            builder.appendInt(ColumnType.INT32.id());
            return (T v) -> builder.appendInt((Integer) v);
        } else if (obj instanceof Long) {
            builder.appendInt(ColumnType.INT64.id());
            return (T v) -> builder.appendLong((Long) v);
        } else if (obj instanceof Float) {
            builder.appendInt(ColumnType.FLOAT.id());
            return (T v) -> builder.appendFloat((Float) v);
        } else if (obj instanceof Double) {
            builder.appendInt(ColumnType.DOUBLE.id());
            return (T v) -> builder.appendDouble((Double) v);
        } else if (obj instanceof BigDecimal) {
            builder.appendInt(ColumnType.DECIMAL.id());
            return (T v) -> builder.appendDecimal((BigDecimal) v, ((BigDecimal) v).scale());
        } else if (obj instanceof UUID) {
            builder.appendInt(ColumnType.UUID.id());
            return (T v) -> builder.appendUuid((UUID) v);
        } else if (obj instanceof String) {
            builder.appendInt(ColumnType.STRING.id());
            return (T v) -> builder.appendString((String) v);
        } else if (obj instanceof byte[]) {
            builder.appendInt(ColumnType.BYTE_ARRAY.id());
            return (T v) -> builder.appendBytes((byte[]) v);
        } else if (obj instanceof BitSet) {
            builder.appendInt(ColumnType.BITMASK.id());
            return (T v) -> builder.appendBitmask((BitSet) v);
        } else if (obj instanceof LocalDate) {
            builder.appendInt(ColumnType.DATE.id());
            return (T v) -> builder.appendDate((LocalDate) v);
        } else if (obj instanceof LocalTime) {
            builder.appendInt(ColumnType.TIME.id());
            return (T v) -> builder.appendTime((LocalTime) v);
        } else if (obj instanceof LocalDateTime) {
            builder.appendInt(ColumnType.DATETIME.id());
            return (T v) -> builder.appendDateTime((LocalDateTime) v);
        } else if (obj instanceof Instant) {
            builder.appendInt(ColumnType.TIMESTAMP.id());
            return (T v) -> builder.appendTimestamp((Instant) v);
        } else if (obj instanceof BigInteger) {
            builder.appendInt(ColumnType.NUMBER.id());
            return (T v) -> builder.appendNumber((BigInteger) v);
        } else if (obj instanceof Duration) {
            builder.appendInt(ColumnType.DURATION.id());
            return (T v) -> builder.appendDuration((Duration) v);
        } else if (obj instanceof Period) {
            builder.appendInt(ColumnType.PERIOD.id());
            return (T v) -> builder.appendPeriod((Period) v);
        } else {
            throw unsupportedTypeException(obj.getClass());
        }
    }

    /**
     * Writes a column value to the binary tuple.
     *
     * @param builder Builder.
     * @param type Column type.
     * @param name Column name.
     * @param scale Scale.
     * @param v Value.
     */
    public static void appendValue(BinaryTupleBuilder builder, ColumnType type, String name, int scale, @Nullable Object v) {
        if (v == null) {
            builder.appendNull();
            return;
        }

        try {
            switch (type) {
                case BOOLEAN:
                    builder.appendBoolean((boolean) v);
                    return;

                case INT8:
                    builder.appendByte((byte) v);
                    return;

                case INT16:
                    builder.appendShort((short) v);
                    return;

                case INT32:
                    builder.appendInt((int) v);
                    return;

                case INT64:
                    builder.appendLong((long) v);
                    return;

                case FLOAT:
                    builder.appendFloat((float) v);
                    return;

                case DOUBLE:
                    builder.appendDouble((double) v);
                    return;

                case DECIMAL:
                    builder.appendDecimalNotNull((BigDecimal) v, scale);
                    return;

                case UUID:
                    builder.appendUuidNotNull((UUID) v);
                    return;

                case STRING:
                    builder.appendStringNotNull((String) v);
                    return;

                case BYTE_ARRAY:
                    builder.appendBytesNotNull((byte[]) v);
                    return;

                case BITMASK:
                    builder.appendBitmaskNotNull((BitSet) v);
                    return;

                case DATE:
                    builder.appendDateNotNull((LocalDate) v);
                    return;

                case TIME:
                    builder.appendTimeNotNull((LocalTime) v);
                    return;

                case DATETIME:
                    builder.appendDateTimeNotNull((LocalDateTime) v);
                    return;

                case TIMESTAMP:
                    builder.appendTimestampNotNull((Instant) v);
                    return;

                case NUMBER:
                    builder.appendNumberNotNull((BigInteger) v);
                    return;

                default:
                    throw new IllegalArgumentException("Unsupported type: " + type);
            }
        } catch (ClassCastException e) {
            // Exception message is similar to embedded mode - see o.a.i.i.schema.Column#validate
            throw new IgniteException(PROTOCOL_ERR, "Column's type mismatch ["
                    + "column=" + name
                    + ", expectedType=" + type
                    + ", actualType=" + v.getClass() + ']', e);
        }
    }

    private static void appendTypeAndScale(BinaryTupleBuilder builder, ColumnType type, int scale) {
        builder.appendInt(type.id());
        builder.appendInt(scale);
    }

    private static void appendTypeAndScale(BinaryTupleBuilder builder, ColumnType type) {
        builder.appendInt(type.id());
        builder.appendInt(0);
    }

    private static IgniteException unsupportedTypeException(int dataType) {
        return new IgniteException(PROTOCOL_ERR, "Unsupported type: " + dataType);
    }

    private static IgniteException unsupportedTypeException(Class<?> cls) {
        return new IgniteException(PROTOCOL_ERR, "Unsupported type: " + cls);
    }
}
