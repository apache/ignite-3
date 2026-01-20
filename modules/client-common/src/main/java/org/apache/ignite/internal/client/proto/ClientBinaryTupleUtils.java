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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.Client.PROTOCOL_ERR;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.MarshallerException;
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
    public static @Nullable Object readObject(BinaryTupleReader reader, int index) {
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

            case DATE:
                return reader.dateValue(valIdx);

            case TIME:
                return reader.timeValue(valIdx);

            case DATETIME:
                return reader.dateTimeValue(valIdx);

            case TIMESTAMP:
                return reader.timestampValue(valIdx);

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

    /**
     * Writes an object with type info to the binary tuple.
     *
     * @param builder Builder.
     * @param obj Object.
     */
    public static <T> void appendObject(BinaryTupleBuilder builder, @Nullable T obj) {
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
        } else if (obj instanceof UUID) {
            appendTypeAndScale(builder, ColumnType.UUID);
            builder.appendUuid((UUID) obj);
        } else if (obj instanceof String) {
            appendTypeAndScale(builder, ColumnType.STRING);
            builder.appendString((String) obj);
        } else if (obj instanceof byte[]) {
            appendTypeAndScale(builder, ColumnType.BYTE_ARRAY);
            builder.appendBytes((byte[]) obj);
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
                    appendByteValue(builder, v);
                    return;

                case INT16:
                    appendShortValue(builder, v);
                    return;

                case INT32:
                    appendIntValue(builder, v);
                    return;

                case INT64:
                    appendLongValue(builder, v);
                    return;

                case FLOAT:
                    appendFloatValue(builder, v);
                    return;

                case DOUBLE:
                    appendDoubleValue(builder, v);
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

                default:
                    throw new IllegalArgumentException("Unsupported type: " + type);
            }
        } catch (ClassCastException e) {
            NativeType nativeType = NativeTypes.fromObject(v);

            if (nativeType == null) {
                // Unsupported type (does not map to any Ignite type) - throw (same behavior as embedded).
                throw new MarshallerException(
                        String.format(
                                "Invalid value type provided for column [name='%s', expected='%s', actual='%s']",
                                name,
                                type.javaClass().getName(),
                                v.getClass().getName()),
                        e);
            }

            ColumnType actualType = nativeType.spec();

            // Exception message is similar to embedded mode - see o.a.i.i.schema.Column#validate
            String error = format(
                    "Value type does not match [column='{}', expected={}, actual={}]",
                    name, type.name(), actualType.name()
            );

            throw new MarshallerException(error, e);
        }
    }

    private static void appendByteValue(BinaryTupleBuilder builder, Object val) {
        if (val instanceof Byte) {
            builder.appendByte((byte) val);
            return;
        }

        if (val instanceof Short) {
            short shortVal = (short) val;
            byte byteVal = (byte) shortVal;

            if (shortVal == byteVal) {
                builder.appendByte(byteVal);
                return;
            }
        }

        if (val instanceof Integer) {
            int intVal = (int) val;
            byte byteVal = (byte) intVal;

            if (intVal == byteVal) {
                builder.appendByte(byteVal);
                return;
            }
        }

        if (val instanceof Long) {
            long longVal = (long) val;
            byte byteVal = (byte) longVal;

            if (longVal == byteVal) {
                builder.appendByte(byteVal);
                return;
            }
        }

        throw new ClassCastException("Cannot cast to byte: " + val.getClass());
    }

    private static void appendShortValue(BinaryTupleBuilder builder, Object val) {
        if (val instanceof Short) {
            builder.appendShort((short) val);
            return;
        }

        if (val instanceof Byte) {
            builder.appendShort((byte) val);
            return;
        }

        if (val instanceof Integer) {
            int intVal = (int) val;
            short shortVal = (short) intVal;

            if (intVal == shortVal) {
                builder.appendShort(shortVal);
                return;
            }
        }

        if (val instanceof Long) {
            long longVal = (long) val;
            short shortVal = (short) longVal;

            if (longVal == shortVal) {
                builder.appendShort(shortVal);
                return;
            }
        }

        throw new ClassCastException("Cannot cast to short: " + val.getClass());
    }

    private static void appendIntValue(BinaryTupleBuilder builder, Object val) {
        if (val instanceof Integer) {
            builder.appendInt((int) val);
            return;
        }

        if (val instanceof Short) {
            builder.appendInt((short) val);
            return;
        }

        if (val instanceof Byte) {
            builder.appendInt((byte) val);
            return;
        }

        if (val instanceof Long) {
            long longVal = (long) val;
            int intVal = (int) longVal;

            if (longVal == intVal) {
                builder.appendInt(intVal);
                return;
            }
        }

        throw new ClassCastException("Cannot cast to int: " + val.getClass());
    }

    private static void appendLongValue(BinaryTupleBuilder builder, Object val) {
        if (val instanceof Integer) {
            builder.appendLong((int) val);
            return;
        }

        if (val instanceof Short) {
            builder.appendLong((short) val);
            return;
        }

        if (val instanceof Byte) {
            builder.appendLong((byte) val);
            return;
        }

        if (val instanceof Long) {
            builder.appendLong((long) val);
            return;
        }

        throw new ClassCastException("Cannot cast to long: " + val.getClass());
    }

    private static void appendFloatValue(BinaryTupleBuilder builder, Object val) {
        if (val instanceof Float) {
            builder.appendFloat((float) val);
            return;
        }

        if (val instanceof Double) {
            double doubleVal = (double) val;
            float floatVal = (float) doubleVal;

            if (doubleVal == floatVal || Double.isNaN(doubleVal)) {
                builder.appendFloat(floatVal);
                return;
            }
        }

        throw new ClassCastException("Cannot cast to float: " + val.getClass());
    }

    private static void appendDoubleValue(BinaryTupleBuilder builder, Object val) {
        if (val instanceof Double) {
            builder.appendDouble((double) val);
            return;
        }

        if (val instanceof Float) {
            builder.appendDouble((float) val);
            return;
        }

        throw new ClassCastException("Cannot cast to double: " + val.getClass());
    }

    private static void appendTypeAndScale(BinaryTupleBuilder builder, ColumnType type, int scale) {
        builder.appendInt(type.id());
        builder.appendInt(scale);
    }

    private static void appendTypeAndScale(BinaryTupleBuilder builder, ColumnType type) {
        builder.appendInt(type.id());
        builder.appendInt(0);
    }

    static IgniteException unsupportedTypeException(int dataType) {
        return new IgniteException(PROTOCOL_ERR, "Unsupported type: " + dataType);
    }

    static IgniteException unsupportedTypeException(Class<?> cls) {
        return new IgniteException(PROTOCOL_ERR, "Unsupported type: " + cls);
    }
}
