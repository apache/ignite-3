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

package org.apache.ignite.internal.sql.engine.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Produces values of {@link NativeType}s.
 */
public final class NativeTypeValues {

    private NativeTypeValues() {

    }

    /**
     * Returns a list that contains {@code count} elements of a {@link NativeType native type}
     * that corresponds to the given {@link RelDataType}.
     */
    public static List<Object> values(RelDataType type, int count) {
        List<Object> result = new ArrayList<>(count);
        for (var i = 0; i < count; i++) {
            result.add(value(i, type));
        }
        return result;
    }

    /** Returns a value of a {@link NativeType native type} that corresponds to the given {@link ColumnType}. */
    @Nullable
    public static Object value(int i, ColumnType type) {
        switch (type) {
            case BOOLEAN:
                return i % 2 == 0;
            case INT8:
                return (byte) i;
            case INT16:
                return (short) i;
            case INT32:
                return i;
            case INT64:
                return (long) i;
            case FLOAT:
                return i + ((float) i / 1000);
            case DOUBLE:
                return i + ((double) i / 1000);
            case STRING:
                return "str_" + i;
            case BYTE_ARRAY:
                return new byte[]{(byte) i, (byte) (i + 1), (byte) (i + 2)};
            case NULL:
                return null;
            case DECIMAL:
                return BigDecimal.valueOf(i + ((double) i / 1000));
            case NUMBER:
                return BigInteger.valueOf(i);
            case UUID:
                return new UUID(i, i);
            case BITMASK:
                return new byte[]{(byte) i};
            case DURATION:
                return Duration.ofNanos(i);
            case DATETIME:
                return LocalDateTime.of(
                        (LocalDate) value(i, ColumnType.DATE),
                        (LocalTime) value(i, ColumnType.TIME)
                );
            case TIMESTAMP:
                return Instant.from(ZonedDateTime.of((LocalDateTime) value(i, ColumnType.DATETIME), ZoneId.systemDefault()));
            case DATE:
                return LocalDate.of(2022, 1, 1).plusDays(i % 30);
            case TIME:
                return LocalTime.of(0, 0, 0).plusSeconds(i % 1000);
            case PERIOD:
                return Period.of(i % 2, i % 12, i % 29);
            default:
                throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    /** Returns a value of a {@link NativeType native type} that corresponds to the given {@link NativeTypeSpec}. */
    public static Object value(int i, NativeTypeSpec spec) {
        Object value = value(i, spec.asColumnType());

        assert value != null : "Returned a null value for " + spec;
        return value;
    }

    /** Returns a value of a {@link NativeType native type} that corresponds to the given {@link RelDataType}. */
    @Nullable
    public static Object value(int i, RelDataType type) {
        ColumnType columnType = TypeUtils.columnType(type);

        assert columnType != null : "Returned a null column type for " + type;
        return value(i, columnType);
    }
}
