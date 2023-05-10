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
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomType;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.jetbrains.annotations.Nullable;

/**
 * Produces values of {@link NativeType}s.
 */
public final class NativeTypeValues {

    private NativeTypeValues() {

    }

    /** Returns a list of values of a {@link NativeType} that corresponds to the given {@link RelDataType}. */
    public static List<Object> values(RelDataType type, int count) {
        List<Object> result = new ArrayList<>(count);
        for (var i = 0; i < count; i++) {
            result.add(value(type, i));
        }
        return result;
    }

    /** Returns a value of a {@link NativeType} that corresponds to the given {@link RelDataType}. */
    @Nullable
    public static Object value(RelDataType type, int i) {
        switch (type.getSqlTypeName()) {
            case BOOLEAN:
                return true;
            case TINYINT:
                return (byte) i;
            case SMALLINT:
                return (short) i;
            case INTEGER:
                return i;
            case BIGINT:
                return (long) i;
            case DECIMAL:
                return new BigDecimal("0.02").add(BigDecimal.ONE);
            case FLOAT:
            case REAL:
                return (float) i;
            case DOUBLE:
                return (double) i;
            case DATE:
                return LocalDate.of(2022, 1, 1).plusDays(i);
            case TIME:
                return LocalTime.of(12, 0, 0, 1).plusMinutes(i);
            case TIME_WITH_LOCAL_TIME_ZONE:
                throw new IllegalArgumentException("Time with local time zone is not supported yet.");
            case TIMESTAMP:
                LocalDateTime dateTime = LocalDateTime.of(2022, 1, 1, 1, 0, 0, 0).plusSeconds(i);
                return Instant.from(ZonedDateTime.of(dateTime, ZoneId.of("UTC")));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                throw new IllegalArgumentException("Timestamp with local time zone is not supported yet.");
            case INTERVAL_YEAR:
                return Period.ofYears(i);
            case INTERVAL_YEAR_MONTH:
                return Period.ofYears(1).plusMonths(i);
            case INTERVAL_MONTH:
                return Period.ofMonths(i);
            case INTERVAL_DAY:
                return Duration.ofDays(i);
            case INTERVAL_DAY_HOUR:
                return Duration.ofDays(1).plusHours(i);
            case INTERVAL_DAY_MINUTE:
                return Duration.ofDays(1).plusMinutes(i);
            case INTERVAL_DAY_SECOND:
                return Duration.ofDays(1).plusSeconds(i);
            case INTERVAL_HOUR:
                return Duration.ofHours(1);
            case INTERVAL_HOUR_MINUTE:
                return Duration.ofHours(1).plusMinutes(i);
            case INTERVAL_HOUR_SECOND:
                return Duration.ofHours(1).plusSeconds(i);
            case INTERVAL_MINUTE:
                return Duration.ofMinutes(1);
            case INTERVAL_MINUTE_SECOND:
                return Duration.ofMinutes(1).plusSeconds(i);
            case INTERVAL_SECOND:
                return Duration.ofSeconds(i);
            case CHAR:
            case VARCHAR:
                return "abc" + i;
            case BINARY:
            case VARBINARY:
                return new byte[]{(byte) i};
            case NULL:
                return null;
            case ANY:
                IgniteCustomType customType = (IgniteCustomType) type;
                if (UuidType.NAME.equals(customType.getCustomTypeName())) {
                    return new UUID(i, 0);
                }
                //fallthrough
            case UNKNOWN:
            case SYMBOL:
            case MULTISET:
            case ARRAY:
            case MAP:
            case DISTINCT:
            case STRUCTURED:
            case ROW:
            case OTHER:
            case CURSOR:
            case COLUMN_LIST:
            case DYNAMIC_STAR:
            case GEOMETRY:
            case MEASURE:
            case SARG:
            default:
                throw new IllegalArgumentException("Type is not supported: " + type);
        }
    }
}
