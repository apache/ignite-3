/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema.row;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import org.apache.ignite.internal.schema.TemporalNativeType;

/**
 * Helper class for temporal type conversions.
 * <p>
 * Provides methods to encode/decode temporal types in a compact way  for futher writing to row.
 * Conversion preserves natural type order.
 *
 * @see org.apache.ignite.internal.schema.row.Row
 * @see org.apache.ignite.internal.schema.row.RowAssembler
 */
public class TemporalTypesHelper {
    /** Month field length. */
    public static final int MONTH_FIELD_LENGTH = 4;

    /** Day field length. */
    public static final int DAY_FIELD_LENGTH = 5;

    /** Hours field length. */
    public static final int HOUR_FIELD_LENGTH = 5;

    /** Minutes field length. */
    public static final int MINUTES_FIELD_LENGTH = 6;

    /** Seconds field length. */
    public static final int SECONDS_FIELD_LENGTH = 6;

    /** Milliseconds field length. */
    public static final int MILLIS_FIELD_LENGTH = 10;

    /** Microseconds field length. */
    public static final int MICROS_FIELD_LENGTH = 20;

    /** Nanoseconds field length. */
    public static final int NANOS_FIELD_LENGTH = 30;

    /** Max year boundary. */
    public static final int MAX_YEAR = (1 << 14) - 1;

    /** Min year boundary. */
    public static final int MIN_YEAR = -(1 << 14);

    /** Time precision unit. */
    public static final ChronoUnit TIME_PRECISION = ChronoUnit.MICROS;

    /**
     * @param len Mask length in bits.
     * @return Mask.
     */
    private static int mask(int len) {
        return (1 << len) - 1;
    }

    /**
     * Compact LocalTime.
     *
     * @param type
     * @param time Time.
     * @return Encoded time.
     */
    public static long compactTime(TemporalNativeType type, LocalTime time) {
        long val = (long)time.getHour() << MINUTES_FIELD_LENGTH;
        val = (val | (long)time.getMinute()) << SECONDS_FIELD_LENGTH;

        switch (type.precision()) {
            case 3:
                val = (val | (long)time.getSecond()) << MILLIS_FIELD_LENGTH;
                val |= (long)time.getNano() / 1_000_000; // Conver to millis.

                return val;

            case 6:
                val = (val | (long)time.getSecond()) << MICROS_FIELD_LENGTH;
                val |= (long)time.getNano() / 1_000; // Conver to micros.

                return val;

            case 9:
                val = (val | (long)time.getSecond()) << NANOS_FIELD_LENGTH;
                val |= (long)time.getNano();

                return val;

            default: // Should never get here.
                throw new IllegalArgumentException("Unsupported time precision: " + type.precision());
        }
    }

    /**
     * Compact LocalDate.
     *
     * @param date Date.
     * @return Encoded date.
     */
    public static int encodeDate(LocalDate date) {
        int val = date.getYear() << MONTH_FIELD_LENGTH;
        val = (val | date.getMonthValue()) << DAY_FIELD_LENGTH;
        val |= date.getDayOfMonth();

        return val & (0x00FFFFFF);
    }

    /**
     * Expands to LocalTime.
     *
     * @param time Encoded time.
     * @param prec Precision.
     * @return LocalTime instance.
     */
    public static LocalTime decodeTime(long time, int prec) {
        int nanos;

        switch (prec) {
            case 3:
                nanos = (int)(time & mask(MILLIS_FIELD_LENGTH)) * 1_000_000;
                time >>>= MILLIS_FIELD_LENGTH;
                break;
            case 6:
                nanos = (int)(time & mask(MICROS_FIELD_LENGTH)) * 1_000;
                time >>>= MICROS_FIELD_LENGTH;
                break;
            case 9:
                nanos = (int)(time & mask(NANOS_FIELD_LENGTH));
                time >>>= NANOS_FIELD_LENGTH;
                break;

            default: // Should never get here.
                throw new IllegalArgumentException("Unsupported time precision: " + prec);
        }

        int sec = (int)(time & mask(SECONDS_FIELD_LENGTH));
        int min = (int)((time >>>= SECONDS_FIELD_LENGTH) & mask(MINUTES_FIELD_LENGTH));
        int hour = (int)((time >>> MINUTES_FIELD_LENGTH) & mask(HOUR_FIELD_LENGTH));

        return LocalTime.of(hour, min, sec, nanos);
    }

    /**
     * Expands to LocalDate.
     *
     * @param date Encoded date.
     * @return LocalDate instance.
     */
    public static LocalDate decodeDate(int date) {
        date = (date << 8) >> 8; // Restore sign.

        int day = (date) & mask(DAY_FIELD_LENGTH);
        int mon = (date >>= DAY_FIELD_LENGTH) & mask(MONTH_FIELD_LENGTH); // Sign matters.
        int year = (date >> MONTH_FIELD_LENGTH); // Sign matters.

        return LocalDate.of(year, mon, day);
    }
}
