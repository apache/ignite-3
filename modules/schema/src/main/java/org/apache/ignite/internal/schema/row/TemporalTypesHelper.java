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
 * <p>
 * DATE is a fixed-length type which compacted representation keeps ordering, value is signed and fit into a 3-bytes.
 * Thus, DATE value can be compared by bytes where first byte is signed and others - unsigned.
 * Thus temporal functions, like YEAR(), can easily extracts fields with a mask,
 * <p>
 * Date compact structure:
 * ┌──────────────┬─────────┬────────┐
 * │ Year(signed) │ Month   │ Day    │
 * ├──────────────┼─────────┼────────┤
 * │ 15 bits      │ 4 bits  │ 5 bits │
 * └──────────────┴─────────┴────────┘
 * <p>
 * TIME is a fixed-length type supporting accuracy for millis, micros or nanos.
 * Compacted time representation keeps ordering, values fits to 8-bytes value ({@code long}) with padding
 * can be compared as just unsigned bytes.
 * Padding part is filled with zeroes.
 * Subsecond part stores in a separate bit sequence which is omited for {@code 0} accuracy.
 * <p>
 * Total value size is 3/7 bytes depensing on the type accuracy.
 * <p>
 * Time compact structure:
 * ┌─────────┬─────────┬──────────┬──────────┬───────────────┐
 * │ Padding │ Hours   │ Minutes  │ Seconds  │ Subseconds    │
 * ├─────────┼─────────┼──────────┼──────────┼───────────────┤
 * │ 7  bits │ 5 bit   │ 6 bits   │ 6 bit    │ 0/32 bits     │
 * └─────────┴─────────┴──────────┴──────────┴───────────────┘
 * <p>
 * DATETIME is just a concatenation of DATE and TIME values.
 * <p>
 * TIMESTAMP has similart structure to {@link java.time.Instant} and supports accuracy for millis, micros or nanos.
 * Subsecond part stores in a separate bit sequence which is omited for {@code 0} accuracy.
 * <p>
 * Total value size is 8/12 bytes depensing on the type accuracy.
 * <p>
 * Timestamp compact structure:
 * ┌──────────────────────────┬─────────────┐
 * │ Seconds since the epoch  │ Subseconds  │
 * ├──────────────────────────┼─────────────┤
 * │    64 bits               │ 0/32 bits   │
 * └──────────────────────────┴─────────────┘
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

    /** Seconds field offset. */
    public static final int SECONDS_FIELD_OFFSET = 32;

    /** Minutes field offset. */
    public static final int MINUTES_FIELD_OFFSET = SECONDS_FIELD_OFFSET + SECONDS_FIELD_LENGTH;

    /** Hout field offset. */
    public static final int HOUR_FIELD_OFFSET = MINUTES_FIELD_OFFSET + MINUTES_FIELD_LENGTH;

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
     * @param type Native temporal type.
     * @param time Time.
     * @return Encoded time. Last 7 bytes meaningful.
     */
    public static long encodeTime(TemporalNativeType type, LocalTime time) {
        long val = (long)time.getHour() << HOUR_FIELD_OFFSET;
        val |= (long)time.getMinute() << MINUTES_FIELD_OFFSET;
        val |= (long)time.getSecond() << SECONDS_FIELD_OFFSET;

        long nanos = normalizeNanos(time.getNano(), type.precision()); // Implicit cast of unsinged int (30-bit) to long.

        return val | nanos;
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

        return val & (0x00FF_FFFF);
    }

    /**
     * Expands to LocalTime.
     *
     * @param time Encoded time.
     * @param prec Precision.
     * @return LocalTime instance.
     */
    public static LocalTime decodeTime(long time) {
        int sec = (int)(time >> SECONDS_FIELD_OFFSET) & mask(SECONDS_FIELD_LENGTH);
        int min = (int)(time >>> MINUTES_FIELD_OFFSET) & mask(MINUTES_FIELD_LENGTH);
        int hour = (int)(time >>> HOUR_FIELD_OFFSET) & mask(HOUR_FIELD_LENGTH);
        int nanos = (int)(time & 0xFFFF_FFFFL);

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

    /**
     * Normalize nanos regarding the presicion.
     *
     * @param nanos Nanoseconds.
     * @param precision Meaningful digits.
     * @return Normalized nanoseconds.
     */
    public static int normalizeNanos(int nanos, int precision) {
        switch (precision) {
            case 0:
                nanos = 0;
                break;
            case 1:
                nanos = (nanos / 100_000_000) * 100_000_000; // 100ms accuracy.
                break;
            case 2:
                nanos = (nanos / 10_000_000) * 10_000_000; // 10ms accuracy.
                break;
            case 3: {
                nanos = (nanos / 1_000_000) * 1_000_000; // 1ms accuracy.
                break;
            }
            case 4: {
                nanos = (nanos / 100_000) * 100_000; // 100mcs accuracy.
                break;
            }
            case 5: {
                nanos = (nanos / 10_000) * 10_000; // 10mcs accuracy.
                break;
            }
            case 6: {
                nanos = (nanos / 1_000) * 1_000; // 1mcs accuracy.
                break;
            }
            case 7: {
                nanos = (nanos / 100) * 100; // 100ns accuracy.
                break;
            }
            case 8: {
                nanos = (nanos / 10) * 10; // 10ns accuracy.
                break;
            }
            case 9: {
                nanos = nanos; // 1ns accuracy
                break;
            }
            default: // Should never get here.
                throw new IllegalArgumentException("Unsupported fractional seconds precision: " + precision);
        }

        return nanos;
    }
}
