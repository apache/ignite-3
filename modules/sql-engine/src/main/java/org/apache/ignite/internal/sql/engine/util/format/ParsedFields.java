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

package org.apache.ignite.internal.sql.engine.util.format;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.time.Clock;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.FieldKind;
import org.jetbrains.annotations.Nullable;

/**
 * Parsed fields.
 */
final class ParsedFields {

    private final Map<FieldKind, Object> values = new EnumMap<>(FieldKind.class);

    /** AM/PM, hours-12. */
    private IntPair hours12Value;

    /** Tz hours, tz minutes, both positive or negative. */
    private IntPair tzValue;

    /** base years, year number. */
    private IntPair yearValue;

    private Integer roundedYearValue;

    void addYear(int base, int value) {
        this.yearValue = IntPair.of(base, value);
    }

    void addRoundedYear(int years) {
        this.roundedYearValue = years;
    }

    void add(FieldKind field, Object value) {
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(value, "value");

        assert field != FieldKind.HOUR_12 : "use add hours12";
        assert field != FieldKind.TIMEZONE : "use add tz";

        Object prev = this.values.put(field, value);
        if (prev != null) {
            throw new IllegalStateException("Field appeared more than once " + field);
        }
    }

    void addHours12(boolean pm, int hours) {
        hours12Value = IntPair.of(pm ? 1 : 0, hours);
    }

    void addTz(int hours, int minutes) {
        tzValue = IntPair.of(hours, minutes);
    }

    LocalTime getTime() {
        if (values.containsKey(FieldKind.SECOND_OF_DAY)) {
            long seconds = fieldValue(FieldKind.SECOND_OF_DAY, 0);
            long nanos = fieldValue(FieldKind.FRACTION, 0);

            if (nanos == 0) {
                return LocalTime.ofSecondOfDay(seconds);
            } else {
                long ns = TimeUnit.SECONDS.toNanos(1);
                long value = (seconds * ns + nanos) / ns;

                return LocalTime.ofSecondOfDay(value);
            }
        } else {
            int hours;

            if (hours12Value != null) {
                hours = convertToHours24(hours12Value);
            } else {
                hours = fieldValue(FieldKind.HOUR_24, 0);
            }

            int minutes = fieldValue(FieldKind.MINUTE, 0);
            int seconds = fieldValue(FieldKind.SECOND_OF_MINUTE, 0);
            int nanos = fieldValue(FieldKind.FRACTION, 0);

            return LocalTime.of(hours, minutes, seconds, nanos);
        }
    }

    LocalDate getDate(Clock clock) {
        Objects.requireNonNull(clock, "clock");

        LocalDate now = LocalDate.now(clock);

        int year;
        if (roundedYearValue != null) {
            year = convertRoundedYear(clock, roundedYearValue);
        } else if (yearValue != null) {
            year = convertYear(yearValue);
        } else {
            year = Year.now(clock).getValue();
        }

        if (values.containsKey(FieldKind.DAY_OF_YEAR)) {
            int day = fieldValue(FieldKind.DAY_OF_YEAR, 0);
            return LocalDate.ofYearDay(year, day);
        } else {
            int month = fieldValue(FieldKind.MONTH, now.getMonthValue());
            int day = fieldValue(FieldKind.DAY_OF_MONTH, now.getDayOfMonth());

            return LocalDate.of(year, month, day);
        }
    }

    LocalDateTime getDateTime(Clock clock) {
        Objects.requireNonNull(clock, "clock");

        LocalTime time = getTime();
        LocalDate date = getDate(clock);

        return LocalDateTime.of(date, time);
    }

    @Nullable
    ZoneOffset toZoneOffset() {
        if (tzValue == null) {
            return null;
        }

        // ZoneOffset factory method checks hours.
        int hours = tzValue.source;
        int minutes = tzValue.target;
        // minutes are signed.
        validateRange(Math.abs(minutes), 0, 59, DateTimeTemplateField.TZM.displayName());

        return ZoneOffset.ofHoursMinutes(hours, minutes);
    }

    private int fieldValue(FieldKind kind, int defaultValue) {
        return (Integer) values.getOrDefault(kind, defaultValue);
    }

    private int convertRoundedYear(Clock clock, int v) {
        int now = Year.now(clock).getValue();
        int year2digits = now % 100;
        int base = now - year2digits;

        if (v <= 49) {
            return (year2digits <= 49) ? base + v   // same century
                    : base + 100 + v; // next century
        } else if (v < 100) { // 50-99
            return (year2digits <= 49) ? base - 100 + v // previous century
                    : base + v;       // same century
        } else {
            return v;
        }
    }

    private int convertYear(IntPair baseAndYears) {
        int value = baseAndYears.source + baseAndYears.target;
        validateRange(value, 1, 9999, DateTimeTemplateField.YYYY.displayName());
        return value;
    }

    private int convertToHours24(IntPair hours12) {
        boolean pm = hours12.source == 1;
        int value = hours12.target;

        validateRange(value, 1, 12, DateTimeTemplateField.HH12.displayName());

        if (pm) {
            if (value == 12) {
                return hours12.target;
            } else {
                return hours12.target % 12 + 12;
            }
        } else {
            return value % 12;
        }
    }

    private static void validateRange(int val, int min, int max, String field) {
        if (val < min || val > max) {
            String error = format("Invalid value for {} (valid values {} - {}): {}", field, min, max, val);
            throw new DateTimeException(error);
        }
    }
}
