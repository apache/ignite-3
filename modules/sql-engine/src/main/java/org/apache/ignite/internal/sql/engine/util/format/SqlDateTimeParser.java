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

import java.time.Clock;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.FieldKind;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.SqlException;

/**
 * SQL date/time parser.
 */
public final class SqlDateTimeParser {

    private static final Clock CLOCK = Clock.systemDefaultZone();

    private static final Set<FieldKind> DATE_FIELDS = Set.of(
            FieldKind.YEAR, FieldKind.ROUNDED_YEAR,
            FieldKind.MONTH,
            FieldKind.DAY_OF_MONTH,
            FieldKind.DAY_OF_YEAR
    );

    private static final Set<FieldKind> TIME_FIELDS = Set.of(
            FieldKind.HOUR_12, FieldKind.HOUR_24,
            FieldKind.MINUTE,
            FieldKind.SECOND_OF_MINUTE,
            FieldKind.SECOND_OF_DAY,
            FieldKind.FRACTION,
            FieldKind.AM_PM
    );

    private static final Set<FieldKind> TIMESTAMP_FIELDS = Set.of(
            FieldKind.YEAR, FieldKind.ROUNDED_YEAR,
            FieldKind.MONTH,
            FieldKind.DAY_OF_MONTH,
            FieldKind.DAY_OF_YEAR,
            FieldKind.HOUR_12, FieldKind.HOUR_24,
            FieldKind.MINUTE,
            FieldKind.SECOND_OF_MINUTE,
            FieldKind.SECOND_OF_DAY,
            FieldKind.FRACTION,
            FieldKind.AM_PM
    );

    private final List<DateTimeFormatElement> elements;

    SqlDateTimeParser(List<DateTimeFormatElement> elements) {
        this.elements = elements;
    }

    /**
     * Creates an instance of a time formatter.
     *
     * @param pattern Pattern.
     * @return Formatter.
     */
    public static SqlDateTimeParser timeFormatter(String pattern) {
        try {
            return new SqlDateTimeParser(new Scanner(pattern, "TIME", TIME_FIELDS).scan());
        } catch (DateTimeException e) {
            throw new SqlException(Sql.RUNTIME_ERR, e.getMessage(), e);
        }
    }

    /**
     * Parse an input into a time value according to format rules.
     *
     * @param input Input.
     * @return Time value.
     */
    public LocalTime parseTime(String input) {
        Objects.requireNonNull(input, "input");

        Parser parser = new Parser(elements);

        try {
            ParsedFields fields = parser.parse(input);
            return fields.getTime();
        } catch (DateTimeException e) {
            throw new SqlException(Sql.RUNTIME_ERR, e.getMessage(), e);
        }
    }

    /**
     * Creates an instance of a date formatter.
     *
     * @param pattern Pattern.
     * @return Formatter.
     */
    public static SqlDateTimeParser dateFormatter(String pattern) {
        try {
            return new SqlDateTimeParser(new Scanner(pattern, "DATE", DATE_FIELDS).scan());
        } catch (DateTimeException e) {
            throw new SqlException(Sql.RUNTIME_ERR, e.getMessage(), e);
        }
    }

    /**
     * Parse an input into a date value according to format rules.
     *
     * @param input Input.
     * @return Date value.
     */
    public LocalDate parseDate(String input) {
        return parseDate(input, CLOCK);
    }

    /**
     * Parse an input into a date value according to format rules.
     *
     * @param input Input.
     * @param clock Clock.
     * @return Date value.
     */
    public LocalDate parseDate(String input, Clock clock) {
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(clock, "clock");

        Parser parser = new Parser(elements);

        try {
            ParsedFields fields = parser.parse(input);
            return fields.getDate(clock);
        } catch (DateTimeException e) {
            throw new SqlException(Sql.RUNTIME_ERR, e.getMessage(), e);
        }
    }

    /**
     * Creates an instance of a timestamp formatter.
     *
     * @param pattern Pattern.
     * @return Formatter.
     */
    public static SqlDateTimeParser timestampFormatter(String pattern) {
        try {
            return new SqlDateTimeParser(new Scanner(pattern, "TIMESTAMP", TIMESTAMP_FIELDS).scan());
        } catch (DateTimeException e) {
            throw new SqlException(Sql.RUNTIME_ERR, e.getMessage(), e);
        }
    }

    /**
     * Parse an input into a timestamp value according to format rules.
     *
     * @param input Input.
     * @return Timestamp value.
     */
    public LocalDateTime parseTimestamp(String input) {
        return parseTimestamp(input, CLOCK);
    }

    /**
     * Parse an input into a timestamp value according to format rules.
     *
     * @param input Input.
     * @param clock Clock.
     * @return Timestamp value.
     */
    public LocalDateTime parseTimestamp(String input, Clock clock) {
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(clock, "clock");

        Parser parser = new Parser(elements);

        try {
            ParsedFields fields = parser.parse(input);
            LocalDate date = fields.getDate(clock);
            LocalTime time = fields.getTime();

            return LocalDateTime.of(date, time);
        } catch (DateTimeException e) {
            throw new SqlException(Sql.RUNTIME_ERR, e.getMessage(), e);
        }
    }
}
