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

import static org.apache.calcite.avatica.util.DateTimeUtils.dateStringToUnixDate;

import java.time.Year;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.engine.exec.exp.IgniteSqlFunctions;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

/**
 * Contains a set of utility methods for converting temporal types.
 */
public class IgniteSqlDateTimeUtils {
    /** Regex for time. */
    private static final Pattern TIME_PATTERN =
            Pattern.compile("^(\\d+):(\\d+):(\\d+)(\\.\\d*)?$");

    /** Regex for date. */
    private static final Pattern DATE_PATTERN =
            Pattern.compile("^(\\d{4})-([0]\\d|1[0-2])-([0-2]\\d|3[01])$");

    /** The maximum number of digits after the decimal point that are taken into account. */
    private static final int USEFUL_FRACTION_OF_SECOND_LENGTH = 3;

    private static final int[] FRACTION_OF_SECOND_MULTIPLIERS = {100, 10, 1};

    /** Returns the timestamp value minus the offset of the specified timezone. */
    public static Long subtractTimeZoneOffset(long timestamp, TimeZone timeZone) {
        // A second offset calculation is required to handle DST transition period correctly.
        int offset = timeZone.getOffset(timestamp - timeZone.getOffset(timestamp));

        // After adjusting to UTC, you need to make sure that the value matches the allowed values.
        return IgniteSqlFunctions.toTimestampLtzExact(timestamp - offset);
    }

    /** Returns the timestamp value truncated to the specified fraction of a second. */
    @Contract("null, _ -> null")
    public static @Nullable Long adjustTimestampMillis(@Nullable Long timestamp, int fractionOfSecond) {
        if (timestamp == null) {
            return null;
        }

        assert fractionOfSecond >= 0;

        long unit;

        switch (fractionOfSecond) {
            case 0:
                unit = 1000;
                break;

            case 1:
                unit = 100;
                break;

            case 2:
                unit = 10;
                break;

            default:
                return timestamp;
        }

        return timestamp - Math.floorMod(timestamp, unit);
    }

    /** Returns the time value truncated to the specified fraction of a second. */
    @Contract("null, _ -> null")
    public static @Nullable Integer adjustTimeMillis(@Nullable Integer time, int fractionOfSecond) {
        if (time == null) {
            return null;
        }

        assert time >= 0 : time;
        assert fractionOfSecond >= 0;

        int unit;

        switch (fractionOfSecond) {
            case 0:
                unit = 1000;
                break;

            case 1:
                unit = 100;
                break;

            case 2:
                unit = 10;
                break;

            default:
                return time;
        }

        return time - (time % unit);
    }

    /**
     * SQL {@code CURRENT_DATE} function.
     */
    public static int currentDate(DataContext ctx) {
        // We using LOCAL_TIMESTAMP, because CURRENT_TIMESTAMP returns TIMESTAMP_LTZ
        // which requires to be converted to TIMESTAMP taking into account client time zone.
        // At the same time, LOCAL_TIMESTAMP already has the required value.
        long timestamp = DataContext.Variable.LOCAL_TIMESTAMP.get(ctx);
        int date = SqlFunctions.timestampToDate(timestamp);
        int time = SqlFunctions.timestampToTime(timestamp);
        if (time < 0) {
            --date;
        }
        return date;
    }

    /**
     * Helper for CAST({time} AS VARCHAR(n)).
     *
     * <p>Note: this method is a copy of the avatica {@link DateTimeUtils#unixTimestampToString(long, int)} method,
     *          with the only difference being that it does not add trailing zeros.
     */
    public static String unixTimeToString(int time, int precision) {
        IgniteStringBuilder buf = new IgniteStringBuilder(8 + (precision > 0 ? 1 + precision : 0));

        unixTimeToString(buf, time, precision);

        return buf.toString();
    }

    private static void unixTimeToString(IgniteStringBuilder buf, int time, int precision) {
        int h = time / 3600000;
        int time2 = time % 3600000;
        int m = time2 / 60000;
        int time3 = time2 % 60000;
        int s = time3 / 1000;
        int ms = time3 % 1000;

        buf.app((char) ('0' + (h / 10) % 10))
                .app((char) ('0' + h % 10))
                .app(':')
                .app((char) ('0' + (m / 10) % 10))
                .app((char) ('0' + m % 10))
                .app(':')
                .app((char) ('0' + (s / 10) % 10))
                .app((char) ('0' + s % 10));

        if (precision == 0 || ms == 0) {
            return;
        }

        buf.app('.');
        do {
            buf.app((char) ('0' + (ms / 100)));

            ms = ms % 100;
            ms = ms * 10;
            --precision;
        } while (ms > 0 && precision > 0);
    }

    /**
     * Helper for CAST({timestamp} AS VARCHAR(n)).
     *
     * <p>Note: this method is a copy of the avatica {@link DateTimeUtils#unixTimestampToString(long, int)} method,
     *          with the only difference being that it does not add trailing zeros.
     */
    public static String unixTimestampToString(long timestamp, int precision) {
        IgniteStringBuilder buf = new IgniteStringBuilder(17);
        int date = (int) (timestamp / DateTimeUtils.MILLIS_PER_DAY);
        int time = (int) (timestamp % DateTimeUtils.MILLIS_PER_DAY);

        if (time < 0) {
            --date;

            time += (int) DateTimeUtils.MILLIS_PER_DAY;
        }

        buf.app(DateTimeUtils.unixDateToString(date)).app(' ');

        unixTimeToString(buf, time, precision);

        return buf.toString();
    }

    /**
     * Converts time string into unix time.
     *
     * <p>Note: this method is a copy of the avatica {@link DateTimeUtils#timeStringToUnixDate(String)} method,
     *          with the only difference that result is truncated to milliseconds without rounding.
     *
     * @param value Time string.
     * @return Timestamp.
     */
    public static int timeStringToUnixDate(String value) {
        value = value.trim();

        validateTime(value, value);
        return timeStringToUnixDate(value, value);
    }

    private static int timeStringToUnixDate(String v, String full) {
        int colon1 = v.indexOf(':');
        int hour;
        int minute;
        int second;
        int milli = 0;
        try {
            if (colon1 < 0) {
                hour = Integer.parseInt(v.trim());
                minute = 0;
                second = 0;
            } else {
                hour = Integer.parseInt(v.substring(0, colon1).trim());
                int colon2 = v.indexOf(':', colon1 + 1);
                if (colon2 < 0) {
                    minute = Integer.parseInt(v.substring(colon1 + 1).trim());
                    second = 0;
                } else {
                    minute = Integer.parseInt(v.substring(colon1 + 1, colon2).trim());
                    int dot = v.indexOf('.', colon2);
                    if (dot < 0) {
                        second = Integer.parseInt(v.substring(colon2 + 1).trim());
                    } else {
                        second = Integer.parseInt(v.substring(colon2 + 1, dot).trim());
                        String fraction = v.substring(dot + 1).trim();
                        for (int i = 0; i < Math.min(fraction.length(), USEFUL_FRACTION_OF_SECOND_LENGTH); i++) {
                            int x = fraction.charAt(i) - '0';

                            assert x >= 0 && x <= 9 : x;

                            milli += FRACTION_OF_SECOND_MULTIPLIERS[i] * x;
                        }
                    }
                }
            }
        } catch (NumberFormatException e) {
            throw invalidType("TIME", full, e);
        }

        return hour * (int) DateTimeUtils.MILLIS_PER_HOUR
                + minute * (int) DateTimeUtils.MILLIS_PER_MINUTE
                + second * (int) DateTimeUtils.MILLIS_PER_SECOND
                + milli;
    }

    /**
     * Converts timestamp string into unix timestamp.
     *
     * <p>Note: this method is a copy of the avatica {@link DateTimeUtils#timestampStringToUnixDate(String)} method,
     *          with the only difference that result is truncated to milliseconds without rounding.
     *
     * @param s Timestamp string.
     * @return Timestamp.
     */
    public static long timestampStringToUnixDate(String s) {
        try {
            long d;
            long t;
            s = s.trim();
            int space = s.indexOf(' ');
            if (space >= 0) {
                String datePart = s.substring(0, space);
                validateDate(datePart, s);
                d = dateStringToUnixDate(datePart);

                String timePart = s.substring(space + 1);
                validateTime(timePart, s);
                t = timeStringToUnixDate(timePart, s);
            } else {
                validateDate(s, s);
                d = dateStringToUnixDate(s);
                t = 0;
            }
            return d * DateTimeUtils.MILLIS_PER_DAY + t;
        } catch (NumberFormatException e) {
            throw invalidType("TIMESTAMP", s, e);
        }
    }

    /** Returns a flag indicating whether the year field of a datetime literal value is less than 10000. */
    public static boolean isYearOutOfRange(String value) {
        int pos = value.indexOf('-');

        if (pos < 1) {
            return true;
        }

        try {
            String yearString = value.substring(0, pos);
            long year = Long.parseLong(yearString);

            return year > 9999;
        } catch (NumberFormatException ignore) {
            return true;
        }
    }

    private static void validateTime(String time, String full) {
        Matcher matcher = TIME_PATTERN.matcher(time);
        if (matcher.find()) {
            checkRange(matcher.group(1), 23, "HOUR", full);
            checkRange(matcher.group(2), 59, "MINUTE", full);
            checkRange(matcher.group(3), 59, "SECOND", full);
        } else {
            throw invalidType("TIME", full, null);
        }
    }

    private static void checkRange(String intValue, int maxVal, String fieldName, String full) {
        try {
            int value = Integer.parseInt(intValue);

            if (value > maxVal) {
                throw fieldOutOfRange(fieldName, full, null);
            }
        } catch (NumberFormatException e) {
            throw fieldOutOfRange(fieldName, full, e);
        }
    }

    private static void validateDate(String s, String full) {
        Matcher matcher = DATE_PATTERN.matcher(s);
        if (matcher.find()) {
            int year = Integer.parseInt(matcher.group(1));
            int month = Integer.parseInt(matcher.group(2));
            int day = Integer.parseInt(matcher.group(3));
            checkLegalDate(year, month, day, full);
        } else {
            throw invalidType("DATE", full, null);
        }
    }

    /** Check that the combination year, month, date forms a legal date. */
    private static void checkLegalDate(int year, int month, int day, String full) {
        if (day > daysInMonth(year, month)) {
            throw fieldOutOfRange("DAY", full, null);
        }
        if (month < 1 || month > 12) {
            throw fieldOutOfRange("MONTH", full, null);
        }
        if (year <= 0) {
            // Year 0 is not really a legal value.
            throw fieldOutOfRange("YEAR", full, null);
        }
    }

    /** Returns the number of days in a month in the proleptic Gregorian calendar
     * used by ISO-8601.
     *
     * <p>"Proleptic" means that we apply the calendar to dates before the
     * Gregorian calendar was invented (in 1582). Thus, years 0 and 1200 are
     * considered leap years, and 1500 is not. */
    private static int daysInMonth(int year, int month) {
        switch (month) {
            case 9:
            case 4:
            case 6:
            case 11:
                // Thirty days hath September,
                // April, June, and November,
                return 30;

            case 2:
                // Except February, twenty-eight days clear,
                // And twenty-nine in each leap year.
                return Year.isLeap(year) ? 29 : 28;

            default:
                // All the rest have thirty-one,
                return 31;
        }
    }

    private static IllegalArgumentException fieldOutOfRange(String field,
            String full, @Nullable Exception cause) {
        return new IllegalArgumentException("Value of " + field
                + " field is out of range in '" + full + "'", cause);
    }

    private static IllegalArgumentException invalidType(String type,
            String full, @Nullable Exception cause) {
        return new IllegalArgumentException("Invalid " + type + " value, '"
                + full + "'", cause);
    }
}
