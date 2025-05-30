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
import java.time.Year;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.Map;
import org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.DateTimeField;
import org.jetbrains.annotations.Nullable;

/**
 * Parsed fields.
 */
final class ParsedFields {

    private final Clock clock;

    private final Map<DateTimeField, Object> fields = new EnumMap<>(DateTimeField.class);

    private TimeZoneFields tz;

    private int sign;

    ParsedFields(Clock clock) {
        this.clock = clock;
    }

    Map<DateTimeField, Object> fields() {
        if (tz != null) {
            fields.put(DateTimeField.TIMEZONE, zoneOffset());
        }

        return fields;
    }

    private @Nullable ZoneOffset zoneOffset() {
        if (tz != null) {
            return tz.toZoneOffset();
        } else {
            return null;
        }
    }

    boolean hasSign() {
        return sign != 0;
    }

    void setSign(int sign) {
        this.sign = sign;
    }

    void addField(DateTimeTemplateField field, StringBuilder buf) {
        String value = buf.toString();
        buf.setLength(0);

        int signVal = sign;
        // Reset sign
        this.sign = 0;

        switch (field) {
            case YYYY:
                parseYear(field, value, 1, 9999, 0);
                break;
            case YYY:
                int m = Year.now(clock).getValue() / 1000 * 1000;
                parseYear(field, value, 0, 999, m);
                break;
            case YY:
                int m2 = Year.now(clock).getValue() / 100 * 100;
                parseYear(field, value, 0, 99, m2);
                break;
            case Y:
                int m3 = Year.now(clock).getValue() / 10 * 10;
                parseYear(field, value, 0, 9, m3);
                break;
            case MM:
                parseChronoField(field, value, 1, 12);
                break;
            case DD:
                parseChronoField(field, value, 1, 31);
                break;
            case DDD:
                parseChronoField(field, value, 1, 365);
                break;
            case HH:
            case HH12:
                parseChronoField(field, value, 0, 12);
                break;
            case HH24:
                parseChronoField(field, value, 0, 23);
                break;
            case MI:
                parseChronoField(field, value, 0, 59);
                break;
            case SS:
                parseChronoField(field, value, 0, 59);
                break;
            case SSSSS:
                parseChronoField(field, value, 0, 24 * 60 * 60);
                break;
            case RRRR:
                parseRoundedYear(field, value, 0, 9999);
                break;
            case RR:
                parseRoundedYear(field, value, 0, 99);
                break;
            case FF1:
            case FF3:
            case FF2:
                parseFaction(field, value, 3, 0, 999, 1_000_000);
                break;
            case FF4:
            case FF5:
            case FF6:
                parseFaction(field, value, 6, 0, 999_999, 1_000);
                break;
            case FF7:
            case FF8:
            case FF9:
                parseFaction(field, value, 9, 0, 999_999_999, 1);
                break;
            case PM:
            case AM:
                parseAmPm(value);
                break;
            case TZH:
                parseTimeZone("time zone hours", TimeZoneFields.HOURS, value, signVal, 0, 23);
                break;
            case TZM:
                parseTimeZone("time zone minutes", TimeZoneFields.MINUTES, value, 0, 0, 59);
                break;
            default:
                throw new IllegalStateException("Unexpected field: " + field);
        }
    }

    private void parseYear(DateTimeTemplateField field, String value, int min, int max, int base) {
        int v = parseInt(field.displayName(), value, min, max) + base;
        fields.put(field.field(), v);
    }

    private void parseRoundedYear(DateTimeTemplateField field, String value, int min, int max) {
        int v = parseInt(field.displayName(), value, min, max);

        if (v < 100) {
            if (v <= 49) {
                v = 2000 + v;
            } else {
                v = 1900 + v;
            }
        }

        fields.put(field.field(), v);
    }

    private void parseFaction(DateTimeTemplateField field, String value, int len, int min, int max, int multiplier) {
        if (value.length() < len) {
            value = value + "0".repeat(len - value.length());
        }
        int v = parseInt(field.displayName(), value, min, max) * multiplier;
        fields.put(field.field(), v);
    }

    private void parseChronoField(DateTimeTemplateField field, String value, int min, int max) {
        int v = parseInt(field.displayName(), value, min, max);
        fields.put(field.field(), v);
    }

    private static void parseAmPm(String value) {
        if (!"A.M.".equals(value) && !"P.M.".equals(value)) {
            throw new DateTimeFormatException("Expected A.M./P.M. but got " + value);
        }
    }

    private void parseTimeZone(String fieldName, int f, String value, int sign, int min, int max) {
        if (tz == null) {
            tz = new TimeZoneFields();
        }
        int num = parseInt(fieldName, value, min, max);
        tz.setField(f, num, sign);
    }

    private static int parseInt(String field, String text, int min, int max) {
        int num;
        try {
            num = Integer.parseInt(text);
        } catch (NumberFormatException ignore) {
            throw new DateTimeFormatException("Field value is out of range " + field);
        }

        if (num < min || num > max) {
            throw new DateTimeFormatException("Field value is out of range " + field);
        }
        return num;
    }

    private static class TimeZoneFields {
        private static final int HOURS = 1;
        private static final int MINUTES = 2;
        private int hours;
        private int minutes;
        private int parsed;
        private int sign = 1;

        void setField(int field, int value, int fieldSign) {
            if (field == HOURS) {
                sign = fieldSign;
                hours = value;
            } else if (field == MINUTES) {
                if (fieldSign != 0) {
                    throw new IllegalStateException("Field does not support sign character");
                }
                minutes = value;
            } else {
                throw new DateTimeFormatException("Unexpected time zone field");
            }
            parsed |= field;
        }

        @Nullable ZoneOffset toZoneOffset() {
            if (parsed == 0) {
                throw new IllegalStateException("Time zone has not been parsed");
            }
            try {
                return ZoneOffset.ofHoursMinutes(sign * hours, sign * minutes);
            } catch (Exception e) {
                throw  new DateTimeFormatException("Invalid value for field time zone", e);
            }
        }
    }
}
