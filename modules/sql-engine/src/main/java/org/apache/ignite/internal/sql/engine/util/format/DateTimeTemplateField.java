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

/**
 * Supported datetime template fields.
 */
enum DateTimeTemplateField {
    YYYY("Year", FieldKind.YEAR, 4),
    YYY("Year", FieldKind.YEAR, 3),
    YY("Year", FieldKind.YEAR, 2),
    Y("Year", FieldKind.YEAR, 1),
    RRRR("Year", FieldKind.ROUNDED_YEAR, 4),
    RR("Year", FieldKind.ROUNDED_YEAR, 2),
    MM("MonthOfYear", FieldKind.MONTH, 2),
    DD("DayOfMonth", FieldKind.DAY_OF_MONTH, 2),
    DDD("DayOfYear", FieldKind.DAY_OF_YEAR, 3),
    HH("HourAmPm", FieldKind.HOUR_12, 2),
    HH12("HourAmPm", FieldKind.HOUR_12, 2),
    HH24("HourOfDay", FieldKind.HOUR_24, 2),
    MI("MinuteOfHour", FieldKind.MINUTE, 2),
    SS("SecondOfMinute", FieldKind.SECOND_OF_MINUTE, 2),
    SSSSS("SecondOfDay", FieldKind.SECOND_OF_DAY, 5),
    FF1("Fraction", FieldKind.FRACTION, 1),
    FF2("Fraction", FieldKind.FRACTION, 2),
    FF3("Fraction", FieldKind.FRACTION, 3),
    FF4("Fraction", FieldKind.FRACTION, 4),
    FF5("Fraction", FieldKind.FRACTION, 5),
    FF6("Fraction", FieldKind.FRACTION, 6),
    FF7("Fraction", FieldKind.FRACTION, 7),
    FF8("Fraction", FieldKind.FRACTION, 8),
    FF9("Fraction", FieldKind.FRACTION, 9),
    PM("Pm", FieldKind.AM_PM, "P.M."),
    AM("Am", FieldKind.AM_PM, "A.M."),
    TZH("TimeZone Hour", FieldKind.TIMEZONE, 2),
    TZM("TimeZone MinuteOfHour", FieldKind.TIMEZONE, 2);

    private final String displayName;

    private final FieldKind kind;

    private final String pattern;

    private final int maxDigits;

    DateTimeTemplateField(String displayName, FieldKind kind, int maxDigits) {
        this.displayName = displayName;
        this.kind = kind;
        this.pattern = this.name();
        this.maxDigits = maxDigits;
    }

    DateTimeTemplateField(String displayName, FieldKind kind, String pattern) {
        this.displayName = displayName;
        this.kind = kind;
        this.pattern = pattern;
        this.maxDigits = -1;
    }

    String displayName() {
        return displayName;
    }

    String pattern() {
        return pattern;
    }

    FieldKind kind() {
        return kind;
    }

    enum FieldKind {
        YEAR,
        ROUNDED_YEAR,
        MONTH,
        DAY_OF_MONTH,
        DAY_OF_YEAR,
        HOUR_12,
        HOUR_24,
        MINUTE,
        SECOND_OF_MINUTE,
        SECOND_OF_DAY,
        FRACTION,
        AM_PM,
        TIMEZONE;
    }

    int maxDigits() {
        if (maxDigits == -1) {
            throw new IllegalStateException(this + " has no digits");
        }
        return maxDigits;
    }
}
