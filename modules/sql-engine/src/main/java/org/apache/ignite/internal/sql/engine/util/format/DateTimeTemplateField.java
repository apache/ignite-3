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
    YYYY(FieldKind.YEAR, 4),
    YYY(FieldKind.YEAR, 3),
    YY(FieldKind.YEAR, 2),
    Y(FieldKind.YEAR, 1),
    RRRR(FieldKind.ROUNDED_YEAR, 4),
    RR(FieldKind.ROUNDED_YEAR, 2),
    MM(FieldKind.MONTH, 2),
    DD(FieldKind.DAY_OF_MONTH, 2),
    DDD(FieldKind.DAY_OF_YEAR, 3),
    HH(FieldKind.HOUR_12, 2),
    HH12(FieldKind.HOUR_12, 2),
    HH24(FieldKind.HOUR_24, 2),
    MI(FieldKind.MINUTE, 2),
    SS(FieldKind.SECOND_OF_MINUTE, 2),
    SSSSS(FieldKind.SECOND_OF_DAY, 5),
    FF1(FieldKind.FRACTION, 1),
    FF2(FieldKind.FRACTION, 2),
    FF3(FieldKind.FRACTION, 3),
    FF4(FieldKind.FRACTION, 4),
    FF5(FieldKind.FRACTION, 5),
    FF6(FieldKind.FRACTION, 6),
    FF7(FieldKind.FRACTION, 7),
    FF8(FieldKind.FRACTION, 8),
    FF9(FieldKind.FRACTION, 9),
    PM(FieldKind.AM_PM, "P.M."),
    AM(FieldKind.AM_PM, "A.M."),
    TZH(FieldKind.TIMEZONE, 2),
    TZM(FieldKind.TIMEZONE, 2)
    ;

    private final FieldKind kind;

    private final String pattern;

    private int maxDigits;

    DateTimeTemplateField(FieldKind kind, int maxDigits) {
        this.kind = kind;
        this.pattern = this.name();
        this.maxDigits = maxDigits;
    }

    DateTimeTemplateField(FieldKind kind, String pattern) {
        this.kind = kind;
        this.pattern = pattern;
        this.maxDigits = -1;
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
