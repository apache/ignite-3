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
    YYYY(DateTimeField.YEAR),
    YYY(DateTimeField.YEAR),
    YY(DateTimeField.YEAR),
    Y(DateTimeField.YEAR),

    RRRR(DateTimeField.ROUNDED_YEAR),
    RR(DateTimeField.ROUNDED_YEAR),

    MM(DateTimeField.MONTH),

    DD(DateTimeField.DAY_OF_MONTH),
    DDD(DateTimeField.DAY_OF_YEAR),

    HH(DateTimeField.HOUR_12),
    HH12(DateTimeField.HOUR_12),
    HH24(DateTimeField.HOUR_24),

    MI(DateTimeField.MINUTE),
    SS(DateTimeField.SECOND_OF_MINUTE),
    SSSSS(DateTimeField.SECOND_OF_DAY),

    FF1(DateTimeField.FRACTION),
    FF2(DateTimeField.FRACTION),
    FF3(DateTimeField.FRACTION),
    FF4(DateTimeField.FRACTION),
    FF5(DateTimeField.FRACTION),
    FF6(DateTimeField.FRACTION),
    FF7(DateTimeField.FRACTION),
    FF8(DateTimeField.FRACTION),
    FF9(DateTimeField.FRACTION),

    PM(DateTimeField.AM_PM),
    AM(DateTimeField.AM_PM),

    TZH(DateTimeField.TIMEZONE),
    TZM(DateTimeField.TIMEZONE);

    private final DateTimeField kind;

    DateTimeTemplateField(DateTimeField kind) {
        this.kind = kind;
    }

    String asPattern() {
        if (this == AM) {
            return "A.M.";
        } else if (this == PM) {
            return "P.M.";
        } else {
            return this.name();
        }
    }

    int maxLength() {
        switch (this) {
            case YYYY:
                return 4;
            case YYY:
                return 3;
            case YY:
                return 2;
            case Y:
                return 1;
            case RRRR:
                return 4;
            case RR:
                return 2;
            case MM:
                return 2;
            case DD:
                return 2;
            case DDD:
                return 3;
            case HH:
                return 2;
            case HH12:
                return 2;
            case HH24:
                return 2;
            case MI:
                return 2;
            case SS:
                return 2;
            case SSSSS:
                return 5;
            case FF1:
                return 1;
            case FF2:
                return 2;
            case FF3:
                return 3;
            case FF4:
                return 4;
            case FF5:
                return 5;
            case FF6:
                return 6;
            case FF7:
                return 7;
            case FF8:
                return 8;
            case FF9:
                return 9;
            case PM:
            case AM:
                return 4;
            case TZH:
                return 2;
            case TZM:
                return 2;
            default:
                throw new IllegalArgumentException();
        }
    }

    String displayName() {
        switch (this) {
            case YYYY:
            case YYY:
            case YY:
            case Y:
                return "year";
            case RRRR:
            case RR:
                return "rounded year";
            case MM:
                return "month";
            case DD:
                return "day of month";
            case DDD:
                return "day of year";
            case HH:
            case HH12:
                return "12-hour";
            case HH24:
                return "24-hour";
            case MI:
                return "minutes";
            case SS:
                return "second of minute";
            case SSSSS:
                return "second of day";
            case FF1:
                return "fraction(1)";
            case FF2:
                return "fraction(2)";
            case FF3:
                return "fraction(3)";
            case FF4:
                return "fraction(4)";
            case FF5:
                return "fraction(5)";
            case FF6:
                return "fraction(6)";
            case FF7:
                return "fraction(7)";
            case FF8:
                return "fraction(8)";
            case FF9:
                return "fraction(9)";
            case PM:
                return "p.m.";
            case AM:
                return "a.m.";
            case TZH:
                return "time zone hours";
            case TZM:
                return "time zone minutes";
            default:
                throw new IllegalStateException();
        }
    }

    DateTimeField field() {
        return kind;
    }

    enum DateTimeField {
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
}
