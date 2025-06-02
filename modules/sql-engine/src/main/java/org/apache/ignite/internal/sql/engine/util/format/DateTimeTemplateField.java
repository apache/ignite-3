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
    YYYY,
    YYY,
    YY,
    Y,
    RRRR,
    RR,
    MM,
    DD,
    DDD,
    HH,
    HH12,
    HH24,
    MI,
    SS,
    SSSSS,
    FF1,
    FF2,
    FF3,
    FF4,
    FF5,
    FF6,
    FF7,
    FF8,
    FF9,
    PM,
    AM,
    TZH,
    TZM;

    String asPattern() {
        if (this == AM) {
            return "A.M.";
        } else if (this == PM) {
            return "P.M.";
        } else {
            return this.name();
        }
    }

    FieldKind kind() {
        switch (this) {
            case YYYY:
            case YYY:
            case YY:
            case Y:
                return FieldKind.YEAR;
            case RRRR:
            case RR:
                return FieldKind.ROUNDED_YEAR;
            case MM:
                return FieldKind.MONTH;
            case DD:
                return FieldKind.DAY_OF_MONTH;
            case DDD:
                return FieldKind.DAY_OF_YEAR;
            case HH:
            case HH12:
                return FieldKind.HOUR_12;
            case HH24:
                return FieldKind.HOUR_24;
            case MI:
                return FieldKind.MINUTE;
            case SS:
                return FieldKind.SECOND_OF_MINUTE;
            case SSSSS:
                return FieldKind.SECOND_OF_DAY;
            case FF1:
            case FF2:
            case FF3:
            case FF4:
            case FF5:
            case FF6:
            case FF7:
            case FF8:
            case FF9:
                return FieldKind.FRACTION;
            case PM:
            case AM:
                return FieldKind.AM_PM;
            case TZH:
            case TZM:
                return FieldKind.TIMEZONE;
            default:
                throw new IllegalStateException("Unexpected template field: " + this);
        }
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
}
