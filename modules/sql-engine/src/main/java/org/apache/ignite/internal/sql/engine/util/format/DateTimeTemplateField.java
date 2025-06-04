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
    YYYY(FieldKind.YEAR),
    YYY(FieldKind.YEAR),
    YY(FieldKind.YEAR),
    Y(FieldKind.YEAR),
    RRRR(FieldKind.ROUNDED_YEAR),
    RR(FieldKind.ROUNDED_YEAR),
    MM(FieldKind.MONTH),
    DD(FieldKind.DAY_OF_MONTH),
    DDD(FieldKind.DAY_OF_YEAR),
    HH(FieldKind.HOUR_12),
    HH12(FieldKind.HOUR_12),
    HH24(FieldKind.HOUR_24),
    MI(FieldKind.MINUTE),
    SS(FieldKind.SECOND_OF_MINUTE),
    SSSSS(FieldKind.SECOND_OF_DAY),
    FF1(FieldKind.FRACTION),
    FF2(FieldKind.FRACTION),
    FF3(FieldKind.FRACTION),
    FF4(FieldKind.FRACTION),
    FF5(FieldKind.FRACTION),
    FF6(FieldKind.FRACTION),
    FF7(FieldKind.FRACTION),
    FF8(FieldKind.FRACTION),
    FF9(FieldKind.FRACTION),
    PM(FieldKind.AM_PM, "P.M."),
    AM(FieldKind.AM_PM, "A.M."),
    TZH(FieldKind.TIMEZONE),
    TZM(FieldKind.TIMEZONE)
    ;

    private final FieldKind kind;

    private final String pattern;

    DateTimeTemplateField(FieldKind kind) {
        this.kind = kind;
        this.pattern = this.name();
    }

    DateTimeTemplateField(FieldKind kind, String pattern) {
        this.kind = kind;
        this.pattern = pattern;
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
}
