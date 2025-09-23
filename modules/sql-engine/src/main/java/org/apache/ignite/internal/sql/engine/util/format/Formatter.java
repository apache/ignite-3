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

import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Format values according to format elements.
 */
final class Formatter {

    private final StringBuilder out = new StringBuilder();

    private final List<DateTimeFormatElement> elements;

    Formatter(List<DateTimeFormatElement> elements) {
        this.elements = elements;
    }

    String format(TemporalAccessor value, ZoneOffset zoneOffset) {
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(zoneOffset, "zoneOffset");

        out.setLength(0);

        appendElements(value, zoneOffset);

        return out.toString();
    }

    private void appendElements(TemporalAccessor value, @Nullable ZoneOffset zoneOffset) {
        for (DateTimeFormatElement element : elements) {
            switch (element.kind) {
                case DELIMITER:
                    out.append(element.delimiter);
                    break;
                case FIELD:
                    assert element.template != null;
                    appendField(value, element.template, zoneOffset);
                    break;
                default:
                    throw new IllegalStateException("Unexpected element kind: " + element.kind);
            }
        }
    }

    private void appendField(TemporalAccessor value, DateTimeTemplateField field, @Nullable ZoneOffset zoneOffset) {
        switch (field) {
            case YYYY:
                appendField(value, ChronoField.YEAR, 4);
                break;
            case YYY:
                appendField(value, ChronoField.YEAR, 3);
                break;
            case YY:
                appendField(value, ChronoField.YEAR, 2);
                break;
            case Y:
                appendField(value, ChronoField.YEAR, 1);
                break;
            case RRRR:
                appendField(value, ChronoField.YEAR, 4);
                break;
            case RR:
                appendField(value, ChronoField.YEAR, 2);
                break;
            case MM:
                appendField(value, ChronoField.MONTH_OF_YEAR, 2);
                break;
            case DD:
                appendField(value, ChronoField.DAY_OF_MONTH, 2);
                break;
            case DDD:
                appendField(value, ChronoField.DAY_OF_YEAR, 3);
                break;
            case HH:
            case HH12: {
                int hours = value.get(ChronoField.HOUR_OF_DAY);
                if (hours >= 13) {
                    hours -= 12;
                } else if (hours == 0) {
                    hours = 12;
                } else if (hours != 12) {
                    hours %= 12;
                }
                appendField(hours, 2);
                break;
            }
            case HH24:
                appendField(value, ChronoField.HOUR_OF_DAY, 2);
                break;
            case MI:
                appendField(value, ChronoField.MINUTE_OF_HOUR, 2);
                break;
            case SS:
                appendField(value, ChronoField.SECOND_OF_MINUTE, 2);
                break;
            case SSSSS:
                appendField(value, ChronoField.SECOND_OF_DAY, 5);
                break;
            case FF1:
                appendField(value.get(ChronoField.NANO_OF_SECOND) / 100_000_000, 1);
                break;
            case FF2:
                appendField(value.get(ChronoField.NANO_OF_SECOND) / 10_000_000, 2);
                break;
            case FF3:
                appendField(value.get(ChronoField.NANO_OF_SECOND) / 1_000_000, 3);
                break;
            case FF4:
                appendField(value.get(ChronoField.NANO_OF_SECOND) / 100_000, 4);
                break;
            case FF5:
                appendField(value.get(ChronoField.NANO_OF_SECOND) / 10_000, 5);
                break;
            case FF6:
                appendField(value.get(ChronoField.NANO_OF_SECOND) / 1000, 6);
                break;
            case FF7:
                appendField(value.get(ChronoField.NANO_OF_SECOND) / 100, 7);
                break;
            case FF8:
                appendField(value.get(ChronoField.NANO_OF_SECOND) / 10, 8);
                break;
            case FF9:
                appendField(value.get(ChronoField.NANO_OF_SECOND), 9);
                break;
            case PM:
            case AM: {
                int hours = value.get(ChronoField.HOUR_OF_DAY);
                if (hours <= 11) {
                    out.append("A.M.");
                } else {
                    out.append("P.M.");
                }
                break;
            }
            case TZH: {
                assert zoneOffset != null;
                if (zoneOffset.getTotalSeconds() < 0) {
                    out.append('-');
                } else {
                    out.append('+');
                }
                int tzSeconds = Math.abs(zoneOffset.get(ChronoField.OFFSET_SECONDS));
                int tzHours = tzSeconds / (60 * 60);
                appendField(tzHours, 2);
                break;
            }
            case TZM: {
                assert zoneOffset != null;
                int tzSeconds = Math.abs(zoneOffset.get(ChronoField.OFFSET_SECONDS));
                int tzHours = tzSeconds / (60 * 60);
                int tzMinutes = (tzSeconds - tzHours * 60 * 60) / 60;
                appendField(tzMinutes, 2);
                break;
            }
            default:
                throw new IllegalStateException("Unexpected field: " + field);
        }
    }

    private void appendField(TemporalAccessor data, ChronoField field, int maxDigits) {
        long scaled = data.get(field);

        appendField(scaled, maxDigits);
    }

    private void appendField(long val, int maxDigits) {
        assert val >= 0;

        for (int i = 0; i < maxDigits; i++) {
            out.append('0');
        }

        int pos = maxDigits - 1;
        int start = out.length() - maxDigits;
        long scaled = val;

        do {
            int digit = (int) (scaled % 10);
            char c = (char) ('0' + digit);
            out.setCharAt(start + pos, c);
            scaled /= 10;
            pos--;
        } while (scaled != 0 && pos >= 0);
    }
}
