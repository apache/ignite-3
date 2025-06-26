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
import java.time.Year;
import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.jetbrains.annotations.Nullable;

/**
 * Parses text according to format elements.
 */
final class Parser {

    private static final char EOF = '\0';

    private final List<DateTimeFormatElement> elements;

    private final StringBuilder buf = new StringBuilder();

    private final Clock clock;

    private int current;

    private int elementIndex;

    private String text;

    private ParsedFields parsedFields;

    private @Nullable Hours12 hh12;

    private @Nullable TimeZoneFields tz;

    Parser(List<DateTimeFormatElement> elements) {
        this(elements, Clock.systemDefaultZone());
    }

    Parser(List<DateTimeFormatElement> elements, Clock clock) {
        if (elements.isEmpty()) {
            throw new IllegalArgumentException("Element must not be empty");
        }
        Objects.requireNonNull(clock, "clock");

        this.elements = elements;
        this.clock = clock;
    }

    ParsedFields parse(String text) {
        Objects.requireNonNull(text, "text");

        current = 0;
        buf.setLength(0);
        elementIndex = 0;
        this.text = text;

        parsedFields = new ParsedFields();

        parseElements(text);

        if (hh12 != null) {
            assert hh12.clock != 0;
            parsedFields.addHours12(hh12.clock == Hours12.PM, hh12.value);
        }

        if (tz != null) {
            assert tz.parsed != 0;
            parsedFields.addTz(tz.hours * tz.sign, tz.minutes * tz.sign);
        }

        return parsedFields;
    }

    private void parseElements(String text) {
        for (DateTimeFormatElement element : elements) {
            if (atEnd()) {
                break;
            }

            switch (element.kind) {
                case DELIMITER:
                    delimiter(element);
                    break;
                case FIELD:
                    field(element);
                    break;
                default:
                    throw new IllegalStateException("Unexpected element kind: " + element.kind);
            }
        }

        if (elementIndex < elements.size()) {
            String elems = elements.subList(elementIndex, elements.size()).stream()
                    .map(DateTimeFormatElement::toString)
                    .collect(Collectors.joining(", "));

            throw parseError("No values for elements {}", elems);
        }

        if (current < text.length()) {
            DateTimeFormatElement e = elements.get(elementIndex - 1);
            throw parseError("Unexpected trailing characters after {}", e);
        }
    }

    private boolean atEnd() {
        return current >= text.length();
    }

    private void delimiter(DateTimeFormatElement element) {
        // Value is always set for a delimiter
        assert element.delimiter != EOF;

        char c = currentChar();
        if (element.delimiter != c) {
            throw parseError("Invalid format. Expected literal <{}> but got <{}>", element.delimiter, c);
        }

        advancePosition();
        elementIndex += 1;
    }

    private void field(DateTimeFormatElement element) {
        DateTimeTemplateField field = element.template;
        // Field must be present at this point.
        assert field != null;

        boolean matches;
        switch (field) {
            case AM:
            case PM:
                // Both AM and PM accept either A.M or P.M.
                matches = matchHh12(() -> matchChars("A.M.") || matchChars("P.M."));
                break;
            case HH:
            case HH12:
                matches = matchHh12(() -> matchAtMostDigits(field.maxDigits()));
                break;
            case TZH:
                matches = matchTzField(this::tzh);
                break;
            case TZM:
                matches = matchTzField(() -> matchAtMostDigits(field.maxDigits()));
                break;
            default:
                matches = matchAtMostDigits(field.maxDigits());
                break;
        }

        if (matches) {
            parseFieldValue(field);
            elementIndex += 1;
        } else {
            throw parseError("Expected field {} but got <{}>", field, currentChar());
        }
    }

    private boolean matchTzField(BooleanSupplier matcher) {
        // Initialize tz field if it is not present.
        if (tz == null) {
            tz = new TimeZoneFields();
        }

        boolean matches = matcher.getAsBoolean();
        if (!matches) {
            // Reset on error.
            tz = null;
            return false;
        } else {
            return true;
        }
    }

    private boolean matchHh12(BooleanSupplier matcher) {
        if (hh12 == null) {
            hh12 = new Hours12();
        }

        boolean matches = matcher.getAsBoolean();
        if (!matches) {
            // Reset on error.
            hh12 = null;
            return false;
        } else {
            return true;
        }
    }

    private boolean tzh() {
        // Time zone hour: +/- followed by 1-2 Digit(s)

        int start = current;

        // TZM may precede TZH, so tz can be null.
        if (tz == null) {
            tz = new TimeZoneFields();
        }

        if (matchSign()) {
            tz.sign = buf.charAt(0) == '+' ? 1 : -1;
        } else {
            return false;
        }

        // Reset the buffer because we parse a sign character and digits separately.
        buf.setLength(0);

        if (!matchAtMostDigits(DateTimeTemplateField.TZH.maxDigits())) {
            // Return to the position prior to parsing for error reporting.
            current = start;
            return false;
        } else {
            return true;
        }
    }

    private boolean matchSign() {
        char c = currentChar();

        if (c == '+' || c == '-') {
            addChar();
            advancePosition();
            return true;
        } else {
            return false;
        }
    }

    private boolean matchChars(String chars) {
        // Expects next chars.length() characters from an input to match exactly.
        int start = current;

        for (int i = 0; i < chars.length(); i++) {
            char c = currentChar();
            char p = chars.charAt(i);
            if (Character.toUpperCase(c) != Character.toUpperCase(p)) {
                // Does not match, reset position and clear the buffer.
                current = start;
                buf.setLength(0);
                return false;
            }
            addChar();
            advancePosition();
        }

        return true;
    }

    private boolean matchAtMostDigits(int n) {
        // Expected at most n digits, but not least than 1.

        int numDigits = 0;
        while (numDigits < n) {
            char c = currentChar();
            if (!Character.isDigit(c)) {
                break;
            }
            numDigits += 1;
            addChar();
            advancePosition();
        }

        // Succeeds if we parsed at least one digit
        return numDigits > 0;
    }

    private char currentChar() {
        if (atEnd()) {
            return EOF;
        }
        return text.charAt(current);
    }

    private void advancePosition() {
        current++;
    }

    private void addChar() {
        char c = currentChar();
        assert c != EOF : "Should never read EOF";

        buf.append(c);
    }

    private void parseFieldValue(DateTimeTemplateField field) {
        // This should never happen - this method must not be called when the buffer has some data
        assert buf.length() > 0 : "Field value is empty";

        String value = buf.toString();
        buf.setLength(0);

        switch (field) {
            case YYYY:
                parseYear(field, value, 0);
                break;
            case YYY:
                int baseYyy = Year.now(clock).getValue() / 1000 * 1000;
                parseYear(field, value, baseYyy);
                break;
            case YY:
                int baseYy = Year.now(clock).getValue() / 100 * 100;
                parseYear(field, value, baseYy);
                break;
            case Y:
                int baseY = Year.now(clock).getValue() / 10 * 10;
                parseYear(field, value, baseY);
                break;
            case MM:
                parseNumber(field, value);
                break;
            case DD:
                parseNumber(field, value);
                break;
            case DDD:
                parseNumber(field, value);
                break;
            case HH:
            case HH12:
                parse12Hour(field, value);
                break;
            case HH24:
                parseNumber(field, value);
                break;
            case MI:
                parseNumber(field, value);
                break;
            case SS:
                parseNumber(field, value);
                break;
            case SSSSS:
                parseNumber(field, value);
                break;
            case RRRR:
            case RR:
                parseRoundedYear(field, value);
                break;
            case FF1:
            case FF3:
            case FF2:
                parseFaction(field, value, 3, 1_000_000);
                break;
            case FF4:
            case FF5:
            case FF6:
                parseFaction(field, value, 6, 1_000);
                break;
            case FF7:
            case FF8:
            case FF9:
                parseFaction(field, value, 9, 1);
                break;
            case PM:
            case AM:
                parseAmPm(value);
                break;
            case TZH:
                parseTimeZone(field, TimeZoneFields.HOURS, value);
                break;
            case TZM:
                parseTimeZone(field, TimeZoneFields.MINUTES, value);
                break;
            default:
                throw new IllegalStateException("Unexpected field: " + field);
        }
    }

    private void parseYear(DateTimeTemplateField field, String value, int base) {
        int v = parseInt(field, value);
        parsedFields.addYear(base, v);
    }

    private void parseRoundedYear(DateTimeTemplateField field, String value) {
        int v = parseInt(field, value);
        parsedFields.addRoundedYear(v);
    }

    private void parseFaction(DateTimeTemplateField field, String value, int len, int multiplier) {
        if (value.length() < len) {
            value = value + "0".repeat(len - value.length());
        }
        int v = parseInt(field, value) * multiplier;
        parsedFields.add(field.kind(), v);
    }

    private void parseNumber(DateTimeTemplateField field, String value) {
        int v = parseInt(field, value);
        parsedFields.add(field.kind(), v);
    }

    private void parse12Hour(DateTimeTemplateField field, String value) {
        int v = parseInt(field, value);

        assert hh12 != null : "hh12 should have been initialized";
        hh12.value = v;
    }

    private void parseAmPm(String value) {
        assert hh12 != null : "hh12 should have been initialized";

        // A.M. accepts both A.M. and P.M. and vice versa.
        hh12.setFlag("P.M.".equalsIgnoreCase(value));
    }

    private void parseTimeZone(DateTimeTemplateField field, int f, String value) {
        int v = parseInt(field, value);
        assert tz != null : "tz should have been initialized";
        tz.setField(f, v);
    }

    private static int parseInt(DateTimeTemplateField field, String text) {
        int num;
        try {
            num = Integer.parseInt(text);
        } catch (NumberFormatException ignore) {
            throw parseError("Invalid value for {}", field.displayName());
        }
        return num;
    }

    private static class Hours12 {
        private static final int AM = 1;
        private static final int PM = 2;
        private int clock;
        private int value = -1;

        void setFlag(boolean pm) {
            clock = pm ? PM : AM;
        }
    }

    private static class TimeZoneFields {
        private static final int HOURS = 1;
        private static final int MINUTES = 2;
        private int hours;
        private int minutes;
        private int parsed;
        private int sign;

        void setField(int field, int value) {
            if (field == HOURS) {
                assert sign != 0 : "No sign character";
                hours = value;
            } else if (field == MINUTES) {
                minutes = value;
            } else {
                throw parseError("Unexpected time zone field");
            }
            parsed |= field;
        }
    }

    private static DateTimeException parseError(String message, Object... elements) {
        return new DateTimeException(format(message, elements));
    }
}
