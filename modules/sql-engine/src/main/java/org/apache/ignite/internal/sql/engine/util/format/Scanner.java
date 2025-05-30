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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.ignite.internal.sql.engine.util.format.DateTimeFormatElement.ElementKind;
import org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.DateTimeField;

/**
 * Converts a format string into a sequence of elements (delimiters or fields).
 */
final class Scanner {
    private static final Map<String, DateTimeTemplateField> FIELDS_BY_PATTERN;

    private final String source;

    /** Encountered elements. */
    private final List<DateTimeFormatElement> elements = new ArrayList<>();

    /** A set of encountered fields. Each field can be present at most once. */
    private final EnumSet<DateTimeTemplateField> fields = EnumSet.noneOf(DateTimeTemplateField.class);

    /** A set of encountered fields kind. */
    private final EnumSet<DateTimeField> groups = EnumSet.noneOf(DateTimeField.class);

    private int start;

    private int current;

    static {
        FIELDS_BY_PATTERN = new HashMap<>();
        for (var field : DateTimeTemplateField.values()) {
            FIELDS_BY_PATTERN.put(field.asPattern(), field);
        }
    }

    Scanner(String source) {
        // the SQL date time format is case-insensitive.
        this.source = source.toUpperCase(Locale.US);
    }

    List<DateTimeFormatElement> parse() {
        while (!atEnd()) {
            start = current;
            scanToken();
        }

        validateFields();

        return elements;
    }

    private boolean atEnd() {
        return current >= source.length();
    }

    private void scanToken() {
        char c = advance();
        switch (c) {
            case '-':
            case '.':
            case '/':
            case ',':
            case '\'':
            case ';':
            case ':':
            case ' ':
                addToken(ElementKind.DELIMITER);
                break;
            default:
                if (isAlpha(c)) {
                    field();
                } else {
                    throw new DateTimeFormatException("Invalid format: " + source);
                }
        }
    }

    private void field() {
        if (match("YYYY") || match("YYY") || match("YY") || match("Y")) {
            addToken(ElementKind.FIELD);
        } else if (match("RRRR") || match("RR")) {
            addToken(ElementKind.FIELD);
        } else if (match("MM")) {
            addToken(ElementKind.FIELD);
        } else if (match("DDD") || match("DD")) {
            addToken(ElementKind.FIELD);
        } else if (match("HH24") || match("HH12") || match("HH")) {
            addToken(ElementKind.FIELD);
        } else if (match("MI")) {
            addToken(ElementKind.FIELD);
        } else if (match("SSSSS") || match("SS")) {
            addToken(ElementKind.FIELD);
        } else if (match("FF1") || match("FF2") || match("FF3")
                || match("FF4") || match("FF5") || match("FF6")
                || match("FF7") || match("FF8") || match("FF9")) {
            addToken(ElementKind.FIELD);
        } else if (match("A.M.") || match("P.M.")) {
            addToken(ElementKind.FIELD);
        } else if (match("TZH") || match("TZM")) {
            addToken(ElementKind.FIELD);
        } else {
            // Consume unexpected field
            while (isAlphaNumeric(peek())) {
                advance();
            }
            throw new DateTimeFormatException("Invalid format: " + source);
        }
    }

    private boolean match(String text) {
        // Match the first char
        if (source.charAt(start) != text.charAt(0)) {
            return false;
        }

        int pos = current;
        boolean matches = true;

        // Match the rest, if do not match then current position.
        for (int i = 1; i < text.length(); i++) {
            if (atEnd()) {
                matches = false;
                break;
            }
            char c = advance();
            if (c != text.charAt(i)) {
                matches = false;
                break;
            }
        }

        if (!matches) {
            current = pos;
            return false;
        } else {
            return true;
        }
    }

    private char advance() {
        current += 1;
        return source.charAt(current - 1);
    }

    private char peek() {
        if (atEnd()) {
            return '\0';
        } else {
            return source.charAt(current);
        }
    }

    private void addToken(ElementKind elementKind) {
        String token = source.substring(start, current);

        if (elementKind == ElementKind.FIELD) {
            addField(token);
        } else if (token.length() == 1) {
            addDelimiter(token);
        } else {
            throw new DateTimeFormatException("Unexpected token: " + token);
        }
    }

    private void addDelimiter(String token) {
        // Syntax rules 3) Ensure there are no consecutive delimiters.
        if (!elements.isEmpty()) {
            DateTimeFormatElement e = elements.get(elements.size() - 1);
            if (e.kind == ElementKind.DELIMITER) {
                throw new DateTimeFormatException("Consecutive delimiters are not allowed");
            }
        }

        elements.add(new DateTimeFormatElement(token.charAt(0)));
    }

    private void addField(String token) {
        DateTimeTemplateField field = FIELDS_BY_PATTERN.get(token);
        if (field == null) {
            throw new DateTimeFormatException("Unexpected field: " + token);
        }

        // Syntax rules 4) each field can be present at most once
        DateTimeField dtField = field.field();
        boolean alreadyPresent = !groups.add(dtField);

        if (dtField == DateTimeField.TIMEZONE) {
            alreadyPresent = !fields.add(field);
        }

        if (alreadyPresent) {
            throw new DateTimeFormatException("Element is already present: " + dtField);
        }

        elements.add(new DateTimeFormatElement(field));
    }

    private static boolean isAlpha(char c) {
        return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z';
    }

    private static boolean isAlphaNumeric(char c) {
        return isAlpha(c) || Character.isDigit(c);
    }

    private void ensureOnlyOnePresent(DateTimeField f1, DateTimeField f2, String message) {
        if (groups.contains(f1) && groups.contains(f2)) {
            throw new DateTimeFormatException("Invalid format. Only one field must be present: " + message);
        }
    }

    private void ensureBothPresent(DateTimeField f1, DateTimeField f2, String message) {
        if (groups.contains(f1) != groups.contains(f2)) {
            throw new DateTimeFormatException("Invalid format. Expected both fields: " + message);
        }
    }

    private void validateFields() {
        // Syntax rules 5) It shall not contain both <datetime template year> and <datetime template rounded year>,
        ensureOnlyOnePresent(DateTimeField.YEAR, DateTimeField.ROUNDED_YEAR, "year / rounded year");

        // Syntax rules 6) if contains <datetime template day of year>,
        // then it shall not contain <datetime template month> or <datetime template day of month>.
        ensureOnlyOnePresent(DateTimeField.DAY_OF_YEAR, DateTimeField.MONTH, "day of year / month");
        ensureOnlyOnePresent(DateTimeField.DAY_OF_YEAR, DateTimeField.DAY_OF_MONTH, "day of year / day of month");

        // Syntax rules 7)
        ensureOnlyOnePresent(DateTimeField.HOUR_24, DateTimeField.HOUR_12, "24-hour / 12-hour");

        // Syntax rules 8) if any of 12-hour and am pm must present, then both of them must present
        ensureBothPresent(DateTimeField.HOUR_12, DateTimeField.AM_PM, "12-hour / am pm");

        // Syntax rules 9) 24-hour and am pm
        ensureOnlyOnePresent(DateTimeField.HOUR_24, DateTimeField.AM_PM, "24-hour / am pm");

        // Syntax rules 10) second of day
        ensureOnlyOnePresent(DateTimeField.SECOND_OF_DAY, DateTimeField.HOUR_12, "second of day / 12-hour");
        ensureOnlyOnePresent(DateTimeField.SECOND_OF_DAY, DateTimeField.HOUR_24, "second of day / 24-hour");
        ensureOnlyOnePresent(DateTimeField.SECOND_OF_DAY, DateTimeField.MINUTE, "second of day / minute");
        ensureOnlyOnePresent(DateTimeField.SECOND_OF_DAY, DateTimeField.SECOND_OF_MINUTE, "second of day / second of minute");

        // Syntax rules 11) time zone hour and minute
        if (fields.contains(DateTimeTemplateField.TZH) != fields.contains(DateTimeTemplateField.TZM)) {
            throw new DateTimeFormatException("Invalid format. Expected both fields: time zone hours / time zone minutes");
        }
    }
}
