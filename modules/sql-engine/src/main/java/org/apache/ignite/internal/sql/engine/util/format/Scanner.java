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

import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.internal.sql.engine.util.format.DateTimeFormatElement.ElementKind;
import org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.FieldKind;

/**
 * Converts a format string into a sequence of elements (delimiters or fields).
 */
final class Scanner {
    private static final Map<String, DateTimeTemplateField> FIELDS_BY_PATTERN;

    static {
        FIELDS_BY_PATTERN = new HashMap<>();
        for (var field : DateTimeTemplateField.values()) {
            FIELDS_BY_PATTERN.put(field.pattern(), field);
        }
    }

    private final String pattern;

    /** Allowed fields. */
    private final Set<FieldKind> allowedFields;

    /** Format name for error reporting. */
    private final String formatName;

    /** Encountered elements. */
    private final List<DateTimeFormatElement> elements = new ArrayList<>();

    /** A set of encountered fields. Each field can be present at most once. */
    private final EnumSet<DateTimeTemplateField> fields = EnumSet.noneOf(DateTimeTemplateField.class);

    /** A set of encountered fields kind. */
    private final EnumSet<FieldKind> groups = EnumSet.noneOf(FieldKind.class);

    private int start;

    private int current;

    Scanner(String pattern) {
        this(pattern, "ALL_FIELDS", EnumSet.allOf(FieldKind.class));
    }

    Scanner(String pattern, String formatName, Set<FieldKind> allowedFields) {
        Objects.requireNonNull(pattern, "pattern");
        Objects.requireNonNull(formatName, "formatName");
        Objects.requireNonNull(allowedFields, "allowedFields");
        // the SQL date time format is case-insensitive.
        this.pattern = pattern.toUpperCase(Locale.US);
        this.formatName = formatName;
        this.allowedFields = allowedFields;
    }

    List<DateTimeFormatElement> scan() {
        while (!atEnd()) {
            start = current;
            scanToken();
        }

        validateFields();

        return elements;
    }

    private boolean atEnd() {
        return current >= pattern.length();
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
                addDelimiter(c);
                break;
            default:
                if (isAlpha(c)) {
                    addField(c);
                } else {
                    throw formatError("Unexpected character <{}> in pattern <{}>", c, pattern);
                }
        }
    }

    private void addField(char c) {
        switch (c) {
            case 'Y': {
                if (match("YYYY") || match("YYY") || match("YY") || match("Y")) {
                    doAddField();
                    return;
                }
                break;
            }
            case 'R': {
                if (match("RRRR") || match("RR")) {
                    doAddField();
                    return;
                }
                break;
            }
            case 'M': {
                if (match("MM") || match("MI")) {
                    doAddField();
                    return;
                }
                break;
            }
            case 'D': {
                if (match("DDD") || match("DD")) {
                    doAddField();
                    return;
                }
                break;
            }
            case 'H': {
                if (match("HH24") || match("HH12") || match("HH")) {
                    doAddField();
                    return;
                }
                break;
            }
            case 'S': {
                if (match("SSSSS") || match("SS")) {
                    doAddField();
                    return;
                }
                break;
            }
            case 'F': {
                if (match("FF1") || match("FF2") || match("FF3")
                        || match("FF4") || match("FF5") || match("FF6")
                        || match("FF7") || match("FF8") || match("FF9")) {
                    doAddField();
                    return;
                }
                break;
            }
            case 'T': {
                if (match("TZH") || match("TZM")) {
                    doAddField();
                    return;
                }
                break;
            }
            default:
                if (match("A.M.") || match("P.M.")) {
                    doAddField();
                    return;
                }
        }

        // Consume unexpected field
        while (isAlphaNumeric(peek())) {
            advance();
        }

        throw formatError("Unexpected element <{}> in pattern <{}>", pattern.substring(start, current), pattern);
    }

    private boolean match(String text) {
        // Match the first char
        if (pattern.charAt(start) != text.charAt(0)) {
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
        return pattern.charAt(current - 1);
    }

    private char peek() {
        if (atEnd()) {
            return '\0';
        } else {
            return pattern.charAt(current);
        }
    }

    private void addDelimiter(char c) {
        // Syntax rules 3) Ensure there are no consecutive delimiters.
        if (!elements.isEmpty()) {
            DateTimeFormatElement e = elements.get(elements.size() - 1);
            if (e.kind == ElementKind.DELIMITER) {
                throw formatError("Consecutive delimiters are not allowed");
            }
        }

        elements.add(new DateTimeFormatElement(c));
    }

    private void doAddField() {
        String token = pattern.substring(start, current);

        DateTimeTemplateField field = FIELDS_BY_PATTERN.get(token);
        if (field == null) {
            throw formatError("Unexpected field <{}>", token);
        }

        // Syntax rules 4) each field can be present at most once
        FieldKind dtField = field.kind();
        boolean alreadyPresent = !groups.add(dtField);

        if (!allowedFields.contains(dtField)) {
            throw formatError("Illegal field <{}> for format {}", field, formatName);
        }

        if (dtField == FieldKind.TIMEZONE) {
            alreadyPresent = !fields.add(field);
        }

        if (alreadyPresent) {
            throw formatError("Element is already present: {}", dtField);
        }

        elements.add(new DateTimeFormatElement(field));
    }

    private static boolean isAlpha(char c) {
        return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z';
    }

    private static boolean isAlphaNumeric(char c) {
        return isAlpha(c) || Character.isDigit(c);
    }

    private void ensureOnlyOnePresent(FieldKind f1, FieldKind f2, String message) {
        if (groups.contains(f1) && groups.contains(f2)) {
            throw formatError("Invalid format. Only one field must be present: {}", message);
        }
    }

    private void ensureBothPresent(FieldKind f1, FieldKind f2, String message) {
        if (groups.contains(f1) != groups.contains(f2)) {
            throw formatError("Invalid format. Expected both fields: {}", message);
        }
    }

    private void validateFields() {
        // Syntax rules 5) It shall not contain both <datetime template year> and <datetime template rounded year>,
        ensureOnlyOnePresent(FieldKind.YEAR, FieldKind.ROUNDED_YEAR, "year / rounded year");

        // Syntax rules 6) if contains <datetime template day of year>,
        // then it shall not contain <datetime template month> or <datetime template day of month>.
        ensureOnlyOnePresent(FieldKind.DAY_OF_YEAR, FieldKind.MONTH, "day of year / month");
        ensureOnlyOnePresent(FieldKind.DAY_OF_YEAR, FieldKind.DAY_OF_MONTH, "day of year / day of month");

        // Syntax rules 7)
        ensureOnlyOnePresent(FieldKind.HOUR_24, FieldKind.HOUR_12, "24-hour / 12-hour");

        // Syntax rules 8) if any of 12-hour and am pm must present, then both of them must present
        ensureBothPresent(FieldKind.HOUR_12, FieldKind.AM_PM, "12-hour / am pm");

        // Syntax rules 9) 24-hour and am pm
        ensureOnlyOnePresent(FieldKind.HOUR_24, FieldKind.AM_PM, "24-hour / am pm");

        // Syntax rules 10) second of day
        ensureOnlyOnePresent(FieldKind.SECOND_OF_DAY, FieldKind.HOUR_12, "second of day / 12-hour");
        ensureOnlyOnePresent(FieldKind.SECOND_OF_DAY, FieldKind.HOUR_24, "second of day / 24-hour");
        ensureOnlyOnePresent(FieldKind.SECOND_OF_DAY, FieldKind.MINUTE, "second of day / minute");
        ensureOnlyOnePresent(FieldKind.SECOND_OF_DAY, FieldKind.SECOND_OF_MINUTE, "second of day / second of minute");

        // Syntax rules 11) time zone hour and minute
        if (fields.contains(DateTimeTemplateField.TZH) != fields.contains(DateTimeTemplateField.TZM)) {
            throw formatError("Invalid format. Expected both fields: time zone hour / time zone minute");
        }
    }

    private static DateTimeException formatError(String message, Object... elements) {
        return new DateTimeException(format(message, elements));
    }
}
