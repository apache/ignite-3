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
import java.util.List;
import org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.DateTimeField;

/**
 * Parses text using format elements.
 */
final class Parser {

    private final List<DateTimeFormatElement> elements;

    private final StringBuilder buf = new StringBuilder();

    private int position;

    private int elementIndex;

    private String text;

    Parser(List<DateTimeFormatElement> elements) {
        this.elements = elements;
    }

    ParsedFields parse(String text) {
        return parse(text, Clock.systemDefaultZone());
    }

    ParsedFields parse(String text, Clock clock) {

        position = 0;
        buf.setLength(0);
        elementIndex = 0;
        this.text = text;

        ParsedFields parsedFields = new ParsedFields(clock);

        while (position < text.length() && elementIndex < elements.size()) {
            DateTimeFormatElement element = elements.get(elementIndex);
            switch (element.kind) {
                case DELIMITER:
                    delimiter(element);
                    break;
                case FIELD:
                    field(element.template, parsedFields);
                    break;
                default:
                    throw new IllegalStateException("Unexpected element kind: " + element.kind);
            }
        }

        if (elementIndex < elements.size()) {
            throw new DateTimeFormatException("No values for fields: " + elements.subList(elementIndex, elements.size()));
        }

        if (position < text.length()) {
            DateTimeFormatElement e = elements.get(elementIndex - 1);
            throw new DateTimeFormatException("Unexpected trailing characters after " + e);
        }

        return parsedFields;
    }

    private char currentChar() {
        if (position >= text.length()) {
            return '\0';
        }
        return text.charAt(position);
    }

    private void advancePosition() {
        position++;
    }

    private void delimiter(DateTimeFormatElement element) {
        // Text is always present for a delimiter
        assert element.text != null;

        char c = currentChar();

        if (element.text.charAt(0) != c) {
            throw new DateTimeFormatException("Invalid format. Expected literal <" + element.text + "> but got <" + c + ">");
        }

        elementIndex += 1;
        advancePosition();
    }

    private void field(DateTimeTemplateField field, ParsedFields parsedFields) {
        if (field.field() == DateTimeField.AM_PM) {
            amPm(field, parsedFields);
        } else if (field == DateTimeTemplateField.TZH) {
            tzh(parsedFields);
        } else {
            numericField(field, parsedFields);
        }
    }

    private void amPm(DateTimeTemplateField field, ParsedFields parsedFields) {
        while (matchField(field)) {
            buf.append(currentChar());

            if (fieldParsed(field)) {
                addField(field, parsedFields);
                advancePosition();
                break;
            } else {
                advancePosition();
            }
        }
    }

    private void tzh(ParsedFields parsedFields) {
        // Time zone hour: +/- 1-2 Digit(s)
        char c = currentChar();

        if (!parsedFields.hasSign()) {
            if (c == '+') {
                parsedFields.setSign(1);
            } else if (c == '-') {
                parsedFields.setSign(-1);
            } else {
                throw new DateTimeFormatException("Expected +/- but got <" + c + ">");
            }
            advancePosition();
        }

        while (matchDigitField(DateTimeTemplateField.TZH)) {
            buf.append(currentChar());

            if (fieldParsed(DateTimeTemplateField.TZH)) {
                addField(DateTimeTemplateField.TZH, parsedFields);
                advancePosition();
                return;
            }

            advancePosition();
        }

        // Less then 2 digits or unexpected character
        tryAddField(DateTimeTemplateField.TZH, parsedFields);
    }

    private void numericField(DateTimeTemplateField field, ParsedFields parsedFields) {
        while (matchDigitField(field)) {
            buf.append(currentChar());

            if (fieldParsed(field)) {
                addField(field, parsedFields);
                advancePosition();
                return;
            }

            advancePosition();
        }

        // Less then N digits or unexpected character
        tryAddField(field, parsedFields);
    }

    private boolean matchDigitField(DateTimeTemplateField field) {
        char c = currentChar();
        return Character.isDigit(c) && buf.length() <= field.maxLength();
    }

    private boolean matchField(DateTimeTemplateField field) {
        return buf.length() < field.maxLength();
    }

    private boolean fieldParsed(DateTimeTemplateField field) {
        // We reached the maximum number of characters per field,
        return buf.length() == field.maxLength();
    }

    private void tryAddField(DateTimeTemplateField field, ParsedFields parsedFields) {
        // Handles the case when a parser expects a fields, but it encounters a delimiter or a unexpected text.
        // Report the current char as error.
        if (buf.length() == 0) {
            throw new DateTimeFormatException("Expected field " + field + " but got <" + currentChar() + ">");
        }
        addField(field, parsedFields);
    }

    private void addField(DateTimeTemplateField field, ParsedFields parsedFields) {
        // This should never happen - addField must not be called when this buffer is empty.
        if (buf.length() == 0) {
            throw new IllegalStateException("Field value is empty");
        }
        // The current character does not belong to the current field,
        // try to add new field
        parsedFields.addField(field, buf);
        elementIndex += 1;
    }
}
