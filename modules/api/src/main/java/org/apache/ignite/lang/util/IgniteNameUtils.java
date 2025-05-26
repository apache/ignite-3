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

package org.apache.ignite.lang.util;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Utility methods used for cluster's named objects: schemas, tables, columns, indexes, etc.
 */
public final class IgniteNameUtils {
    /** No instance methods. */
    private IgniteNameUtils() {
    }

    /**
     * Parses an SQL-compliant object identifier.
     *
     * @param identifier Object identifier.
     * @return Unquoted identifier or identifier is cast to upper case. "tbl0" -&gt; "TBL0", "\"Tbl0\"" -&gt; "Tbl0".
     */
    public static String parseIdentifier(String identifier) {
        ensureNotNullAndNotEmpty(identifier, "name");

        var tokenizer = new Tokenizer(identifier);

        String parsedName = tokenizer.nextToken();

        if (tokenizer.hasNext()) {
            throw new IllegalArgumentException("Fully qualified name is not expected [name=" + identifier + "]");
        }

        return parsedName;
    }

    /**
     * Parses the canonical name of an object.
     *
     * @param name Full object name in canonical form.
     * @return List of identifiers, where each identifier within the full name chain will be either unquoted or converted to uppercase.
     */
    public static List<String> parseName(String name) {
        ensureNotNullAndNotEmpty(name, "name");

        List<String> identifiers = new ArrayList<>(2);
        Tokenizer tokenizer = new Tokenizer(name);

        do {
            identifiers.add(tokenizer.nextToken());
        } while (tokenizer.hasNext());

        return identifiers;
    }

    /**
     * Creates a fully qualified name in canonical form, that is, enclosing each part of the identifier chain in double quotes.
     *
     * @param schemaName Normalized name of the schema.
     * @param objectName Normalized name of the object.
     * @return Returns fully qualified name in canonical form.
     */
    public static String canonicalName(String schemaName, String objectName) {
        return quoteIfNeeded(schemaName) + '.' + quoteIfNeeded(objectName);
    }

    /**
     * Wraps the given name with double quotes if it is not uppercased non-quoted name, e.g. "myColumn" -&gt; "\"myColumn\"",
     * "MYCOLUMN" -&gt; "MYCOLUMN"
     *
     * @param identifier Object identifier.
     * @return Quoted object name.
     */
    public static String quoteIfNeeded(String identifier) {
        ensureNotNullAndNotEmpty(identifier, "identifier");

        int codePoint = identifier.codePointAt(0);

        if (codePoint != '_' && !(Character.isUpperCase(codePoint) && identifierStart(codePoint))) {
            return quote(identifier);
        }

        for (int pos = 1; pos < identifier.length(); pos++) {
            codePoint = identifier.codePointAt(pos);

            if (!((Character.isUpperCase(codePoint) && identifierStart(codePoint)) || identifierExtend(codePoint))) {
                return quote(identifier);
            }
        }

        return identifier;
    }

    /**
     * Returns {@code true} if given string is valid normalized identifier, {@code false} otherwise.
     */
    public static boolean isValidNormalizedIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return false;
        }

        int codePoint = identifier.codePointAt(0);
        if (!identifierStart(codePoint)) {
            return false;
        }

        for (int pos = 1; pos < identifier.length(); pos++) {
            codePoint = identifier.codePointAt(pos);
            if (!(identifierStart(codePoint) || identifierExtend(codePoint))) {
                return false;
            }
        }

        return true;
    }

    /** An {@code identifier start} is any character in the Unicode General Category classes "Lu", "Ll", "Lt", "Lm", "Lo", or "Nl". */
    private static boolean identifierStart(int codePoint) {
        return Character.isAlphabetic(codePoint) || codePoint == '_';
    }

    /** An {@code identifier extend} is U+00B7, or any character in the Unicode General Category classes "Mn", "Mc", "Nd", "Pc", or "Cf".*/
    private static boolean identifierExtend(int codePoint) {
        return codePoint == ('·' & 0xff) /* "Middle Dot" character */
                || ((((1 << Character.NON_SPACING_MARK)
                | (1 << Character.COMBINING_SPACING_MARK)
                | (1 << Character.DECIMAL_DIGIT_NUMBER)
                | (1 << Character.CONNECTOR_PUNCTUATION)
                | (1 << Character.FORMAT)) >> Character.getType(codePoint)) & 1) != 0;

    }

    /** Wraps the given name with double quotes. */
    private static String quote(String name) {
        if (name.chars().noneMatch(cp -> cp == '\"')) {
            return '\"' + name + '\"';
        }

        StringBuilder sb = new StringBuilder(name.length() + 2).append('\"');
        for (int currentPosition = 0; currentPosition < name.length(); currentPosition++) {
            char ch = name.charAt(currentPosition);
            if (ch == '\"') {
                sb.append('\"');
            }
            sb.append(ch);
        }
        sb.append('\"');

        return sb.toString();
    }

    private static void ensureNotNullAndNotEmpty(@Nullable String argument, String argumentName) {
        Objects.requireNonNull(argument, "name");

        if (argument.isEmpty()) {
            throw new IllegalArgumentException("Argument \"" + argumentName + "\" can't be empty.");
        }
    }

    /**
     * Identifier chain tokenizer.
     *
     * <p>Splits provided identifier chain (complex identifier like PUBLIC.MY_TABLE) into its component parts.
     */
    private static class Tokenizer {
        private final String source;
        private int currentPosition;
        private boolean foundDot;

        /**
         * Creates a tokenizer for given string source.
         *
         * @param source Source string to split.
         */
        private Tokenizer(String source) {
            this.source = source;
        }

        /** Returns {@code true} if at least one token is available. */
        private boolean hasNext() {
            return foundDot || !isEol();
        }

        /** Returns next token. */
        private String nextToken() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more tokens available.");
            } else if (isEol()) {
                assert foundDot;

                foundDot = false;

                return "";
            }

            boolean quoted = currentChar() == '"';

            if (quoted) {
                currentPosition++;
            }

            int start = currentPosition;
            StringBuilder sb = new StringBuilder();
            foundDot = false;

            if (!quoted && !isEol()) {
                if (identifierStart(source.codePointAt(currentPosition))) {
                    currentPosition++;
                } else {
                    throwMalformedIdentifierException();
                }
            }

            for (; !isEol(); currentPosition++) {
                char c = currentChar();

                if (c == '"') {
                    if (!quoted) {
                        throwMalformedIdentifierException();
                    }

                    if (hasNextChar() && nextChar() == '"') {  // quote is escaped
                        sb.append(source, start, currentPosition + 1);

                        start = currentPosition + 2;
                        currentPosition += 1;

                        continue;
                    } else if (!hasNextChar() || nextChar() == '.') {
                        // looks like we just found a closing quote
                        sb.append(source, start, currentPosition);

                        foundDot = hasNextChar();
                        currentPosition += 2;

                        return sb.toString();
                    }

                    throwMalformedIdentifierException();
                } else if (c == '.') {
                    if (quoted) {
                        continue;
                    }

                    sb.append(source, start, currentPosition);

                    currentPosition++;
                    foundDot = true;

                    return sb.toString().toUpperCase();
                } else if (!quoted
                        && !identifierStart(source.codePointAt(currentPosition))
                        && !identifierExtend(source.codePointAt(currentPosition))
                ) {
                    throwMalformedIdentifierException();
                }
            }

            if (quoted) {
                // seems like there is no closing quote
                throwMalformedIdentifierException();
            }

            return source.substring(start).toUpperCase();
        }

        private boolean isEol() {
            return currentPosition >= source.length();
        }

        private char currentChar() {
            return source.charAt(currentPosition);
        }

        private boolean hasNextChar() {
            return currentPosition + 1 < source.length();
        }

        private char nextChar() {
            return source.charAt(currentPosition + 1);
        }

        private void throwMalformedIdentifierException() {
            throw new IllegalArgumentException("Malformed identifier [identifier=" + source + ", pos=" + currentPosition + ']');
        }
    }
}
