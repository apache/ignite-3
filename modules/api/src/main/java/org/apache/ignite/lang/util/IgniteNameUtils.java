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

import java.util.regex.Pattern;
import org.jetbrains.annotations.Nullable;

/**
 * Utility methods used for cluster's named objects: schemas, tables, columns, indexes, etc.
 */
public final class IgniteNameUtils {
    private static final Pattern NAME_PATTER = Pattern.compile("^(?:\\p{Alpha}\\w*)(?:\\.\\p{Alpha}\\w*)?$");

    /** No instance methods. */
    private IgniteNameUtils() {
    }

    /**
     * Parse simple name: unquote name or cast to upper case not-quoted name.
     *
     * @param name String to parse object name.
     * @return Unquoted name or name is cast to upper case. "tbl0" -&gt; "TBL0", "\"Tbl0\"" -&gt; "Tbl0".
     * @throws IllegalArgumentException if name is {@code null} or empty.
     */
    public static String parseSimpleName(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Name is null or empty.");
        }

        var tokenizer = new Tokenizer(name);

        String parsedName = tokenizer.nextToken();

        if (tokenizer.hasNext()) {
            throw new IllegalArgumentException("Fully qualified name is not expected [name=" + name + "]");
        }

        return parsedName;
    }

    /**
     * Creates a fully qualified name in canonical form, that is,
     * enclosing each part of the identifier chain in double quotes.
     *
     * @param schemaName Name of the schema.
     * @param objectName Name of the object.
     * @return Returns fully qualified name in canonical form.
     */
    public static String canonicalName(String schemaName, String objectName) {
        return quote(schemaName) + '.' + quote(objectName);
    }

    /**
     * Tests if given string is fully qualified name in canonical form or simple name.
     *
     * @param s String to test.
     * @return {@code True} if given string is fully qualified name in canonical form or simple name.
     */
    public static boolean canonicalOrSimpleName(String s) {
        return NAME_PATTER.matcher(s).matches();
    }

    /**
     * Wraps the given name with double quotes, e.g. "myColumn" -&gt; "\"myColumn\""
     *
     * @param name Object name.
     * @return Quoted object name.
     * @throws IllegalArgumentException if name is {@code null} or empty.
     */
    public static String quote(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Name is null or empty.");
        }

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

    /**
     * Wraps the given name with double quotes if it not upper case not-quoted name,
     *     e.g. "myColumn" -&gt; "\"myColumn\"", "MYCOLUMN" -&gt; "MYCOLUMN"
     *
     * @param name Object name.
     * @return Quoted object name.
     * @throws IllegalArgumentException if name is {@code null} or empty.
     */
    public static String quoteIfNeeded(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Name is null or empty.");
        }

        if (name.charAt(0) == '\"') {
            String simpleName = parseSimpleName(name);

            return name.equals(quote(simpleName)) ? name : quote(name);
        }

        if (!NAME_PATTER.matcher(name).matches()) {
            return quote(name);
        }

        return name.equals(name.toUpperCase()) ? name : quote(name);
    }

    /**
     * Identifier chain tokenizer.
     *
     * <p>Splits provided identifier chain (complex identifier like PUBLIC.MY_TABLE) into its component parts.
     *
     * <p>This tokenizer is not SQL compliant, but it is ok since it used to retrieve an object only. The sole purpose of this tokenizer
     * is to split the chain into parts by a dot considering the quotation.
     */
    private static class Tokenizer {
        private int currentPosition;
        private final String source;

        /**
         * Creates a tokenizer for given string source.
         *
         * @param source Source string to split.
         */
        public Tokenizer(String source) {
            this.source = source;
        }

        /** Returns {@code true} if at least one token is available. */
        public boolean hasNext() {
            return currentPosition < source.length();
        }

        /** Returns next token. */
        public @Nullable String nextToken() {
            if (!hasNext()) {
                return null;
            }

            boolean quoted = source.charAt(currentPosition) == '"';

            if (quoted) {
                currentPosition++;
            }

            int start = currentPosition;
            StringBuilder sb = new StringBuilder();

            for (; currentPosition < source.length(); currentPosition++) {
                if (currentChar() == '"') {
                    if (!quoted) {
                        throwMalformedNameException();
                    }

                    if (hasNextChar() && nextChar() == '"') {  // quote is escaped
                        sb.append(source, start, currentPosition + 1);

                        start = currentPosition + 2;
                        currentPosition += 1;

                        continue;
                    } else if (!hasNextChar() || nextChar() == '.') {
                        // looks like we just found a closing quote
                        sb.append(source, start, currentPosition);

                        currentPosition += 2;

                        return sb.toString();
                    }

                    throwMalformedNameException();
                } else if (!quoted && (currentChar() == '.' || currentChar() == ' ')) {
                    sb.append(source, start, currentPosition);

                    currentPosition++;

                    return sb.toString().toUpperCase();
                }
            }

            if (quoted) {
                // seems like there is no closing quote
                throwMalformedNameException();
            }

            return source.substring(start).toUpperCase();
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

        private void throwMalformedNameException() {
            throw new IllegalArgumentException("Malformed name [name=" + source + ", pos=" + currentPosition + ']');
        }
    }
}
