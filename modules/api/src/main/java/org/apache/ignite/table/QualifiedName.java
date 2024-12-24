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

package org.apache.ignite.table;

import java.util.Objects;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Class represents a catalog object name (table, index and etc.) and provides factory methods.
 *
 * <p>Qualified name is a pair of schema name and object name. Schema name is optional and can be null, which means that it will be
 * resolved in lazy manner against default schema. Default schema depends on the context, where the name will be used. If no context is
 * available (e.g. client or embedded KeyValue API), then  "PUBLIC" will be used as default schema name.
 *
 * <p>Both schema name and object name can be case sensitive or insensitive respecting SQL-parser style quotation.
 * Unquoted name will be cast to upper case. E.g. "tbl0" - is equivalent to "TBL0", "\"Tbl0\"" - "Tbl0", etc.
 */
public class QualifiedName {
    /**
     * Parses given simple or canonical name, and returns the object qualified name.
     */
    public static QualifiedName parse(String simpleOrCanonicalName) {
        verifyObjectName(simpleOrCanonicalName);

        Tokenizer tokenizer = new Tokenizer(simpleOrCanonicalName);

        String schemaName = null;
        String objectName = tokenizer.nextToken();

        if (tokenizer.hasNext()) {
            // Canonical name
            schemaName = objectName;
            objectName = tokenizer.nextToken();
        }

        if (tokenizer.hasNext()) {
            throw new IllegalArgumentException("Canonical name format mismatch: " + simpleOrCanonicalName);
        }

        verifySchemaName(schemaName);
        verifyObjectName(objectName);

        return new QualifiedName(schemaName, objectName);
    }

    /**
     * Resolves (maybe lazily) given simple name against default schema.
     */
    public static QualifiedName fromSimple(String simpleName) {
        return of(null, simpleName);
    }

    /**
     * Normalize schemaName and objectName, and returns the object qualified name.
     */
    public static QualifiedName of(@Nullable String schemaName, String objectName) {
        String normalizedSchemaName = schemaName == null ? null : parseIdentifier(schemaName);
        String normalizedObjectName = parseIdentifier(objectName);

        verifySchemaName(normalizedSchemaName);
        verifyObjectName(normalizedObjectName);

        return new QualifiedName(normalizedSchemaName, normalizedObjectName);
    }

    private static String parseIdentifier(String name) {
        if (name == null || name.isEmpty()) {
            return name;
        }

        var tokenizer = new Tokenizer(name);

        String parsedName = tokenizer.nextToken();

        if (tokenizer.hasNext()) {
            throw new IllegalArgumentException("Non-qualified identifier expected: " + name);
        }

        return parsedName;
    }

    private static void verifyObjectName(@Nullable String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Object name can't be null or empty.");
        }
    }

    private static void verifySchemaName(@Nullable String schemaName) {
        if (schemaName != null && schemaName.isEmpty()) {
            throw new IllegalArgumentException("Schema name can't be empty.");
        }
    }

    private final @Nullable String schemaName;
    private final String objectName;

    /**
     * Constructs a qualified name.
     *
     * @param schemaName Normalized schema name.
     * @param objectName Normalized object name.
     */
    private QualifiedName(@Nullable String schemaName, String objectName) {
        this.schemaName = schemaName;
        this.objectName = objectName;
    }

    /** Returns normalized schema name. */
    public @Nullable String schemaName() {
        return schemaName;
    }

    /** Returns normalized object name. */
    public String name() {
        return objectName;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        QualifiedName that = (QualifiedName) object;
        return Objects.equals(schemaName, that.schemaName) && Objects.equals(objectName, that.objectName);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(schemaName, objectName);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        String simpleName = IgniteNameUtils.quoteIfNeeded(objectName);

        return schemaName == null ? simpleName : IgniteNameUtils.quoteIfNeeded(schemaName) + '.' + simpleName;
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
        private final String source;
        private int currentPosition;
        private boolean foundDot;

        /**
         * Creates a tokenizer for given string source.
         *
         * @param source Source string to split.
         */
        Tokenizer(String source) {
            this.source = source;
        }

        /** Returns {@code true} if at least one token is available. */
        boolean hasNext() {
            return foundDot || !isEol();
        }

        /** Returns next token. */
        @Nullable String nextToken() {
            if (!hasNext()) {
                return null;
            } else if (isEol()) {
                foundDot = false;
                return null;
            }

            boolean quoted = currentChar() == '"';

            if (quoted) {
                currentPosition++;
            }

            int start = currentPosition;
            StringBuilder sb = new StringBuilder();
            foundDot = false;

            if (!quoted && !isEol()) {
                if (identifierStart(currentPosition)) {
                    currentPosition++;
                } else {
                    throwMalformedNameException();
                }
            }

            for (; !isEol(); currentPosition++) {
                char c = currentChar();

                if (c == '"') {
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

                        foundDot = hasNextChar();
                        currentPosition += 2;

                        return sb.toString();
                    }

                    throwMalformedNameException();
                } else if (c == '.') {
                    if (quoted) {
                        continue;
                    }

                    sb.append(source, start, currentPosition);

                    currentPosition++;
                    foundDot = true;

                    return sb.toString().toUpperCase();
                } else if (!quoted && !identifierStart(currentPosition) && !identifierExtend(currentPosition)) {
                    throwMalformedNameException();
                }
            }

            if (quoted) {
                // seems like there is no closing quote
                throwMalformedNameException();
            }

            return source.substring(start).toUpperCase();
        }

        /* An <identifier start> is any character in the Unicode General Category classes “Lu”, “Ll”, “Lt”, “Lm”, “Lo”, or “Nl”. */
        private boolean identifierStart(int position) {
            return Character.isAlphabetic(Character.codePointAt(source, position));
        }

        /* An <identifier extend> is U+00B7, or any character in the Unicode General Category classes “Mn”, “Mc”, “Nd”, “Pc”, or “Cf”.*/
        private boolean identifierExtend(int position) {
            if (source.charAt(position) == '\u00B7') {
                /* “Middle Dot” character */
                return true;
            }

            int codePoint = Character.codePointAt(source, position);

            return ((((1 << Character.NON_SPACING_MARK)
                    | (1 << Character.COMBINING_SPACING_MARK)
                    | (1 << Character.DECIMAL_DIGIT_NUMBER)
                    | (1 << Character.CONNECTOR_PUNCTUATION)
                    | (1 << Character.FORMAT)) >> Character.getType(codePoint)) & 1) != 0;
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

        private void throwMalformedNameException() {
            throw new IllegalArgumentException("Malformed name [name=" + source + ", pos=" + currentPosition + ']');
        }
    }
}
