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

import static org.apache.ignite.lang.util.IgniteNameUtils.identifierExtend;
import static org.apache.ignite.lang.util.IgniteNameUtils.identifierStart;
import static org.apache.ignite.lang.util.IgniteNameUtils.quote;

import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Class represents a catalog object name (table, index and etc.) and provides factory methods.
 *
 * <p>Qualified name is a pair of schema name and object name.
 *
 * <p>Factory methods expects that given names (both: schema name and object name) respect SQL syntax rules for identifiers.
 * <ul>
 * <li>Identifier must starts from any character in the Unicode General Category classes “Lu”, “Ll”, “Lt”, “Lm”, “Lo”, or “Nl”.
 * <li>Identifier character (expect the first one) may be U+00B7 (middle dot), or any character in the Unicode General Category classes
 * “Mn”, “Mc”, “Nd”, “Pc”, or “Cf”.
 * <li>Identifier that contains any other characters must be quoted with double-quotes.
 * <li>Double-quote inside the identifier must be encoded as 2 consequent double-quote chars.
 * </ul>
 *
 * <p>{@link QualifiedName#parse(String)} method also accepts a qualified name in canonical form: {@code [<schema_name>.]<object_name>}.
 * Schema name is optional, when not set, then {@link QualifiedName#DEFAULT_SCHEMA_NAME} will be used.
 *
 * <p>The object contains normalized names, which are case sensitive. That means, an unquoted name will be cast to upper case;
 * for quoted names - the unnecessary quotes will be removed preserving escaped double-quote symbols.
 * E.g. "tbl0" - is equivalent to "TBL0", "\"Tbl0\"" - "Tbl0", etc.
 */
public final class QualifiedName implements Serializable {
    private static final long serialVersionUID = -7016402388810709149L;

    /** Default schema name. */
    public static final String DEFAULT_SCHEMA_NAME = "PUBLIC";

    /** Normalized schema name. */
    private final String schemaIdentifier;

    /** Normalized object name. */
    private final String objectIdentifier;

    /**
     * Factory method that creates qualified name object by parsing given simple or canonical object name.
     *
     * @param simpleOrCanonicalName Object simple name or qualified name in canonical form.
     * @return Qualified name.
     */
    public static QualifiedName parse(String simpleOrCanonicalName) {
        verifyObjectIdentifier(simpleOrCanonicalName);

        Tokenizer tokenizer = new Tokenizer(simpleOrCanonicalName);

        String schemaName = DEFAULT_SCHEMA_NAME;
        String objectName = tokenizer.nextToken();

        if (tokenizer.hasNext()) {
            // Canonical name
            schemaName = objectName;
            objectName = tokenizer.nextToken();
        }

        if (tokenizer.hasNext()) {
            throw new IllegalArgumentException("Canonical name format mismatch: " + simpleOrCanonicalName);
        }

        verifySchemaIdentifier(schemaName);
        verifyObjectIdentifier(objectName);

        return new QualifiedName(schemaName, objectName);
    }

    /**
     * Factory method that creates qualified name from given simple name by resolving it against the default schema.
     *
     * @param simpleName Object name.
     * @return Qualified name.
     */
    public static QualifiedName fromSimple(String simpleName) {
        return of(null, simpleName);
    }

    /**
     * Factory method that creates qualified name from given schema and object name.
     *
     * @param schemaName Schema name or {@code null} for default schema.
     * @param objectName Object name.
     * @return Qualified name.
     */
    public static QualifiedName of(@Nullable String schemaName, String objectName) {
        String schemaIdentifier = schemaName == null ? DEFAULT_SCHEMA_NAME : parseIdentifier(schemaName);
        String objectIdentifier = parseIdentifier(objectName);

        verifySchemaIdentifier(schemaIdentifier);
        verifyObjectIdentifier(objectIdentifier);

        return new QualifiedName(schemaIdentifier, objectIdentifier);
    }

    /**
     * Constructs a qualified name.
     *
     * @param schemaName Normalized schema name.
     * @param objectName Normalized object name.
     */
    QualifiedName(String schemaName, String objectName) {
        this.schemaIdentifier = schemaName;
        this.objectIdentifier = objectName;
    }

    /** Returns normalized schema name. */
    public String schemaName() {
        return schemaIdentifier;
    }

    /** Returns normalized object name. */
    public String objectName() {
        return objectIdentifier;
    }

    /** Returns qualified name in canonical form. */
    public String toCanonicalForm() {
        // TODO https://issues.apache.org/jira/browse/IGNITE-24021 Extract method and move to IgniteNameUtils
        return quoteIfNeeded(schemaIdentifier) + '.' + quoteIfNeeded(objectIdentifier);
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
        return Objects.equals(schemaIdentifier, that.schemaIdentifier) && Objects.equals(objectIdentifier, that.objectIdentifier);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(schemaIdentifier, objectIdentifier);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "QualifiedName["
                + "schemaName='" + schemaIdentifier + '\''
                + ", objectName='" + objectIdentifier + '\''
                + ']';
    }

    private static void verifyObjectIdentifier(@Nullable String identifier) {
        Objects.requireNonNull(identifier);

        if (identifier.isEmpty()) {
            throw new IllegalArgumentException("Object identifier can't be empty.");
        }
    }

    private static void verifySchemaIdentifier(@Nullable String identifier) {
        if (identifier != null && identifier.isEmpty()) {
            throw new IllegalArgumentException("Schema identifier can't be empty.");
        }
    }

    /**
     * Parse simple identifier.
     *
     * @param name Object name to parse.
     * @return Unquoted case-sensitive name name or uppercased case-insensitive name.
     * @see QualifiedName javadoc with syntax rules.
     */
    // TODO https://issues.apache.org/jira/browse/IGNITE-24021: Move to IgniteNameUtils and replace parseSimple(String).
    private static String parseIdentifier(String name) {
        if (name == null || name.isEmpty()) {
            return name;
        }

        var tokenizer = new Tokenizer(name);

        String parsedName = tokenizer.nextToken();

        if (tokenizer.hasNext()) {
            throw new IllegalArgumentException("Fully qualified name is not expected [name=" + name + "]");
        }

        return parsedName;
    }

    /**
     * Wraps the given name with double quotes if it not uppercased non-quoted name, e.g. "myColumn" -&gt; "\"myColumn\"", "MYCOLUMN" -&gt;
     * "MYCOLUMN"
     *
     * @param identifier Object identifier.
     * @return Quoted object name.
     */
    // TODO https://issues.apache.org/jira/browse/IGNITE-24021: Move to IgniteNameUtils and replace current one.
    private static String quoteIfNeeded(String identifier) {
        if (identifier.isEmpty()) {
            return identifier;
        }

        if (!identifierStart(identifier.codePointAt(0)) && !Character.isUpperCase(identifier.codePointAt(0))) {
            return quote(identifier);
        }

        for (int pos = 1; pos < identifier.length(); pos++) {
            int codePoint = identifier.codePointAt(pos);

            if (!identifierExtend(codePoint) && !Character.isUpperCase(codePoint)) {
                return quote(identifier);
            }
        }

        return identifier;
    }

    /**
     * Identifier chain tokenizer.
     *
     * <p>Splits provided identifier chain (complex identifier like PUBLIC.MY_TABLE) into its component parts.
     */
    // TODO https://issues.apache.org/jira/browse/IGNITE-24021: Move to IgniteNameUtils and replace parseSimple(String).
    static class Tokenizer {
        private final String source;
        private int currentPosition;
        private boolean foundDot;

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
            return foundDot || !isEol();
        }

        /** Returns next token. */
        public String nextToken() {
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
                } else if (!quoted
                        && !identifierStart(source.codePointAt(currentPosition))
                        && !identifierExtend(source.codePointAt(currentPosition))
                ) {
                    throwMalformedNameException();
                }
            }

            if (quoted) {
                // seems like there is no closing quote
                throwMalformedNameException();
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

        private void throwMalformedNameException() {
            throw new IllegalArgumentException("Malformed identifier [identifier=" + source + ", pos=" + currentPosition + ']');
        }
    }
}
