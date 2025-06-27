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

import static org.apache.ignite.lang.util.IgniteNameUtils.parseIdentifier;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Class represents a catalog object name (table, index and etc.) and provides factory methods.
 *
 * <p>Qualified name is a pair of schema name and object name.
 *
 * <p>Factory methods expects that given names (both: schema name and object name) respect SQL syntax rules for identifiers.
 * <ul>
 * <li>Identifier must start from any character in the Unicode General Category classes "Lu", "Ll", "Lt", "Lm", "Lo", or "Nl".
 * <li>Identifier character (expect the first one) may be U+00B7 (middle dot), or any character in the Unicode General Category classes
 * "Mn", "Mc", "Nd", "Pc", or "Cf".
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

        List<String> names = IgniteNameUtils.parseName(simpleOrCanonicalName);

        if (names.size() > 2) {
            throw new IllegalArgumentException("Canonical name format mismatch: " + simpleOrCanonicalName);
        }

        String schemaName;
        String objectName;

        if (names.size() == 1) {
            schemaName = DEFAULT_SCHEMA_NAME;
            objectName = names.get(0);
        } else {
            schemaName = names.get(0);
            objectName = names.get(1);
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
        if (schemaName == null) {
            schemaName = DEFAULT_SCHEMA_NAME;
        }

        verifyObjectIdentifier(objectName);
        verifySchemaIdentifier(schemaName);

        String schemaIdentifier = parseIdentifier(schemaName);
        String objectIdentifier = parseIdentifier(objectName);

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
        return IgniteNameUtils.canonicalName(schemaIdentifier, objectIdentifier);
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
}
