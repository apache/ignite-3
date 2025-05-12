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

#pragma once

#include "ignite/client/sql/sql_statement.h"
#include "ignite/common/detail/config.h"

#include <algorithm>
#include <string>
#include <string_view>

namespace ignite {

/**
 * Represents a qualified name of a database object.
 *
 * Schema name and object name should conform to SQL syntax rules for identifiers.
 * - Identifier must start from any character in the Unicode General Category classes
 *   "Lu", "Ll", "Lt", "Lm", "Lo", or "Nl";
 * - Identifier character (expect the first one) may be U+00B7 (middle dot), or any character in the
 *   Unicode General Category classes "Mn", "Mc", "Nd", "Pc", or "Cf";
 * - Identifier that contains any other characters must be quoted with double-quotes;
 * - Double-quote inside the identifier must be encoded as 2 consequent double-quote chars.
 */
class qualified_name {
public:
    /** Default schema name. */
    static constexpr std::string_view DEFAULT_SCHEMA_NAME = sql_statement::DEFAULT_SCHEMA;

    /** Separator character between schema and object names. */
    static constexpr char SEPARATOR_CHAR = '.';

    /** Quote character for identifiers. */
    static constexpr char QUOTE_CHAR = '"';

    // Default
    qualified_name() = default;

    /**
     * Create a new instance of a qualified_name class.
     *
     * @param schema_name Schema name. If empty, the default schema name is assumed. @see DEFAULT_SCHEMA_NAME.
     * @param object_name Object name. Cannot be empty.
     * @return A new instance of a qualified_name class.
     */
    static qualified_name create(std::string schema_name, std::string object_name);

    /**
     * Create a new instance of a qualified_name class with default schema.
     *
     * @param object_name Object name. Cannot be empty.
     * @return A new instance of a qualified_name class.
     */
    static qualified_name from_simple(std::string object_name) {
        return create({}, std::move(object_name));
    }

    /**
     * Gets the schema name.
     *
     * @return schema name.
     */
    const std::string& schema_name() const { return m_schema_name; }

    /**
     * Gets the object name.
     *
     * @return Object name.
     */
    const std::string& object_name() const { return m_object_name; }

    /**
     * Gets a fully qualified name in canonical form, that is, enclosing each part of the identifier chain in double
     * quotes if necessary.
     *
     * @return A fully qualified name in canonical form.
     */
    std::string canonical_name() const {
        return quote_if_needed(m_object_name) + SEPARATOR_CHAR + quote_if_needed(m_object_name);
    }

private:
    /**
     * Constructor.
     *
     * @param schema_name Schema name. If empty, the default schema name is assumed. @see DEFAULT_SCHEMA_NAME.
     * @param object_name Object name. Cannot be empty.
     */
    qualified_name(std::string schema_name, std::string object_name)
        : m_schema_name(std::move(schema_name))
        , m_object_name(std::move(object_name)) {}

    /**
     * Wraps the given name with double quotes if it is not uppercased non-quoted name,
     * e.g. "myColumn" -> "\"myColumn\"", "MYCOLUMN" -> "MYCOLUMN".
     *
     * @param name Name.
     * @return Quoted name.
     */
    static std::string quote_if_needed(std::string_view name);

    /** Schema name. */
    std::string m_schema_name;

    /** Object name. */
    std::string m_object_name;
};
} // namespace ignite
