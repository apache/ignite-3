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
 * @brief Qualified name of a database object.
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
    IGNITE_API static constexpr std::string_view DEFAULT_SCHEMA_NAME = sql_statement::DEFAULT_SCHEMA;

    /** Separator character between schema and object names. */
    IGNITE_API static constexpr char SEPARATOR_CHAR = '.';

    /** Quote character for identifiers. */
    IGNITE_API static constexpr char QUOTE_CHAR = '"';

    // Default
    qualified_name() = default;

    /**
     * Create a new instance of a qualified_name class.
     *
     * @param schema_name Schema name. If empty, the default schema name is assumed. @see DEFAULT_SCHEMA_NAME.
     * @param object_name Object name. Cannot be empty.
     * @return A new instance of a qualified_name class.
     */
    [[nodiscard]] IGNITE_API static qualified_name create(std::string_view schema_name, std::string_view object_name);

    /**
     * Create a new instance of a qualified_name class from a string.
     *
     * @param simple_or_canonical_name Simple or canonical name. Cannot be empty.
     * @return A new instance of a qualified_name class.
     */
    [[nodiscard]] IGNITE_API static qualified_name parse(std::string_view simple_or_canonical_name);

    /**
     * Gets the schema name.
     *
     * @return schema name.
     */
    [[nodiscard]] const std::string& get_schema_name() const { return m_schema_name; }

    /**
     * Gets the object name.
     *
     * @return Object name.
     */
    [[nodiscard]] const std::string& get_object_name() const { return m_object_name; }

    /**
     * Gets a fully qualified name in canonical form, that is, enclosing each part of the identifier chain in double
     * quotes if necessary.
     *
     * @return A fully qualified name in canonical form.
     */
    [[nodiscard]] const std::string& get_canonical_name() const;

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

    /** Schema name. */
    std::string m_schema_name;

    /** Object name. */
    std::string m_object_name;

    /** Canonical name. */
    mutable std::string m_canonical_name;
};
} // namespace ignite
