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

#include <string>

namespace ignite {

/**
 * @brief SQL column origin.
 *
 * Original name of column.
 * Required when SQL statement change column name using aliases.
 * See @ref ignite::column_metadata
 */
class column_origin {
public:
    // Default
    column_origin() = default;

    /**
     * Constructor.
     *
     * @param column_name Column name.
     * @param table_name Table name
     * @param schema_name Schema name.
     */
    column_origin(std::string column_name, std::string table_name, std::string schema_name)
        : m_column_name(std::move(column_name))
        , m_table_name(std::move(table_name))
        , m_schema_name(std::move(schema_name)) {}

    /**
     * Gets the column name.
     *
     * @return Column name.
     */
    [[nodiscard]] const std::string &column_name() const { return m_column_name; }

    /**
     * Gets the table name.
     *
     * @return Table name.
     */
    [[nodiscard]] const std::string &table_name() const { return m_table_name; }

    /**
     * Gets the schema name.
     *
     * @return Schema name.
     */
    [[nodiscard]] const std::string &schema_name() const { return m_schema_name; }

private:
    /** Column name. */
    std::string m_column_name;

    /** Table name. */
    std::string m_table_name;

    /** Schema name. */
    std::string m_schema_name;
};

} // namespace ignite
