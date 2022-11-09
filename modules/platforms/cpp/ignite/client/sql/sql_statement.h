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

#include <chrono>
#include <string>
#include <unordered_map>
#include <cstdint>

namespace ignite {

/**
 * Column metadata.
 */
class sql_statement {
public:
    // Default
    sql_statement() = default;

    /**
     * Constructor.
     *
     * @param query Query text.
     * @param timeout Timeout.
     * @param schema Schema.
     * @param page_size Page size.
     */
    sql_statement(std::string query, std::chrono::milliseconds timeout, std::string schema, std::int32_t page_size)
        : m_query(std::move(query))
        , m_timeout(timeout)
        , m_schema(std::move(schema))
        , m_page_size(page_size) { }

    /**
     * Gets the query text.
     *
     * @return Query text.
     */
    [[nodiscard]] const std::string &query() const { return m_query; }

    /**
     * Gets the query timeout (zero means no timeout).
     *
     * @return Query timeout (zero means no timeout).
     */
    [[nodiscard]] std::chrono::milliseconds timeout() const { return m_timeout; }

    /**
     * Gets the SQL schema name.
     *
     * @return Schema name.
     */
    [[nodiscard]] const std::string &schema() const { return m_schema; }

    /**
     * Gets the number of rows per data page.
     *
     * @return Number of rows per data page.
     */
    [[nodiscard]] std::int32_t page_size() const { return m_page_size; }

private:
    /** Query text. */
    std::string m_query;

    /** Timeout. */
    std::chrono::milliseconds m_timeout;

    /** Schema. */
    std::string m_schema;

    /** Page size. */
    std::int32_t m_page_size;

    /** Properties. */
    std::unordered_map<std::string, std::string> m_properties;
};

} // namespace ignite
