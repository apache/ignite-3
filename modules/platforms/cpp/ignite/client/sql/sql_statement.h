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

#include "ignite/common/primitive.h"

#include <chrono>
#include <cstdint>
#include <string>
#include <unordered_map>

namespace ignite {

/**
 * Column metadata.
 */
class sql_statement {
public:
    /** Default SQL schema name. */
    static constexpr const char *DEFAULT_SCHEMA{"PUBLIC"};

    /** Default number of rows per data page. */
    static constexpr std::int32_t DEFAULT_PAGE_SIZE{1024};

    /** Default query timeout (zero means no timeout). */
    static constexpr std::chrono::milliseconds DEFAULT_TIMEOUT{0};

    // Default
    sql_statement() = default;

    /**
     * Constructor.
     *
     * @param query Query text.
     * @param timeout Timeout.
     * @param schema Schema.
     * @param page_size Page size.
     * @param properties Properties list.
     * @param timezone_id Timezone ID.
     */
    sql_statement( // NOLINT(google-explicit-constructor)
        std::string query, std::chrono::milliseconds timeout = DEFAULT_TIMEOUT, std::string schema = DEFAULT_SCHEMA,
        std::int32_t page_size = DEFAULT_PAGE_SIZE,
        std::initializer_list<std::pair<const std::string, primitive>> properties = {}, std::string timezone_id = {})
        : m_query(std::move(query))
        , m_timeout(timeout)
        , m_schema(std::move(schema))
        , m_page_size(page_size)
        , m_properties(properties)
        , m_timezone_id(std::move(timezone_id)) {}

    /**
     * Gets the query text.
     *
     * @return Query text.
     */
    [[nodiscard]] const std::string &query() const { return m_query; }

    /**
     * Sets the query text.
     *
     * @param val Query text.
     */
    void query(std::string val) { m_query = std::move(val); }

    /**
     * Gets the query timeout (zero means no timeout).
     *
     * @return Query timeout (zero means no timeout).
     */
    [[nodiscard]] std::chrono::milliseconds timeout() const { return m_timeout; }

    /**
     * Sets the query timeout (zero means no timeout).
     *
     * @param val Query timeout (zero means no timeout).
     */
    void timeout(std::chrono::milliseconds val) { m_timeout = val; }

    /**
     * Gets the SQL schema name.
     *
     * @return Schema name.
     */
    [[nodiscard]] const std::string &schema() const { return m_schema; }

    /**
     * Sets the SQL schema name.
     *
     * @param val Schema name.
     */
    void schema(std::string val) { m_schema = std::move(val); }

    /**
     * Gets the number of rows per data page.
     *
     * @return Number of rows per data page.
     */
    [[nodiscard]] std::int32_t page_size() const { return m_page_size; }

    /**
     * Sets the number of rows per data page.
     *
     * @param val Number of rows per data page.
     */
    void page_size(std::int32_t val) { m_page_size = val; }

    /**
     * Gets the statement properties.
     *
     * @return Properties.
     */
    [[nodiscard]] const std::unordered_map<std::string, primitive> &properties() const { return m_properties; }

    /**
     * Sets the statement properties.
     *
     * @param val Properties.
     */
    void properties(std::initializer_list<std::pair<const std::string, primitive>> val) { m_properties = val; }

    /**
     * Gets the Timezone ID.
     *
     * @return Timezone ID.
     */
    [[nodiscard]] const std::string &timezone_id() const { return m_timezone_id; }

    /**
     * Sets the Timezone ID.
     *
     * Examples: "America/New_York", "UTC+3".
     * Affects time-related SQL functions (e.g. @c CURRENT_TIME) and string literal conversions
     * (e.g. <tt>TIMESTAMP WITH LOCAL TIME ZONE '1992-01-18 02:30:00.123'</tt>).
     *
     * If left empty, defaults to local time zone.
     *
     * @param val Timezone ID.
     */
    void timezone_id(std::string val) { m_timezone_id = std::move(val); }

private:
    /** Query text. */
    std::string m_query;

    /** Timeout. */
    std::chrono::milliseconds m_timeout{DEFAULT_TIMEOUT};

    /** Schema. */
    std::string m_schema{DEFAULT_SCHEMA};

    /** Page size. */
    std::int32_t m_page_size{DEFAULT_PAGE_SIZE};

    /** Properties. */
    std::unordered_map<std::string, primitive> m_properties;

    /** Timezone ID. */
    std::string m_timezone_id;
};

} // namespace ignite
