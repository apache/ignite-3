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

#include <ignite/client/table/table.h>

#include <ignite/common/config.h>
#include <ignite/common/ignite_result.h>

#include <future>
#include <memory>
#include <optional>

namespace ignite {

namespace detail {

class tables_impl;

} // namespace

class ignite_client;

/**
 * Table management.
 */
class tables {
    friend class ignite_client;

public:
    // Default
    tables() = default;
    ~tables() = default;
    tables(tables &&) = default;
    tables &operator=(tables &&) = default;

    // Deleted
    tables(const tables &) = delete;
    tables &operator=(const tables &) = delete;

    /**
     * Gets a table by name, if it was created before.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) with SQL-parser style quotation, e.g.
     *   "public.tbl0" - the table "PUBLIC.TBL0" will be looked up,
     *   "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0",
     *   "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @param callback Callback to be called once operation is complete. On success, the callback is invoked with
     *    an instance of the table with corresponding name or @c std::nullopt if the table does not exist.
     * @throw ignite_error In case of error while trying to send a request.
     */
    IGNITE_API void get_table_async(const std::string &name, ignite_callback<std::optional<table>> callback);

    /**
     * Gets all tables.
     *
     * @param callback Callback to be called once operation is complete. On success, the callback is invoked with
     *    a vector of all tables.
     * @throw ignite_error In case of error while trying to send a request.
     */
    IGNITE_API void get_tables_async(ignite_callback<std::vector<table>> callback);

private:
    /**
     * Constructor
     *
     * @param impl Implementation
     */
    explicit tables(std::shared_ptr<detail::tables_impl> impl)
        : m_impl(std::move(impl)) { }

    /**
     * Get implementation reference.
     *
     * @return Implementation reference.
     */
    [[nodiscard]] detail::tables_impl &impl() { return *m_impl; }

    /**
     * Get implementation reference.
     *
     * @return Implementation reference.
     */
    [[nodiscard]] const detail::tables_impl &impl() const { return *m_impl; }

    /** Implementation. */
    std::shared_ptr<detail::tables_impl> m_impl;
};

} // namespace ignite
