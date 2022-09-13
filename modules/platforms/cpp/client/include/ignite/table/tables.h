/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

#include <future>
#include <memory>
#include <optional>

#include "common/Export.h"
#include "ignite/table/table.h"

namespace ignite
{

namespace impl
{
class TablesImpl;
}

class IgniteClient;

/**
 * Table management.
 */
class Tables
{
    friend class IgniteClient;
public:
    // Deleted
    Tables(const Tables&) = delete;
    Tables& operator=(const Tables&) = delete;

    // Default
    Tables() = default;
    ~Tables() = default;
    Tables(Tables&&) = default;
    Tables& operator=(Tables&&) = default;

    /**
     * Gets a table by name, if it was created before.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) with SQL-parser style quotation, e.g.
     *   "public.tbl0" - the table "PUBLIC.TBL0" will be looked up,
     *   "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0",
     *   "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @return Table with corresponding name or @c std::nullopt if the table does not exist.
     * @throw IgniteError In case of error.
     */
    [[nodiscard]]
    IGNITE_API std::future<std::optional<Table>> getTableAsync(const std::string& name);

private:
    /**
     * Constructor
     *
     * @param impl Implementation
     */
    explicit Tables(std::shared_ptr<void> impl);

    /**
     * Get implementation reference.
     *
     * @return Implementation reference.
     */
    [[nodiscard]]
    impl::TablesImpl& getImpl();

    /**
     * Get implementation reference.
     *
     * @return Implementation reference.
     */
    [[nodiscard]]
    const impl::TablesImpl& getImpl() const;

    /** Implementation. */
    std::shared_ptr<void> m_impl;
};

} // namespace ignite
