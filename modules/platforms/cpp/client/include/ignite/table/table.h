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
#include <utility>

#include "common/Export.h"

namespace ignite
{

namespace impl
{
class TableImpl;
class TablesImpl;
}

/**
 * Table view.
 */
class Table
{
    friend class impl::TablesImpl;
public:
    // Deleted
    Table(const Table&) = delete;
    Table& operator=(const Table&) = delete;

    // Default
    Table() = default;
    ~Table() = default;
    Table(Table&&) = default;
    Table& operator=(Table&&) = default;

    /**
     * Get table name.
     *
     * @return Table name.
     */
    [[nodiscard]]
    IGNITE_API const std::string& getName() const;

private:
    /**
     * Constructor
     *
     * @param impl Implementation
     */
    explicit Table(std::shared_ptr<void> impl) :
        m_impl(std::move(impl)) { }

    /**
     * Get implementation reference.
     *
     * @return Implementation reference.
     */
    [[nodiscard]]
    impl::TableImpl& getImpl();

    /**
     * Get implementation reference.
     *
     * @return Implementation reference.
     */
    [[nodiscard]]
    const impl::TableImpl& getImpl() const;

    /** Implementation. */
    std::shared_ptr<void> m_impl;
};

} // namespace ignite
