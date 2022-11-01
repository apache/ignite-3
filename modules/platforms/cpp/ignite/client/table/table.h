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

#include "ignite/common/config.h"
#include "ignite/client/table/record_view.h"
#include "ignite/client/table/ignite_tuple.h"

#include <memory>
#include <utility>

namespace ignite {

namespace detail {

class table_impl;
class tables_impl;

} // namespace

/**
 * Table view.
 */
class table {
    friend class detail::tables_impl;

public:
    // Default
    table() = default;
    ~table() = default;
    table(table &&) noexcept = default;
    table &operator=(table &&) noexcept = default;

    // Deleted
    table(const table &) = delete;
    table &operator=(const table &) = delete;

    /**
     * Gets table name.
     *
     * @return Table name.
     */
    [[nodiscard]] IGNITE_API const std::string &name() const noexcept;

    /**
     * Gets the record binary view.
     *
     * @return Record binary view.
     */
    [[nodiscard]] IGNITE_API record_view<ignite_tuple> record_binary_view() const noexcept;

private:
    /**
     * Constructor
     *
     * @param impl Implementation
     */
    explicit table(std::shared_ptr<detail::table_impl> impl)
        : m_impl(std::move(impl)) { }

    /** Implementation. */
    std::shared_ptr<detail::table_impl> m_impl;
};

} // namespace ignite
