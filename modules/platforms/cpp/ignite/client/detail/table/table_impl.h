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

#include <ignite/common/uuid.h>

#include <future>
#include <memory>
#include <utility>

namespace ignite::detail {

/**
 * Table view implementation.
 */
class table_impl {
public:
    // Deleted
    table_impl(table_impl &&) = delete;
    table_impl(const table_impl &) = delete;
    table_impl &operator=(table_impl &&) = delete;
    table_impl &operator=(const table_impl &) = delete;

    /**
     * Constructor.
     *
     * @param name Name.
     * @param id ID.
     */
    table_impl(std::string name, const uuid &id)
        : m_name(std::move(name))
        , m_id(id) { }

    /**
     * Get table name.
     *
     * @return Table name.
     */
    [[nodiscard]] const std::string &name() const { return m_name; }

private:
    /** Table name. */
    const std::string m_name;

    /** Table ID. */
    const uuid m_id;
};

} // namespace ignite::detail
