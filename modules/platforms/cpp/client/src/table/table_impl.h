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
class TableImpl {
public:
    // Deleted
    TableImpl(const TableImpl &) = delete;
    TableImpl &operator=(const TableImpl &) = delete;

    // Default
    TableImpl() = default;
    ~TableImpl() = default;
    TableImpl(TableImpl &&) = default;
    TableImpl &operator=(TableImpl &&) = default;

    /**
     * Constructor.
     *
     * @param name Name.
     * @param id ID.
     */
    TableImpl(std::string name, uuid id)
        : m_name(std::move(name))
        , m_id(id) { }

    /**
     * Get table name.
     *
     * @return Table name.
     */
    [[nodiscard]] const std::string &getName() const { return m_name; }

private:
    /** Table name. */
    std::string m_name;

    /** Table ID. */
    uuid m_id;
};

} // namespace ignite::detail
