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

#include "ignite/client/table/ignite_tuple.h"
#include "ignite/client/table/qualified_name.h"
#include "ignite/client/compute/job_target.h"
#include "ignite/client/detail/compute/job_target_type.h"

namespace ignite::detail {

/**
 * Job target defined by a table name and a key.
 */
class colocated_job_target : public job_target {
public:
    // Default
    colocated_job_target() = default;

    /**
     * Constructor.
     *
     * @param table_name Name of the table to be used with a @c key to determine target node.
     * @param key Table key to be used to determine the target node for job execution.
     */
    explicit colocated_job_target(qualified_name table_name, const ignite_tuple &key)
        : m_table_name(std::move(table_name))
        , m_key(key) {}

    /**
     * Get table name.
     *
     * @return Table name.
     */
    [[nodiscard]] const qualified_name &get_table_name() const { return m_table_name; }

    /**
     * Get key.
     *
     * @return Key.
     */
    [[nodiscard]] const ignite_tuple &get_key() const { return m_key; }

    [[nodiscard]] job_target_type get_type() const override { return job_target_type::COLOCATED; }

private:
    /** Table name. */
    qualified_name m_table_name;

    /** Key. */
    ignite_tuple m_key;
};

} // namespace ignite::detail
