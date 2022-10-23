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

#include "ignite/client/detail/cluster_connection.h"
#include "ignite/common/uuid.h"

#include <memory>
#include <mutex>
#include <unordered_map>

namespace ignite::detail {

// TODO: Move to separate file
/**
 * Client data types.
 */
enum class client_data_type
{
    /// Byte.
    INT8 = 1,

    /// Short.
    INT16 = 2,

    /// Int.
    INT32 = 3,

    /// Long.
    INT64 = 4,

    /// Float.
    FLOAT = 5,

    /// Double.
    DOUBLE = 6,

    /// Decimal.
    DECIMAL = 7,

    /// UUID / Guid.
    UUID = 8,

    /// String.
    STRING = 9,

    /// Byte array.
    BYTES = 10,

    /// BitMask.
    BITMASK = 11,

    /// Local date.
    DATE = 12,

    /// Local time.
    TIME = 13,

    /// Local date and time.
    DATE_TIME = 14,

    /// Timestamp (instant).
    TIMESTAMP = 15,

    /// Number (BigInt).
    NUMBER = 16,
};

/**
 * Column.
 */
struct column {
    std::string name;
    client_data_type type;
    bool nullable;
    bool is_key;
    std::int32_t schema_index;
    std::int32_t scale;
};

/**
 * Schema.
 */
struct schema {
    std::int32_t version;
    std::int32_t key_column_count;
    std::vector<column> columns;
};

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
     * @param connection Connection.
     */
    table_impl(std::string name, const uuid &id, std::shared_ptr<cluster_connection> connection)
        : m_name(std::move(name))
        , m_id(id)
        , m_connection(std::move(connection)) { }

    /**
     * Gets table name.
     *
     * @return Table name.
     */
    [[nodiscard]] const std::string &name() const { return m_name; }

    /**
     * Gets the latest schema.
     *
     * @return Latest schema.
     */
    void get_latest_schema_async(const ignite_callback<std::shared_ptr<schema>>& callback) {
        auto latest_schema_version = m_latest_schema_version;

        if (latest_schema_version >= 0) {
            std::shared_ptr<schema> schema;
            {
                std::lock_guard<std::mutex> guard(m_schemas_mutex);
                schema = m_schemas[latest_schema_version];
            }
            callback({std::move(schema)});
            return;
        }

        load_schema_async(callback);
    }

    /**
     * Load latest schema from server asynchronously.
     *
     * @return Latest schema.
     */
    void load_schema_async(const ignite_callback<std::shared_ptr<schema>>& callback) {
        // TODO:
    }

private:
    /** Table name. */
    const std::string m_name;

    /** Table ID. */
    const uuid m_id;

    /** Cluster connection. */
    std::shared_ptr<cluster_connection> m_connection;

    /** Latest schema version. */
    volatile std::int32_t m_latest_schema_version{-1};

    /** Schemas mutex. */
    std::mutex m_schemas_mutex;

    /** Schemas. */
    std::unordered_map<int32_t, std::shared_ptr<schema>> m_schemas;
};

} // namespace ignite::detail
