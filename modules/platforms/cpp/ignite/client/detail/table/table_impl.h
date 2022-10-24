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
 * Get client data type from int value.
 *
 * @param val Value.
 * @return Matching client data type.
 */
inline client_data_type client_data_type_from_int(std::int32_t val) {
    if (val > std::int32_t(client_data_type::NUMBER) || val < std::int32_t(client_data_type::INT8))
        throw ignite_error("Value is out of range for client data type: " + std::to_string(val));
    return client_data_type(val);
}

/**
 * Column.
 */
struct column {
    std::string name;
    client_data_type type{};
    bool nullable{false};
    bool is_key{false};
    std::int32_t schema_index{0};
    std::int32_t scale{0};

    /**
     * Unpack column from MsgPack object.
     *
     * @param object MsgPack object.
     * @return Column value.
     */
    [[nodiscard]] static column unpack(const msgpack_object& object) {
        if (object.type != MSGPACK_OBJECT_ARRAY)
            throw ignite_error("Schema column expected to be serialized as array");

        const msgpack_object_array& arr = object.via.array;

        constexpr std::uint32_t expectedCount = 6;
        assert(arr.size >= expectedCount);

        column res{};
        res.name = protocol::unpack_object<std::string>(arr.ptr[0]);
        res.type = client_data_type_from_int(protocol::unpack_object<std::int32_t>(arr.ptr[1]));
        res.is_key = protocol::unpack_object<bool>(arr.ptr[2]);
        res.nullable = protocol::unpack_object<bool>(arr.ptr[3]);
        res.scale = protocol::unpack_object<std::int32_t>(arr.ptr[5]);

        return std::move(res);
    }
};

/**
 * Schema.
 */
struct schema {
    std::int32_t version{-1};
    std::int32_t key_column_count{0};
    std::vector<column> columns;

    // Default
    schema() = default;

    /**
     * Constructor.
     *
     * @param version Version.
     * @param key_column_count Key column count.
     * @param columns Columns.
     */
    schema(std::int32_t version, std::int32_t key_column_count, std::vector<column>&& columns)
        : version(version)
        , key_column_count(key_column_count)
        , columns(std::move(columns)) {}

    /**
     * Read schema using reader.
     *
     * @param reader Reader to use.
     * @return Schema instance.
     */
    static std::shared_ptr<schema> read(const msgpack_object_kv& object) {
        auto schema_version = protocol::unpack_object<std::int32_t>(object.key);
        std::int32_t key_column_count = 0;

        std::vector<column> columns;
        columns.reserve(protocol::unpack_array_size(object.val));

        protocol::unpack_array_raw(object.val, [&columns, &key_column_count](const msgpack_object& object) {
            auto val = column::unpack(object);
            if (val.is_key)
                ++key_column_count;
            columns.emplace_back(std::move(val));
        });

        return std::make_shared<schema>(schema_version, key_column_count, std::move(columns));
    }
};

/**
 * Table view implementation.
 */
class table_impl : std::enable_shared_from_this<table_impl> {
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

private:
    /**
     * Load latest schema from server asynchronously.
     *
     * @return Latest schema.
     */
    void load_schema_async(ignite_callback<std::shared_ptr<schema>> callback) {
        auto writer_func = [&](protocol::writer &writer) {
            writer.write(m_id);
            writer.write_nil();
        };

        auto table = shared_from_this();
        auto reader_func = [table](protocol::reader &reader) mutable -> std::shared_ptr<schema> {
            auto schema_cnt = reader.read_array_size();
            if (!schema_cnt)
                throw ignite_error("Schema not found");

            std::shared_ptr<schema> last;
            reader.read_map_raw([&last, &table](const msgpack_object_kv& object) {
                last = schema::read(object);
                table->add_schema(last);
            });

            return std::move(last);
        };

        m_connection->perform_request<std::shared_ptr<schema>>(
            client_operation::TABLE_GET, writer_func, std::move(reader_func), std::move(callback));
    }

    /**
     * Add schema.
     *
     * @param val Schema.
     */
    void add_schema(const std::shared_ptr<schema>& val) {
        std::lock_guard<std::mutex> lock(m_schemas_mutex);
        if (m_latest_schema_version < val->version)
            m_latest_schema_version = val->version;

        m_schemas[val->version] = val;
    }

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
