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
#include "ignite/client/detail/table/schema.h"
#include "ignite/client/table/ignite_tuple.h"
#include "ignite/client/transaction/transaction.h"
#include "ignite/common/uuid.h"

#include <memory>
#include <mutex>
#include <unordered_map>

namespace ignite::detail {

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
     * @param callback Callback which is going to be called with the latest schema.
     */
    void get_latest_schema_async(ignite_callback<std::shared_ptr<schema>> callback);

    /**
     * Gets the latest schema.
     *
     * @param handler Callback to call on error during retrieval of the latest schema.
     */
    template <typename T>
    void with_latest_schema_async(ignite_callback<T> handler, std::function<void(const schema&, ignite_callback<T>)> callback) {
        get_latest_schema_async(
            [this, handler = std::move(handler), &callback] (ignite_result<std::shared_ptr<schema>>&& res) mutable {
            if (res.has_error()) {
                handler(ignite_error{res.error()});
                return;
            }

            auto schema = res.value();
            if (!schema) {
                handler(ignite_error{"Can not get a schema for the table " + m_name});
                return;
            }

            callback(*schema, std::move(handler));
        });
    }

    /**
     * Gets a record by key asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *  single operation is used.
     * @param key Key.
     * @param callback Callback.
     */
    void get_async(transaction* tx, const ignite_tuple& key, ignite_callback<std::optional<ignite_tuple>> callback);

    /**
     * Inserts a record into the table if does not exist or replaces the existed one.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *  single operation is used.
     * @param record A record to insert into the table. The record cannot be @c nullptr.
     * @param callback Callback.
     */
    void upsert_async(transaction* tx, const ignite_tuple& record, ignite_callback<void> callback);

private:
    /**
     * Load latest schema from server asynchronously.
     *
     * @return Latest schema.
     */
    void load_schema_async(ignite_callback<std::shared_ptr<schema>> callback);

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

    /**
     * Get schema by version.
     *
     * @param version Schema version.
     */
    std::shared_ptr<schema> get_schema(std::int32_t version) {
        std::lock_guard<std::mutex> lock(m_schemas_mutex);

        auto it = m_schemas.find(version);
        if (it == m_schemas.end())
            return {};

        return it->second;
    }

    /**
     * Read schema version from reader and try and retrieve schema instance for it.
     *
     * @param reader Reader to use.
     */
    std::shared_ptr<schema> get_schema(protocol::reader& reader) {
        auto schema_version = reader.read_object_nullable<std::int32_t>();
        std::shared_ptr<schema> sch;
        if (schema_version)
            sch = get_schema(schema_version.value());

        return std::move(sch);
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
