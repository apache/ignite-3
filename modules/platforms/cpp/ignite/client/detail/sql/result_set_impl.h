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

#include "ignite/client/detail/node_connection.h"
#include "ignite/client/detail/utils.h"
#include "ignite/client/sql/result_set_metadata.h"
#include "ignite/client/table/ignite_tuple.h"
#include "ignite/tuple/binary_tuple_parser.h"

#include <cstdint>

namespace ignite::detail {

/**
 * Query result set.
 */
class result_set_impl : public std::enable_shared_from_this<result_set_impl> {
public:
    // Default
    result_set_impl() = default;

    /**
     * Constructor.
     *
     * @param connection Node connection.
     * @param data Row set data.
     */
    result_set_impl(std::shared_ptr<node_connection> connection, bytes_view data)
        : m_connection(std::move(connection)) {
        protocol::reader reader(data);

        m_resource_id = reader.read_object_nullable<std::int64_t>();
        m_has_rowset = reader.read_bool();
        m_has_more_pages = reader.read_bool();
        m_was_applied = reader.read_bool();
        m_affected_rows = reader.read_int64();

        if (m_has_rowset) {
            auto columns = read_meta(reader);
            m_meta = result_set_metadata(columns);
            m_page = read_page(reader, m_meta);
        }
    }

    /**
     * Destructor.
     */
    ~result_set_impl() {
        close_async([](auto) {});
    }

    /**
     * Gets metadata.
     *
     * @return Metadata.
     */
    [[nodiscard]] const result_set_metadata &metadata() const { return m_meta; }

    /**
     * Gets a value indicating whether this result set contains a collection of rows.
     *
     * @return A value indicating whether this result set contains a collection of rows.
     */
    [[nodiscard]] bool has_rowset() const { return m_has_rowset; }

    /**
     * Gets the number of rows affected by the DML statement execution (such as "INSERT", "UPDATE", etc.), or 0 if
     * the statement returns nothing (such as "ALTER TABLE", etc), or -1 if not applicable.
     *
     * @return The number of rows affected by the DML statement execution.
     */
    [[nodiscard]] std::int64_t affected_rows() const { return m_affected_rows; }

    /**
     * Gets a value indicating whether a conditional query (such as "CREATE TABLE IF NOT EXISTS") was applied
     * successfully.
     *
     * @return A value indicating whether a conditional query was applied successfully.
     */
    [[nodiscard]] bool was_applied() const { return m_was_applied; }

    /**
     * Close result set asynchronously.
     *
     * @param callback Callback to call on completion.
     * @return @c true if the request was sent, and false if the result set was already closed.
     */
    bool close_async(std::function<void(ignite_result<void>)> callback) {
        if (!m_resource_id)
            return false;

        auto writer_func = [id = m_resource_id.value()](protocol::writer &writer) { writer.write(id); };

        auto reader_func = [weak_self = weak_from_this()](protocol::reader &) {
            auto self = weak_self.lock();
            if (!self)
                return;

            self->m_resource_id = std::nullopt;
        };

        return m_connection->perform_request<void>(
            protocol::client_operation::SQL_CURSOR_CLOSE, writer_func, std::move(reader_func), std::move(callback));
    }

    /**
     * Close result set synchronously.
     *
     * @return @c true if the request was sent, and false if the result set was already closed.
     */
    bool close() {
        auto pr = std::make_shared<std::promise<void>>();
        bool res = close_async([pr](auto) mutable { pr->set_value(); });

        if (!res)
            return res;

        pr->get_future().get();
        return true;
    }

    /**
     * Get current page size.
     *
     * @return Current page size.
     */
    [[nodiscard]] std::vector<ignite_tuple> current_page() {
        require_result_set();

        auto ret = std::move(m_page);
        m_page.clear();

        return ret;
    }

    /**
     * Checks whether there are more pages of results.
     *
     * @return @c true if there are more pages with results and @c false otherwise.
     */
    [[nodiscard]] bool has_more_pages() { return m_resource_id.has_value() && m_has_more_pages; }

    /**
     * Fetch the next page of results asynchronously.
     * The current page is changed after the operation is complete.
     *
     * @param callback Callback to call on completion.
     */
    void fetch_next_page_async(std::function<void(ignite_result<void>)> callback) {
        require_result_set();

        if (!m_resource_id)
            throw ignite_error("Query cursor is closed");

        if (!m_has_more_pages)
            throw ignite_error("There are no more pages");

        auto writer_func = [id = m_resource_id.value()](protocol::writer &writer) { writer.write(id); };

        auto reader_func = [weak_self = weak_from_this()](protocol::reader &reader) {
            auto self = weak_self.lock();
            if (!self)
                return;

            self->m_page = read_page(reader, self->m_meta);
            self->m_has_more_pages = reader.read_bool();
        };

        m_connection->perform_request<void>(
            protocol::client_operation::SQL_CURSOR_NEXT_PAGE, writer_func, std::move(reader_func), std::move(callback));
    }

private:
    /**
     * Checks that query has result set and throws error if it has not.
     */
    void require_result_set() const {
        if (!m_has_rowset)
            throw ignite_error("Query does not produce result set");
    }

    /**
     * Reads result set metadata.
     *
     * @param reader Reader.
     * @return Result set meta columns.
     */
    static std::vector<column_metadata> read_meta(protocol::reader &reader) {
        auto size = reader.read_array_size();

        std::vector<column_metadata> columns;
        columns.reserve(size);

        reader.read_array_raw([&columns](std::uint32_t idx, const msgpack_object &obj) {
            if (obj.type != MSGPACK_OBJECT_ARRAY)
                throw ignite_error("Meta column expected to be serialized as array");

            const msgpack_object_array &arr = obj.via.array;

            constexpr std::uint32_t min_count = 6;
            assert(arr.size >= min_count);

            auto name = protocol::unpack_object<std::string>(arr.ptr[0]);
            auto nullable = protocol::unpack_object<bool>(arr.ptr[1]);
            auto typ = ignite_type(protocol::unpack_object<std::int32_t>(arr.ptr[2]));
            auto scale = protocol::unpack_object<std::int32_t>(arr.ptr[3]);
            auto precision = protocol::unpack_object<std::int32_t>(arr.ptr[4]);

            bool origin_present = protocol::unpack_object<bool>(arr.ptr[5]);

            if (!origin_present) {
                columns.emplace_back(std::move(name), typ, precision, scale, nullable, column_origin{});
                return;
            }

            assert(arr.size >= min_count + 3);
            auto origin_name =
                arr.ptr[6].type == MSGPACK_OBJECT_NIL ? name : protocol::unpack_object<std::string>(arr.ptr[6]);

            auto origin_schema_id = protocol::try_unpack_object<std::int32_t>(arr.ptr[7]);
            std::string origin_schema;
            if (origin_schema_id) {
                if (*origin_schema_id >= std::int32_t(columns.size())) {
                    throw ignite_error("Origin schema ID is too large: " + std::to_string(*origin_schema_id)
                        + ", id=" + std::to_string(idx));
                }
                origin_schema = columns[*origin_schema_id].origin().schema_name();
            } else {
                origin_schema = protocol::unpack_object<std::string>(arr.ptr[7]);
            }

            auto origin_table_id = protocol::try_unpack_object<std::int32_t>(arr.ptr[8]);
            std::string origin_table;
            if (origin_table_id) {
                if (*origin_table_id >= std::int32_t(columns.size())) {
                    throw ignite_error("Origin table ID is too large: " + std::to_string(*origin_table_id)
                        + ", id=" + std::to_string(idx));
                }
                origin_table = columns[*origin_table_id].origin().table_name();
            } else {
                origin_table = protocol::unpack_object<std::string>(arr.ptr[8]);
            }

            column_origin origin{std::move(origin_name), std::move(origin_table), std::move(origin_schema)};
            columns.emplace_back(std::move(name), typ, precision, scale, nullable, std::move(origin));
        });

        return columns;
    }

    /**
     * Read page.
     *
     * @param reader Reader to use.
     * @return Page.
     */
    static std::vector<ignite_tuple> read_page(protocol::reader &reader, const result_set_metadata &meta) {
        auto size = reader.read_array_size();

        std::vector<ignite_tuple> page;
        page.reserve(size);

        reader.read_array_raw([&columns = meta.columns(), &page](std::uint32_t, const msgpack_object &obj) {
            auto tuple_data = protocol::unpack_binary(obj);

            auto columns_cnt = columns.size();
            ignite_tuple res(columns_cnt);
            binary_tuple_parser parser(std::int32_t(columns_cnt), tuple_data);

            for (size_t i = 0; i < columns_cnt; ++i) {
                auto &column = columns[i];
                res.set(column.name(), protocol::read_next_column(parser, column.type(), column.scale()));
            }
            page.emplace_back(std::move(res));
        });

        return page;
    }

    /** Result set metadata. */
    result_set_metadata m_meta;

    /** Has row set. */
    bool m_has_rowset{false};

    /** Affected rows. */
    std::int64_t m_affected_rows{-1};

    /** statement was applied. */
    bool m_was_applied{false};

    /** Connection. */
    std::shared_ptr<node_connection> m_connection;

    /** Resource ID. */
    std::optional<std::int64_t> m_resource_id;

    /** Has more pages. */
    bool m_has_more_pages{false};

    /** Current page. */
    std::vector<ignite_tuple> m_page;
};

} // namespace ignite::detail
