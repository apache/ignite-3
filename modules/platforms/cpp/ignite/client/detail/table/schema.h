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

#include "ignite/common/ignite_type.h"
#include "ignite/protocol/reader.h"

#include <algorithm>
#include <memory>
#include <string>

namespace ignite::detail {

/**
 * Column.
 */
struct column {
    std::string name;
    ignite_type type{ignite_type::UNDEFINED};
    bool nullable{false};
    std::int32_t colocation_index{-1};
    std::int32_t key_index{-1};
    std::int32_t scale{0};
    std::int32_t precision{0};

    /**
     * Check if the column is the part of the key.
     *
     * @return @c true, if the column is the part of the key.
     */
    [[nodiscard]] bool is_key() const { return key_index >= 0; }

    /**
     * Unpack column from MsgPack object.
     *
     * @param reader Reader.
     * @return Column value.
     */
    [[nodiscard]] static column read(protocol::reader &reader) {
        auto constexpr minimum_expected_columns = 7;

        auto fields_num = reader.read_int32();
        assert(fields_num >= minimum_expected_columns);

        column res{};
        res.name = reader.read_string();
        res.type = static_cast<ignite_type>(reader.read_int32());
        res.key_index = reader.read_int32();
        res.nullable = reader.read_bool();
        res.colocation_index = reader.read_int32();
        res.scale = reader.read_int32();
        res.precision = reader.read_int32();
        reader.skip(fields_num - minimum_expected_columns);

        return res;
    }
};

/**
 * Schema.
 */
struct schema {
    const std::int32_t version{-1};
    const std::vector<column> columns;
    const std::vector<const column *> key_columns;
    const std::vector<const column *> val_columns;

    // Default
    schema() = default;

    /**
     * Constructor.
     *
     * @param version Version.
     * @param columns Columns.
     * @param key_columns Key Columns.
     * @param val_columns Value Columns.
     */
    schema(std::int32_t version, std::vector<column> &&columns, std::vector<const column *> &&key_columns,
        std::vector<const column *> &&val_columns)
        : version(version)
        , columns(std::move(columns))
        , key_columns(std::move(key_columns))
        , val_columns(std::move(val_columns)) {}

    /**
     * Get column by index.
     *
     * @param key_only Key only flag.
     * @param index Column index.
     * @return Column reference.
     */
    [[nodiscard]] const column &get_column(bool key_only, std::int32_t index) const {
        assert(index >= 0);
        return key_only ? *key_columns[index] : columns[index];
    }

    /**
     * Create schema instance.
     *
     * @param version Version.
     * @param cols Columns.
     * @return A new schema instance.
     */
    static std::shared_ptr<schema> create_instance(std::int32_t version, std::vector<column> &&cols) {
        std::int32_t key_columns_cnt = 0;
        for (const auto &column : cols) {
            if (column.is_key())
                ++key_columns_cnt;
        }
        std::int32_t val_columns_cnt = std::int32_t(cols.size()) - key_columns_cnt;

        std::vector<const column *> key_columns(key_columns_cnt, nullptr);

        std::vector<const column *> val_columns;
        val_columns.reserve(val_columns_cnt);

        for (const auto &column : cols) {
            if (column.is_key()) {
                assert(column.key_index >= 0 && std::size_t(column.key_index) < key_columns.size());
                assert(key_columns[column.key_index] == nullptr);

                key_columns[column.key_index] = &column;
            } else {
                val_columns.push_back(&column);
            }
        }

        return std::make_shared<schema>(version, std::move(cols), std::move(key_columns), std::move(val_columns));
    }

    /**
     * Read schema using reader.
     *
     * @param reader Reader to use.
     * @return Schema instance.
     */
    static std::shared_ptr<schema> read(protocol::reader &reader) {
        auto schema_version = reader.read_int32();

        auto columns_count = reader.read_int32();
        std::vector<column> cols;
        cols.reserve(columns_count);

        for (std::int32_t column_idx = 0; column_idx < columns_count; ++column_idx) {
            auto val = column::read(reader);

            cols.emplace_back(std::move(val));
        }

        return create_instance(schema_version, std::move(cols));
    }
};

} // namespace ignite::detail
