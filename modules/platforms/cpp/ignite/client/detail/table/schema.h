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

#include "ignite/common/ignite_error.h"
#include "ignite/common/ignite_type.h"
#include "ignite/protocol/reader.h"

#include <msgpack.h>

#include <array>
#include <memory>
#include <string>

namespace ignite::detail {

/**
 * Column.
 */
struct column {
    std::string name;
    ignite_type type{};
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
    [[nodiscard]] static column read(protocol::reader &reader) {
        auto fields_num = reader.read_int32();
        assert(fields_num >= 6); // Expect at least six columns.

        column res{};
        res.name = reader.read_string();
        res.type = static_cast<ignite_type>(reader.read_int32());
        res.is_key = reader.read_bool();
        res.nullable = reader.read_bool();
        res.scale = reader.read_int32();
        reader.skip(fields_num - 5);

        return res;
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
    schema(std::int32_t version, std::int32_t key_column_count, std::vector<column> &&columns)
        : version(version)
        , key_column_count(key_column_count)
        , columns(std::move(columns)) {}

    /**
     * Read schema using reader.
     *
     * @param reader Reader to use.
     * @return Schema instance.
     */
    static std::shared_ptr<schema> read(protocol::reader& reader) {
        std::int32_t key_column_count = 0;
        auto schema_version = reader.read_int32();

        auto columns_count = reader.read_int32();
        std::vector<column> columns;
        columns.reserve(columns_count);

        for (std::int32_t column_idx = 0; column_idx < columns_count; ++column_idx) {
            auto val = column::read(reader);
            if (val.is_key)
                ++key_column_count;

            columns.emplace_back(std::move(val));
        }

        return std::make_shared<schema>(schema_version, key_column_count, std::move(columns));
    }
};

} // namespace ignite::detail
