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
#include "ignite/protocol/utils.h"
#include "ignite/schema/ignite_type.h"

#include <msgpack.h>

#include <memory>
#include <string>

namespace ignite::detail {

/**
 * Get Ignite type from int value.
 *
 * @param val Value.
 * @return Matching client data type.
 */
inline ignite_type ignite_type_from_int(std::int32_t val) {
    if (val < 1 || val >= std::int32_t(ignite_type::LAST))
        throw ignite_error("Value is out of range for Ignite type: " + std::to_string(val));
    return ignite_type(val);
}

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
    [[nodiscard]] static column unpack(const msgpack_object &object) {
        if (object.type != MSGPACK_OBJECT_ARRAY)
            throw ignite_error("Schema column expected to be serialized as array");

        const msgpack_object_array &arr = object.via.array;

        constexpr std::uint32_t expectedCount = 6;
        assert(arr.size >= expectedCount);

        column res{};
        res.name = protocol::unpack_object<std::string>(arr.ptr[0]);
        res.type = ignite_type_from_int(protocol::unpack_object<std::int32_t>(arr.ptr[1]));
        res.is_key = protocol::unpack_object<bool>(arr.ptr[2]);
        res.nullable = protocol::unpack_object<bool>(arr.ptr[3]);
        res.scale = protocol::unpack_object<std::int32_t>(arr.ptr[5]);

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
    static std::shared_ptr<schema> read(const msgpack_object_kv &object) {
        auto schema_version = protocol::unpack_object<std::int32_t>(object.key);
        std::int32_t key_column_count = 0;

        std::vector<column> columns;
        columns.reserve(protocol::unpack_array_size(object.val));

        protocol::unpack_array_raw(object.val, [&columns, &key_column_count](const msgpack_object &object) {
            auto val = column::unpack(object);
            if (val.is_key)
                ++key_column_count;
            columns.emplace_back(std::move(val));
        });

        return std::make_shared<schema>(schema_version, key_column_count, std::move(columns));
    }
};

} // namespace ignite::detail
