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

#include "ignite/common/detail/utils.h"

#include "ignite/protocol/sql/column_meta.h"
#include "ignite/common/detail/string_utils.h"

namespace ignite::protocol {

nullability nullability_from_int(std::int8_t int_value) {
    switch (int_value) {
        case std::int8_t(nullability::NO_NULL):
            return nullability::NO_NULL;
        case std::int8_t(nullability::NULLABLE):
            return nullability::NULLABLE;
        default:
            return nullability::NULLABILITY_UNKNOWN;
    }
}

column_meta_vector read_result_set_meta(protocol::reader &reader) {
    auto size = reader.read_int32();

    column_meta_vector columns;
    columns.reserve(size);

    for (std::int32_t column_idx = 0; column_idx < size; ++column_idx) {
        auto fields_cnt = reader.read_int32();
        UNUSED_VALUE fields_cnt;

        assert(fields_cnt >= 6); // Expect at least six fields.

        auto name = reader.read_string();
        auto nullable = reader.read_bool();
        auto typ = ignite_type(reader.read_int32());
        auto scale = reader.read_int32();
        auto precision = reader.read_int32();

        bool origin_present = reader.read_bool();

        if (!origin_present) {
            columns.emplace_back("", "", std::move(name), typ, precision, scale, nullable);
            continue;
        }

        assert(fields_cnt >= 9); // Expect at least three more fields.
        auto origin_name = reader.read_string_nullable();
        auto origin_schema_id = reader.try_read_int32();
        std::string origin_schema;
        if (origin_schema_id) {
            if (*origin_schema_id >= std::int32_t(columns.size())) {
                throw ignite_error("Origin schema ID is too large: " + std::to_string(*origin_schema_id)
                    + ", id=" + std::to_string(column_idx));
            }
            origin_schema = columns[*origin_schema_id].get_schema_name();
        } else {
            origin_schema = reader.read_string();
        }

        auto origin_table_id = reader.try_read_int32();
        std::string origin_table;
        if (origin_table_id) {
            if (*origin_table_id >= std::int32_t(columns.size())) {
                throw ignite_error("Origin table ID is too large: " + std::to_string(*origin_table_id)
                    + ", id=" + std::to_string(column_idx));
            }
            origin_table = columns[*origin_table_id].get_table_name();
        } else {
            origin_table = reader.read_string();
        }

        columns.emplace_back(
            std::move(origin_schema), std::move(origin_table), std::move(name), typ, precision, scale, nullable);
    }

    return columns;
}

} // namespace ignite
