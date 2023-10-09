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

#include "utils.h"

#include <ignite/common/bits.h>
#include <ignite/common/uuid.h>
#include <ignite/protocol/utils.h>

#include <string>

namespace ignite::detail {

/**
 * Claim space for the column.
 *
 * @param builder Binary tuple builder.
 * @param typ Column type.
 * @param value Value.
 * @param scale Column scale.
 */
void claim_column(binary_tuple_builder &builder, ignite_type typ, const primitive &value, std::int32_t scale) {
    switch (typ) {
        case ignite_type::BOOLEAN:
            builder.claim_bool(value.get<bool>());
            break;
        case ignite_type::INT8:
            builder.claim_int8(value.get<std::int8_t>());
            break;
        case ignite_type::INT16:
            builder.claim_int16(value.get<std::int16_t>());
            break;
        case ignite_type::INT32:
            builder.claim_int32(value.get<std::int32_t>());
            break;
        case ignite_type::INT64:
            builder.claim_int64(value.get<std::int64_t>());
            break;
        case ignite_type::FLOAT:
            builder.claim_float(value.get<float>());
            break;
        case ignite_type::DOUBLE:
            builder.claim_double(value.get<double>());
            break;
        case ignite_type::UUID:
            builder.claim_uuid(value.get<uuid>());
            break;
        case ignite_type::STRING:
            builder.claim_varlen(value.get<std::string>());
            break;
        case ignite_type::BYTE_ARRAY:
            builder.claim_varlen(value.get<std::vector<std::byte>>());
            break;
        case ignite_type::DECIMAL: {
            big_decimal to_write;
            value.get<big_decimal>().set_scale(scale, to_write);
            builder.claim_number(to_write);
            break;
        }
        case ignite_type::NUMBER:
            builder.claim_number(value.get<big_integer>());
            break;
        case ignite_type::DATE:
            builder.claim_date(value.get<ignite_date>());
            break;
        case ignite_type::TIME:
            builder.claim_time(value.get<ignite_time>());
            break;
        case ignite_type::DATETIME:
            builder.claim_date_time(value.get<ignite_date_time>());
            break;
        case ignite_type::TIMESTAMP:
            builder.claim_timestamp(value.get<ignite_timestamp>());
            break;
        case ignite_type::PERIOD:
            builder.claim_period(value.get<ignite_period>());
            break;
        case ignite_type::DURATION:
            builder.claim_duration(value.get<ignite_duration>());
            break;
        case ignite_type::BITMASK:
            builder.claim_varlen(value.get<bit_array>().get_raw());
            break;
        default:
            throw ignite_error("Type with id " + std::to_string(int(typ)) + " is not yet supported");
    }
}

/**
 * Append column value to binary tuple.
 *
 * @param builder Binary tuple builder.
 * @param typ Column type.
 * @param value Value.
 * @param scale Column scale.
 */
void append_column(binary_tuple_builder &builder, ignite_type typ, const primitive &value, std::int32_t scale) {
    switch (typ) {
        case ignite_type::BOOLEAN:
            builder.append_bool(value.get<bool>());
            break;
        case ignite_type::INT8:
            builder.append_int8(value.get<std::int8_t>());
            break;
        case ignite_type::INT16:
            builder.append_int16(value.get<std::int16_t>());
            break;
        case ignite_type::INT32:
            builder.append_int32(value.get<std::int32_t>());
            break;
        case ignite_type::INT64:
            builder.append_int64(value.get<std::int64_t>());
            break;
        case ignite_type::FLOAT:
            builder.append_float(value.get<float>());
            break;
        case ignite_type::DOUBLE:
            builder.append_double(value.get<double>());
            break;
        case ignite_type::UUID:
            builder.append_uuid(value.get<uuid>());
            break;
        case ignite_type::STRING:
            builder.append_varlen(value.get<std::string>());
            break;
        case ignite_type::BYTE_ARRAY:
            builder.append_varlen(value.get<std::vector<std::byte>>());
            break;
        case ignite_type::DECIMAL: {
            big_decimal to_write;
            value.get<big_decimal>().set_scale(scale, to_write);
            builder.append_number(to_write);
            break;
        }
        case ignite_type::NUMBER:
            builder.append_number(value.get<big_integer>());
            break;
        case ignite_type::DATE:
            builder.append_date(value.get<ignite_date>());
            break;
        case ignite_type::TIME:
            builder.append_time(value.get<ignite_time>());
            break;
        case ignite_type::DATETIME:
            builder.append_date_time(value.get<ignite_date_time>());
            break;
        case ignite_type::TIMESTAMP:
            builder.append_timestamp(value.get<ignite_timestamp>());
            break;
        case ignite_type::PERIOD:
            builder.append_period(value.get<ignite_period>());
            break;
        case ignite_type::DURATION:
            builder.append_duration(value.get<ignite_duration>());
            break;
        case ignite_type::BITMASK:
            builder.append_varlen(value.get<bit_array>().get_raw());
            break;
        default:
            throw ignite_error("Type with id " + std::to_string(int(typ)) + " is not yet supported");
    }
}

/**
 * Serialize tuple using table schema.
 *
 * @param sch Schema.
 * @param tuple Tuple.
 * @param key_only Should only key fields be serialized.
 * @param no_value No value bitset.
 * @return Serialized binary tuple.
 */
std::vector<std::byte> pack_tuple(
    const schema &sch, const ignite_tuple &tuple, bool key_only, protocol::bitset_span &no_value) {
    auto count = std::int32_t(key_only ? sch.key_column_count : sch.columns.size());
    binary_tuple_builder builder{count};

    builder.start();

    for (std::int32_t i = 0; i < count; ++i) {
        const auto &col = sch.columns[i];
        auto col_idx = tuple.column_ordinal(col.name);

        if (col_idx >= 0)
            claim_column(builder, col.type, tuple.get(col_idx), col.scale);
        else
            builder.claim_null();
    }

    std::int32_t written = 0;
    builder.layout();
    for (std::int32_t i = 0; i < count; ++i) {
        const auto &col = sch.columns[i];
        auto col_idx = tuple.column_ordinal(col.name);

        if (col_idx >= 0) {
            append_column(builder, col.type, tuple.get(col_idx), col.scale);
            ++written;
        } else {
            builder.append_null();
            no_value.set(std::size_t(i));
        }
    }

    if (!key_only && written < tuple.column_count()) {
        std::vector<bool> written_ind(tuple.column_count(), false);
        for (std::int32_t i = 0; i < count; ++i) {
            const auto &col = sch.columns[i];
            auto col_idx = tuple.column_ordinal(col.name);

            if (col_idx >= 0)
                written_ind[col_idx] = true;
        }

        std::stringstream unmapped_columns;
        for (std::int32_t i = 0; i < tuple.column_count(); ++i) {
            if (written_ind[i])
                continue;
            auto &name = tuple.column_name(i);
            unmapped_columns << name << ",";
        }
        auto unmapped_columns_str = unmapped_columns.str();
        unmapped_columns_str.pop_back();

        assert(!unmapped_columns_str.empty());
        throw ignite_error("Key tuple doesn't match schema: schemaVersion="
            + std::to_string(sch.version) + ", extraColumns=" + unmapped_columns_str);
    }

    return builder.build();
}

ignite_tuple concat(const ignite_tuple &left, const ignite_tuple &right) {
    // TODO: IGNITE-18855 eliminate unnecessary tuple transformation;

    ignite_tuple res(left.column_count() + right.column_count());
    res.m_pairs.assign(left.m_pairs.begin(), left.m_pairs.end());
    res.m_indices.insert(left.m_indices.begin(), left.m_indices.end());

    for (const auto &pair : right.m_pairs) {
        bool inserted = res.m_indices.emplace(ignite_tuple::parse_name(pair.first), res.m_pairs.size()).second;
        if (inserted) {
            res.m_pairs.emplace_back(pair);
        }
    }

    return res;
}

void write_tuple(protocol::writer &writer, const schema &sch, const ignite_tuple &tuple, bool key_only) {
    const std::size_t count = key_only ? sch.key_column_count : sch.columns.size();
    const std::size_t bytes_num = bytes_for_bits(count);

    auto no_value_bytes = reinterpret_cast<std::byte *>(alloca(bytes_num));
    protocol::bitset_span no_value(no_value_bytes, bytes_num);

    auto tuple_data = pack_tuple(sch, tuple, key_only, no_value);

    writer.write_bitset(no_value.data());
    writer.write_binary(tuple_data);
}

void write_tuples(protocol::writer &writer, const schema &sch, const std::vector<ignite_tuple> &tuples, bool key_only) {
    writer.write(std::int32_t(tuples.size()));
    for (auto &tuple : tuples)
        write_tuple(writer, sch, tuple, key_only);
}

} // namespace ignite::detail
