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

#include <ignite/protocol/utils.h>
#include <ignite/common/bits.h>
#include <ignite/common/uuid.h>

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

    builder.layout();
    for (std::int32_t i = 0; i < count; ++i) {
        const auto &col = sch.columns[i];
        auto col_idx = tuple.column_ordinal(col.name);

        if (col_idx >= 0)
            append_column(builder, col.type, tuple.get(col_idx), col.scale);
        else {
            builder.append_null();
            no_value.set(std::size_t(i));
        }
    }

    return builder.build();
}

void claim_primitive_with_type(binary_tuple_builder &builder, const primitive &value) {
    if (value.is_null()) {
        builder.claim_null(); // Type.
        builder.claim_null(); // Scale.
        builder.claim_null(); // Value.
        return;
    }

    switch (value.get_type()) {
        case ignite_type::BOOLEAN: {
            protocol::claim_type_and_scale(builder, ignite_type::INT8);
            builder.claim_bool(value.get<bool>());
            break;
        }
        case ignite_type::INT8: {
            protocol::claim_type_and_scale(builder, ignite_type::INT8);
            builder.claim_int8(value.get<std::int8_t>());
            break;
        }
        case ignite_type::INT16: {
            protocol::claim_type_and_scale(builder, ignite_type::INT16);
            builder.claim_int16(value.get<std::int16_t>());
            break;
        }
        case ignite_type::INT32: {
            protocol::claim_type_and_scale(builder, ignite_type::INT32);
            builder.claim_int32(value.get<std::int32_t>());
            break;
        }
        case ignite_type::INT64: {
            protocol::claim_type_and_scale(builder, ignite_type::INT64);
            builder.claim_int64(value.get<std::int64_t>());
            break;
        }
        case ignite_type::FLOAT: {
            protocol::claim_type_and_scale(builder, ignite_type::FLOAT);
            builder.claim_float(value.get<float>());
            break;
        }
        case ignite_type::DOUBLE: {
            protocol::claim_type_and_scale(builder, ignite_type::DOUBLE);
            builder.claim_double(value.get<double>());
            break;
        }
        case ignite_type::UUID: {
            protocol::claim_type_and_scale(builder, ignite_type::UUID);
            builder.claim_uuid(value.get<uuid>());
            break;
        }
        case ignite_type::STRING: {
            protocol::claim_type_and_scale(builder, ignite_type::STRING);
            builder.claim_varlen(value.get<std::string>());
            break;
        }
        case ignite_type::BYTE_ARRAY: {
            protocol::claim_type_and_scale(builder, ignite_type::BYTE_ARRAY);
            auto &data = value.get<std::vector<std::byte>>();
            builder.claim_varlen(data);
            break;
        }
        case ignite_type::DECIMAL: {
            const auto &dec_value = value.get<big_decimal>();
            protocol::claim_type_and_scale(builder, ignite_type::DECIMAL, dec_value.get_scale());
            builder.claim_number(dec_value);
            break;
        }
        case ignite_type::NUMBER: {
            protocol::claim_type_and_scale(builder, ignite_type::NUMBER);
            builder.claim_number(value.get<big_integer>());
            break;
        }
        case ignite_type::DATE: {
            protocol::claim_type_and_scale(builder, ignite_type::DATE);
            builder.claim_date(value.get<ignite_date>());
            break;
        }
        case ignite_type::TIME: {
            protocol::claim_type_and_scale(builder, ignite_type::TIME);
            builder.claim_time(value.get<ignite_time>());
            break;
        }
        case ignite_type::DATETIME: {
            protocol::claim_type_and_scale(builder, ignite_type::DATETIME);
            builder.claim_date_time(value.get<ignite_date_time>());
            break;
        }
        case ignite_type::TIMESTAMP: {
            protocol::claim_type_and_scale(builder, ignite_type::TIMESTAMP);
            builder.claim_timestamp(value.get<ignite_timestamp>());
            break;
        }
        case ignite_type::PERIOD: {
            protocol::claim_type_and_scale(builder, ignite_type::PERIOD);
            builder.claim_period(value.get<ignite_period>());
            break;
        }
        case ignite_type::DURATION: {
            protocol::claim_type_and_scale(builder, ignite_type::DURATION);
            builder.claim_duration(value.get<ignite_duration>());
            break;
        }
        case ignite_type::BITMASK: {
            protocol::claim_type_and_scale(builder, ignite_type::BITMASK);
            builder.claim_varlen(value.get<bit_array>().get_raw());
            break;
        }
        default:
            throw ignite_error("Unsupported type: " + std::to_string(int(value.get_type())));
    }
}

void append_primitive_with_type(binary_tuple_builder &builder, const primitive &value) {
    if (value.is_null()) {
        builder.append_null(); // Type.
        builder.append_null(); // Scale.
        builder.append_null(); // Value.
        return;
    }

    switch (value.get_type()) {
        case ignite_type::BOOLEAN: {
            protocol::append_type_and_scale(builder, ignite_type::INT8);
            builder.append_bool(value.get<bool>());
            break;
        }
        case ignite_type::INT8: {
            protocol::append_type_and_scale(builder, ignite_type::INT8);
            builder.append_int8(value.get<std::int8_t>());
            break;
        }
        case ignite_type::INT16: {
            protocol::append_type_and_scale(builder, ignite_type::INT16);
            builder.append_int16(value.get<std::int16_t>());
            break;
        }
        case ignite_type::INT32: {
            protocol::append_type_and_scale(builder, ignite_type::INT32);
            builder.append_int32(value.get<std::int32_t>());
            break;
        }
        case ignite_type::INT64: {
            protocol::append_type_and_scale(builder, ignite_type::INT64);
            builder.append_int64(value.get<std::int64_t>());
            break;
        }
        case ignite_type::FLOAT: {
            protocol::append_type_and_scale(builder, ignite_type::FLOAT);
            builder.append_float(value.get<float>());
            break;
        }
        case ignite_type::DOUBLE: {
            protocol::append_type_and_scale(builder, ignite_type::DOUBLE);
            builder.append_double(value.get<double>());
            break;
        }
        case ignite_type::UUID: {
            protocol::append_type_and_scale(builder, ignite_type::UUID);
            builder.append_uuid(value.get<uuid>());
            break;
        }
        case ignite_type::STRING: {
            protocol::append_type_and_scale(builder, ignite_type::STRING);
            builder.append_varlen(value.get<std::string>());
            break;
        }
        case ignite_type::BYTE_ARRAY: {
            protocol::append_type_and_scale(builder, ignite_type::BYTE_ARRAY);
            auto &data = value.get<std::vector<std::byte>>();
            builder.append_varlen(data);
            break;
        }
        case ignite_type::DECIMAL: {
            const auto &dec_value = value.get<big_decimal>();
            protocol::append_type_and_scale(builder, ignite_type::DECIMAL, dec_value.get_scale());
            builder.append_number(dec_value);
            break;
        }
        case ignite_type::NUMBER: {
            protocol::append_type_and_scale(builder, ignite_type::NUMBER);
            builder.append_number(value.get<big_integer>());
            break;
        }
        case ignite_type::DATE: {
            protocol::append_type_and_scale(builder, ignite_type::DATE);
            builder.append_date(value.get<ignite_date>());
            break;
        }
        case ignite_type::TIME: {
            protocol::append_type_and_scale(builder, ignite_type::TIME);
            builder.append_time(value.get<ignite_time>());
            break;
        }
        case ignite_type::DATETIME: {
            protocol::append_type_and_scale(builder, ignite_type::DATETIME);
            builder.append_date_time(value.get<ignite_date_time>());
            break;
        }
        case ignite_type::TIMESTAMP: {
            protocol::append_type_and_scale(builder, ignite_type::TIMESTAMP);
            builder.append_timestamp(value.get<ignite_timestamp>());
            break;
        }
        case ignite_type::PERIOD: {
            protocol::append_type_and_scale(builder, ignite_type::PERIOD);
            builder.append_period(value.get<ignite_period>());
            break;
        }
        case ignite_type::DURATION: {
            protocol::append_type_and_scale(builder, ignite_type::DURATION);
            builder.append_duration(value.get<ignite_duration>());
            break;
        }
        case ignite_type::BITMASK: {
            protocol::append_type_and_scale(builder, ignite_type::BITMASK);
            builder.append_varlen(value.get<bit_array>().get_raw());
            break;
        }
        default:
            throw ignite_error("Unsupported type: " + std::to_string(int(value.get_type())));
    }
}

primitive read_next_column(binary_tuple_parser &parser, ignite_type typ, std::int32_t scale) {
    auto val = parser.get_next();
    if (val.empty())
        return {};

    switch (typ) {
        case ignite_type::BOOLEAN:
            return binary_tuple_parser::get_bool(val);
        case ignite_type::INT8:
            return binary_tuple_parser::get_int8(val);
        case ignite_type::INT16:
            return binary_tuple_parser::get_int16(val);
        case ignite_type::INT32:
            return binary_tuple_parser::get_int32(val);
        case ignite_type::INT64:
            return binary_tuple_parser::get_int64(val);
        case ignite_type::FLOAT:
            return binary_tuple_parser::get_float(val);
        case ignite_type::DOUBLE:
            return binary_tuple_parser::get_double(val);
        case ignite_type::UUID:
            return binary_tuple_parser::get_uuid(val);
        case ignite_type::STRING:
            return std::string(binary_tuple_parser::get_varlen(val));
        case ignite_type::BYTE_ARRAY:
            return std::vector<std::byte>(binary_tuple_parser::get_varlen(val));
        case ignite_type::DECIMAL:
            return binary_tuple_parser::get_decimal(val, scale);
        case ignite_type::NUMBER:
            return binary_tuple_parser::get_number(val);
        case ignite_type::DATE:
            return binary_tuple_parser::get_date(val);
        case ignite_type::TIME:
            return binary_tuple_parser::get_time(val);
        case ignite_type::DATETIME:
            return binary_tuple_parser::get_date_time(val);
        case ignite_type::TIMESTAMP:
            return binary_tuple_parser::get_timestamp(val);
        case ignite_type::PERIOD:
            return binary_tuple_parser::get_period(val);
        case ignite_type::DURATION:
            return binary_tuple_parser::get_duration(val);
        case ignite_type::BITMASK:
            return bit_array(binary_tuple_parser::get_varlen(val));
        default:
            throw ignite_error("Type with id " + std::to_string(int(typ)) + " is not yet supported");
    }
}

ignite_tuple concat(const ignite_tuple &left, const ignite_tuple &right) {
    // TODO: IGNITE-18855 eliminate unnecessary tuple transformation;

    ignite_tuple res(left.column_count() + right.column_count());
    res.m_pairs.assign(left.m_pairs.begin(), left.m_pairs.end());
    res.m_indices.insert(left.m_indices.begin(), left.m_indices.end());

    for (const auto &pair : right.m_pairs) {
        res.m_pairs.emplace_back(pair);
        res.m_indices.emplace(ignite_tuple::parse_name(pair.first), res.m_pairs.size() - 1);
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
