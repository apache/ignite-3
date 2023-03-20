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

#include "ignite/client/detail/utils.h"
#include "ignite/common/bits.h"
#include "ignite/common/uuid.h"

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
            builder.claim_string(value.get<std::string>());
            break;
        case ignite_type::BINARY:
            builder.claim_bytes(value.get<std::vector<std::byte>>());
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
        case ignite_type::BITMASK:
            builder.claim_bytes(value.get<bit_array>().get_raw());
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
            builder.append_string(value.get<std::string>());
            break;
        case ignite_type::BINARY:
            builder.append_bytes(value.get<std::vector<std::byte>>());
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
        case ignite_type::BITMASK:
            builder.append_bytes(value.get<bit_array>().get_raw());
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
            builder.claim(std::nullopt);
    }

    builder.layout();
    for (std::int32_t i = 0; i < count; ++i) {
        const auto &col = sch.columns[i];
        auto col_idx = tuple.column_ordinal(col.name);

        if (col_idx >= 0)
            append_column(builder, col.type, tuple.get(col_idx), col.scale);
        else {
            builder.append(std::nullopt);
            no_value.set(std::size_t(i));
        }
    }

    return builder.build();
}

/**
 * Claim type and scale header for a value written in binary tuple.
 *
 * @param builder Tuple builder.
 * @param typ Type.
 * @param scale Scale.
 */
void claim_type_and_scale(binary_tuple_builder &builder, ignite_type typ, std::int32_t scale = 0) {
    builder.claim_int32(std::int32_t(typ));
    builder.claim_int32(scale);
}

/**
 * Append type and scale header for a value written in binary tuple.
 *
 * @param builder Tuple builder.
 * @param typ Type.
 * @param scale Scale.
 */
void append_type_and_scale(binary_tuple_builder &builder, ignite_type typ, std::int32_t scale = 0) {
    builder.append_int32(std::int32_t(typ));
    builder.append_int32(scale);
}

void claim_primitive_with_type(binary_tuple_builder &builder, const primitive &value) {
    if (value.is_null()) {
        builder.claim(std::nullopt); // Type.
        builder.claim(std::nullopt); // Scale.
        builder.claim(std::nullopt); // Value.
        return;
    }

    switch (value.get_type()) {
        case column_type::BOOLEAN: {
            claim_type_and_scale(builder, ignite_type::INT8);
            builder.claim_int8(1);
            break;
        }
        case column_type::INT8: {
            claim_type_and_scale(builder, ignite_type::INT8);
            builder.claim_int8(value.get<std::int8_t>());
            break;
        }
        case column_type::INT16: {
            claim_type_and_scale(builder, ignite_type::INT16);
            builder.claim_int16(value.get<std::int16_t>());
            break;
        }
        case column_type::INT32: {
            claim_type_and_scale(builder, ignite_type::INT32);
            builder.claim_int32(value.get<std::int32_t>());
            break;
        }
        case column_type::INT64: {
            claim_type_and_scale(builder, ignite_type::INT64);
            builder.claim_int64(value.get<std::int64_t>());
            break;
        }
        case column_type::FLOAT: {
            claim_type_and_scale(builder, ignite_type::FLOAT);
            builder.claim_float(value.get<float>());
            break;
        }
        case column_type::DOUBLE: {
            claim_type_and_scale(builder, ignite_type::DOUBLE);
            builder.claim_double(value.get<double>());
            break;
        }
        case column_type::UUID: {
            claim_type_and_scale(builder, ignite_type::UUID);
            builder.claim_uuid(value.get<uuid>());
            break;
        }
        case column_type::STRING: {
            claim_type_and_scale(builder, ignite_type::STRING);
            builder.claim_string(value.get<std::string>());
            break;
        }
        case column_type::BYTE_ARRAY: {
            claim_type_and_scale(builder, ignite_type::BINARY);
            auto &data = value.get<std::vector<std::byte>>();
            builder.claim(ignite_type::BINARY, data);
            break;
        }
        case column_type::DECIMAL: {
            const auto &dec_value = value.get<big_decimal>();
            claim_type_and_scale(builder, ignite_type::DECIMAL, dec_value.get_scale());
            builder.claim_number(dec_value);
            break;
        }
        case column_type::NUMBER: {
            claim_type_and_scale(builder, ignite_type::NUMBER);
            builder.claim_number(value.get<big_integer>());
            break;
        }
        case column_type::DATE: {
            claim_type_and_scale(builder, ignite_type::DATE);
            builder.claim_date(value.get<ignite_date>());
            break;
        }
        case column_type::TIME: {
            claim_type_and_scale(builder, ignite_type::TIME);
            builder.claim_time(value.get<ignite_time>());
            break;
        }
        case column_type::DATETIME: {
            claim_type_and_scale(builder, ignite_type::DATETIME);
            builder.claim_date_time(value.get<ignite_date_time>());
            break;
        }
        case column_type::TIMESTAMP: {
            claim_type_and_scale(builder, ignite_type::TIMESTAMP);
            builder.claim_timestamp(value.get<ignite_timestamp>());
            break;
        }
        case column_type::BITMASK: {
            claim_type_and_scale(builder, ignite_type::BITMASK);
            builder.claim_bytes(value.get<bit_array>().get_raw());
            break;
        }
        case column_type::PERIOD:
        case column_type::DURATION:
        default:
            throw ignite_error("Unsupported type: " + std::to_string(int(value.get_type())));
    }
}

void append_primitive_with_type(binary_tuple_builder &builder, const primitive &value) {
    if (value.is_null()) {
        builder.append(std::nullopt); // Type.
        builder.append(std::nullopt); // Scale.
        builder.append(std::nullopt); // Value.
        return;
    }

    switch (value.get_type()) {
        case column_type::BOOLEAN: {
            append_type_and_scale(builder, ignite_type::INT8);
            builder.append_int8(1);
            break;
        }
        case column_type::INT8: {
            append_type_and_scale(builder, ignite_type::INT8);
            builder.append_int8(value.get<std::int8_t>());
            break;
        }
        case column_type::INT16: {
            append_type_and_scale(builder, ignite_type::INT16);
            builder.append_int16(value.get<std::int16_t>());
            break;
        }
        case column_type::INT32: {
            append_type_and_scale(builder, ignite_type::INT32);
            builder.append_int32(value.get<std::int32_t>());
            break;
        }
        case column_type::INT64: {
            append_type_and_scale(builder, ignite_type::INT64);
            builder.append_int64(value.get<std::int64_t>());
            break;
        }
        case column_type::FLOAT: {
            append_type_and_scale(builder, ignite_type::FLOAT);
            builder.append_float(value.get<float>());
            break;
        }
        case column_type::DOUBLE: {
            append_type_and_scale(builder, ignite_type::DOUBLE);
            builder.append_double(value.get<double>());
            break;
        }
        case column_type::UUID: {
            append_type_and_scale(builder, ignite_type::UUID);
            builder.append_uuid(value.get<uuid>());
            break;
        }
        case column_type::STRING: {
            append_type_and_scale(builder, ignite_type::STRING);
            builder.append_string(value.get<std::string>());
            break;
        }
        case column_type::BYTE_ARRAY: {
            append_type_and_scale(builder, ignite_type::BINARY);
            auto &data = value.get<std::vector<std::byte>>();
            builder.append(ignite_type::BINARY, data);
            break;
        }
        case column_type::DECIMAL: {
            const auto &dec_value = value.get<big_decimal>();
            append_type_and_scale(builder, ignite_type::DECIMAL, dec_value.get_scale());
            builder.append_number(dec_value);
            break;
        }
        case column_type::NUMBER: {
            append_type_and_scale(builder, ignite_type::NUMBER);
            builder.append_number(value.get<big_integer>());
            break;
        }
        case column_type::DATE: {
            append_type_and_scale(builder, ignite_type::DATE);
            builder.append_date(value.get<ignite_date>());
            break;
        }
        case column_type::TIME: {
            append_type_and_scale(builder, ignite_type::TIME);
            builder.append_time(value.get<ignite_time>());
            break;
        }
        case column_type::DATETIME: {
            append_type_and_scale(builder, ignite_type::DATETIME);
            builder.append_date_time(value.get<ignite_date_time>());
            break;
        }
        case column_type::TIMESTAMP: {
            append_type_and_scale(builder, ignite_type::TIMESTAMP);
            builder.append_timestamp(value.get<ignite_timestamp>());
            break;
        }
        case column_type::BITMASK: {
            append_type_and_scale(builder, ignite_type::BITMASK);
            builder.append_bytes(value.get<bit_array>().get_raw());
            break;
        }
        case column_type::PERIOD:
        case column_type::DURATION:
        default:
            throw ignite_error("Unsupported type: " + std::to_string(int(value.get_type())));
    }
}

primitive read_next_column(binary_tuple_parser &parser, ignite_type typ, std::int32_t scale) {
    auto val_opt = parser.get_next();
    if (!val_opt)
        return {};

    auto val = val_opt.value();

    switch (typ) {
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
            return std::string(reinterpret_cast<const char *>(val.data()), val.size());
        case ignite_type::BINARY:
            return std::vector<std::byte>(val);
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
        case ignite_type::BITMASK:
            return bit_array(val);
        default:
            throw ignite_error("Type with id " + std::to_string(int(typ)) + " is not yet supported");
    }
}

primitive read_next_column(binary_tuple_parser &parser, column_type typ, std::int32_t scale) {
    auto val_opt = parser.get_next();
    if (!val_opt)
        return {};

    auto val = val_opt.value();

    switch (typ) {
        case column_type::BOOLEAN:
            return binary_tuple_parser::get_int8(val) != 0;
        case column_type::INT8:
            return binary_tuple_parser::get_int8(val);
        case column_type::INT16:
            return binary_tuple_parser::get_int16(val);
        case column_type::INT32:
            return binary_tuple_parser::get_int32(val);
        case column_type::INT64:
            return binary_tuple_parser::get_int64(val);
        case column_type::FLOAT:
            return binary_tuple_parser::get_float(val);
        case column_type::DOUBLE:
            return binary_tuple_parser::get_double(val);
        case column_type::UUID:
            return binary_tuple_parser::get_uuid(val);
        case column_type::STRING:
            return std::string(reinterpret_cast<const char *>(val.data()), val.size());
        case column_type::BYTE_ARRAY:
            return std::vector<std::byte>(val);
        case column_type::DECIMAL:
            return binary_tuple_parser::get_decimal(val, scale);
        case column_type::NUMBER:
            return binary_tuple_parser::get_number(val);
        case column_type::DATE:
            return binary_tuple_parser::get_date(val);
        case column_type::TIME:
            return binary_tuple_parser::get_time(val);
        case column_type::DATETIME:
            return binary_tuple_parser::get_date_time(val);
        case column_type::TIMESTAMP:
            return binary_tuple_parser::get_timestamp(val);
        case column_type::BITMASK:
            return bit_array(val);
        case column_type::PERIOD:
        case column_type::DURATION:
            // TODO: IGNITE-18745 Support period and duration types
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
