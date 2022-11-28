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
#include "ignite/common/uuid.h"

#include <string>

namespace ignite::detail {

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
    builder.claim_int32(std::int32_t(typ));
    builder.claim_int32(scale);
}

void claim_primitive_with_type(binary_tuple_builder &builder, const primitive &value) {
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

        case column_type::DECIMAL:
        case column_type::DATE:
        case column_type::TIME:
        case column_type::DATETIME:
        case column_type::TIMESTAMP:
        case column_type::BITMASK:
        case column_type::PERIOD:
        case column_type::DURATION:
        case column_type::NUMBER:
        default:
            throw ignite_error("Unsupported type: " + std::to_string(int(value.get_type())));
    }
}

void append_primitive_with_type(binary_tuple_builder &builder, const primitive &value) {
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

        case column_type::DECIMAL:
        case column_type::DATE:
        case column_type::TIME:
        case column_type::DATETIME:
        case column_type::TIMESTAMP:
        case column_type::BITMASK:
        case column_type::PERIOD:
        case column_type::DURATION:
        case column_type::NUMBER:
        default:
            throw ignite_error("Unsupported type: " + std::to_string(int(value.get_type())));
    }
}

} // namespace ignite::detail
