// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

//

#include "hash_calculator.h"

#include "hash_utils.h"

namespace ignite::detail {

std::int32_t hash_calculator::calc_hash(const primitive& val, std::int32_t scale, std::int32_t precision) {
    auto type = val.get_type();

    switch (type) {
        case ignite_type::BOOLEAN:
            return hash(val.get<bool>());
        case ignite_type::NIL:
            return hash(static_cast<std::int8_t>(0));
        case ignite_type::INT8:
            return hash(val.get<std::int8_t>());
        case ignite_type::INT16:
            return hash(val.get<std::int16_t>());
        case ignite_type::INT32:
            return hash(val.get<std::int32_t>());
        case ignite_type::INT64:
            return hash(val.get<std::int64_t>());
        case ignite_type::FLOAT:
            return hash(val.get<float>());
        case ignite_type::DOUBLE:
            return hash(val.get<double>());
        case ignite_type::DECIMAL:
            return hash(val.get<big_decimal>(), scale);
        case ignite_type::DATE:
            return hash(val.get<ignite_date>());
        case ignite_type::TIME:
            return hash(val.get<ignite_time>(), precision);
        case ignite_type::DATETIME:
            return hash(val.get<ignite_date_time>());
        case ignite_type::TIMESTAMP:
            return hash(val.get<ignite_timestamp>(), precision);
        case ignite_type::UUID:
            return hash(val.get<uuid>());
        case ignite_type::STRING:
            return hash(val.get<std::string>());
        case ignite_type::BYTE_ARRAY:
            return hash(val.get<std::vector<std::byte>>());
        case ignite_type::PERIOD:
        case ignite_type::DURATION:
        case ignite_type::UNDEFINED:
        default:
            throw ignite_error(
                error::code::INTERNAL,
                "Can't calculate hash value for type = " + std::to_string(static_cast<int>(type))
            );
    }
}

void hash_calculator::append(const primitive &val, std::int32_t scale, std::int32_t precision) {
    auto h = calc_hash(val, scale, precision);

    m_state = hash32(m_state, h);
}
} // namespace ignite::detail