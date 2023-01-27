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

#include "ignite_type.h"

#include <stdexcept>
#include <string>

namespace ignite {

bool is_fixed_size_type(ignite_type t) {
    switch (t) {
        case ignite_type::BOOLEAN:
        case ignite_type::INT8:
        case ignite_type::INT16:
        case ignite_type::INT32:
        case ignite_type::INT64:
        case ignite_type::FLOAT:
        case ignite_type::DOUBLE:
        case ignite_type::UUID:
        case ignite_type::DATE:
        case ignite_type::TIME:
        case ignite_type::DATETIME:
        case ignite_type::TIMESTAMP:
        case ignite_type::PERIOD:
        case ignite_type::DURATION:
            return true;
        default:
            return false;
    }
}

data_size_t get_type_size(ignite_type t) {
    switch (t) {
        case ignite_type::BOOLEAN:
        case ignite_type::INT8:
            return 1;
        case ignite_type::INT16:
            return 2;
        case ignite_type::INT32:
            return 4;
        case ignite_type::INT64:
            return 8;
        case ignite_type::FLOAT:
            return 4;
        case ignite_type::DOUBLE:
            return 8;
        case ignite_type::UUID:
            return 16;
        case ignite_type::DATE:
            return 3;
        case ignite_type::TIME:
            return 6;
        case ignite_type::DATETIME:
            return 9;
        case ignite_type::TIMESTAMP:
        case ignite_type::PERIOD:
        case ignite_type::DURATION:
            return 12;
        case ignite_type::BITMASK:
        case ignite_type::NUMBER:
        case ignite_type::DECIMAL:
        case ignite_type::STRING:
        case ignite_type::BYTE_ARRAY:
            /* Only fixed size types are supported for now. */
            throw std::logic_error("Can't get size of variable-size type id " + std::to_string(static_cast<int>(t)));
        default:
            throw std::logic_error("Unsupported type id " + std::to_string(static_cast<int>(t)) + " in schema");
    }
}

} // namespace ignite
