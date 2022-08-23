/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "DataType.h"

#include <stdexcept>

namespace ignite {

bool isFixedSizeType(DATA_TYPE t) {
    switch (t) {
        case DATA_TYPE::INT8:
        case DATA_TYPE::INT16:
        case DATA_TYPE::INT32:
        case DATA_TYPE::INT64:
        case DATA_TYPE::FLOAT:
        case DATA_TYPE::DOUBLE:
        case DATA_TYPE::UUID:
        case DATA_TYPE::DATE:
        case DATA_TYPE::TIME:
        case DATA_TYPE::DATETIME:
        case DATA_TYPE::TIMESTAMP:
            return true;
        default:
            return false;
    }
}

SizeT getTypeSize(DATA_TYPE t) {
    switch (t) {
        case DATA_TYPE::INT8:
            return 1;
        case DATA_TYPE::INT16:
            return 2;
        case DATA_TYPE::INT32:
            return 4;
        case DATA_TYPE::INT64:
            return 8;
        case DATA_TYPE::FLOAT:
            return 4;
        case DATA_TYPE::DOUBLE:
            return 8;
        case DATA_TYPE::UUID:
            return 16;
        case DATA_TYPE::DATE:
            return 3;
        case DATA_TYPE::TIME:
            return 5;
        case DATA_TYPE::DATETIME:
            return 8;
        case DATA_TYPE::TIMESTAMP:
            return 10;
        case DATA_TYPE::BITMASK:
        case DATA_TYPE::NUMBER:
        case DATA_TYPE::DECIMAL:
        case DATA_TYPE::STRING:
        case DATA_TYPE::BINARY:
            /* Only fixed size types are supported for now. */
            throw std::logic_error("Can't get size of variable-size type id " + std::to_string(static_cast<int>(t)));
        default:
            throw std::logic_error("Unsupported type id " + std::to_string(static_cast<int>(t)) + " in schema");
    }
}

} // namespace ignite
