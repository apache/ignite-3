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

#pragma once

#include "primitive.h"

namespace ignite::detail {

/**
 * Calculator of hashes for all basic types.
 */
class hash_calculator {
public:
    /**
     * Calculates hash.
     * @param val Value of basic type.
     * @param scale Scale applies to some types (e.g. big_decimal), value ignored by other types.
     * @param precision Precision applies to some type (e.g. ignite_time) value ignored by other types.
     * @return Hash value.
     */
    static std::int32_t calc_hash(const primitive& val, std::int32_t scale, std::int32_t precision);

    /**
     * Appends hash of provided value into internal state.
     * @param val Value of basic type.
     * @param scale Scale applies to some types (e.g. big_decimal), value ignored by other types.
     * @param precision Precision applies to some type (e.g. ignite_time) value ignored by other types.
     */
    void append(const primitive& val, std::int32_t scale, std::int32_t precision);

    /**
     * Retrieves accumulated hash from internal state.
     * @return hash from internal state.
     */
    [[nodiscard]] std::int32_t result_hash() const {
        return m_state;
    }

private:
    /** Internal state. */
    std::int32_t m_state{0};
};

} // namespace ignite::detail
