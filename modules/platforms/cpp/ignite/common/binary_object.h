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

#include "primitive.h"

namespace ignite {

/**
 * Ignite binary_object type.
 */
class binary_object {
public:
    // Default
    binary_object() = default;

    /**
     * Primitive constructor.
     *
     * @param value Primitive type value.
     */
    binary_object(primitive value) {} // NOLINT(google-explicit-constructor)

    /**
     * Get underlying primitive value.
     *
     * @throw ignite_error If the packed value is not a primitive.
     * @return Primitive value.
     */
    primitive get_primitive() const { return m_value; }

private:
    /** Value. */
    primitive m_value;
};

} // namespace ignite
