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

#include "ignite/client/sql/result_set_metadata.h"
#include "ignite/common/config.h"

#include <memory>

namespace ignite {

namespace detail {
class result_set_impl;
}

/**
 * Query result set.
 */
class result_set {
public:
    // Default
    result_set() = default;

    /**
     * Constructor
     *
     * @param impl Implementation
     */
    explicit result_set(std::shared_ptr<detail::result_set_impl> impl)
            : m_impl(std::move(impl)) {}

    /**
     * Gets metadata.
     *
     * @return Metadata.
     */
    [[nodiscard]] IGNITE_API const result_set_metadata& metadata() const;

private:
    /** Implementation. */
    std::shared_ptr<detail::result_set_impl> m_impl;
};

} // namespace ignite
