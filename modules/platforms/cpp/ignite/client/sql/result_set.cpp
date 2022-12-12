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

#include "ignite/client/sql/result_set.h"
#include "ignite/client/detail/sql/result_set_impl.h"

namespace ignite {

const result_set_metadata& result_set::metadata() const {
    return m_impl->metadata();
}

bool result_set::has_rowset() const {
    return m_impl->has_rowset();
}

std::int64_t result_set::affected_rows() const {
    return m_impl->affected_rows();
}

bool result_set::was_applied() const {
    return m_impl->was_applied();
}

} // namespace ignite
