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

#include <algorithm>
#include <type_traits>

namespace ignite::detail {

template<typename D1, typename D2>
std::common_type_t<D1, D2> min(const D1& d1, const D2& d2) {
    return std::min<std::common_type_t<D1, D2>>(d1, d2);
}

template<typename D1, typename D2>
std::common_type_t<D1, D2> max(const D1& d1, const D2& d2) {
    return std::max<std::common_type_t<D1, D2>>(d1, d2);
}

} // namespace ignite::detail
