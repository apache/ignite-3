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

#include "ignite/odbc/utility.h"
#include "ignite/odbc/system/odbc_constants.h"

#include <algorithm>
#include <cstring>

namespace ignite {

size_t copy_string_to_buffer(const std::string &str, char *buf, std::size_t buffer_len) {
    if (!buf || !buffer_len)
        return 0;

    size_t bytes_to_copy = std::min(str.size(), static_cast<size_t>(buffer_len - 1));

    memcpy(buf, str.data(), bytes_to_copy);
    buf[bytes_to_copy] = 0;

    return bytes_to_copy;
}

std::string sql_string_to_string(const unsigned char *sql_str, std::int32_t sql_str_len) {
    std::string res;

    const char *sql_str_c = reinterpret_cast<const char *>(sql_str);

    if (!sql_str || !sql_str_len)
        return res;

    if (sql_str_len == SQL_NTS)
        res.assign(sql_str_c);
    else if (sql_str_len > 0)
        res.assign(sql_str_c, sql_str_len);

    while (!res.empty() && res.back() == 0)
        res.pop_back();

    return res;
}

} // namespace ignite
