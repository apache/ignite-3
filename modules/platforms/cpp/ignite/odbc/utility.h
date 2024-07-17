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

#include <cstdint>
#include <string>

namespace ignite {

template<typename T>
T *get_pointer_with_offset(T *ptr, size_t offset) {
    auto *ptr_bytes = reinterpret_cast<std::uint8_t *>(ptr);
    return (T *) (ptr_bytes + offset);
}

/**
 * Copy string to buffer of the specific length.
 *
 * @param str String to copy data from.
 * @param buf Buffer to copy data to.
 * @param buffer_len Length of the buffer.
 * @return Length of the resulting string in buffer.
 */
size_t copy_string_to_buffer(const std::string &str, char *buf, std::size_t buffer_len);

/**
 * Convert SQL string buffer to std::string.
 *
 * @param sql_str SQL string buffer.
 * @param sql_str_len SQL string length.
 * @return Standard string containing the same data.
 */
std::string sql_string_to_string(const unsigned char *sql_str, std::int32_t sql_str_len);

} // namespace ignite
