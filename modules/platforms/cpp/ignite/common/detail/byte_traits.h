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

#include <cstddef>
#include <cstring>
#include <string>

namespace ignite::detail {

struct byte_traits : std::char_traits<char> {
    using char_type = std::byte;

    static inline void assign(char_type &a, const char_type &b) noexcept { a = b; }

    static inline bool eq(char_type a, char_type b) noexcept { return a == b; }

    static inline bool lt(char_type a, char_type b) noexcept { return a < b; }

    static inline int compare(const char_type *a, const char_type *b, std::size_t size) noexcept {
        return std::memcmp(a, b, size);
    }

    // We cannot determine length of the byte sequence only from the pointer, because there is no 'stop'-byte.
    // So implementing find functions we should not rely upon this implementation of length().
    static inline size_t length(const char_type *data) noexcept { return std::strlen(reinterpret_cast<const char *>(data)); }

    static inline const char_type *find(const char_type *data, std::size_t size, const char_type &value) noexcept {
        return static_cast<const char_type *>(std::memchr(data, static_cast<int>(value), size));
    }

    static inline char_type *move(char_type *dest, const char_type *src, std::size_t size) noexcept {
        return static_cast<char_type *>(std::memmove(dest, src, size));
    }

    static inline char_type *copy(char_type *dest, const char_type *src, std::size_t size) noexcept {
        return static_cast<char_type *>(std::memcpy(dest, src, size));
    }

    static inline char_type *assign(char_type *dest, std::size_t size, char_type value) noexcept {
        return static_cast<char_type *>(std::memset(dest, static_cast<int>(value), size));
    }

    static inline int_type not_eof(int_type value) noexcept { return eq_int_type(value, eof()) ? ~eof() : value; }

    static inline char_type to_char_type(int_type value) noexcept { return char_type(value); }

    static inline int_type to_int_type(char_type value) noexcept { return int_type(value); }

    static inline bool eq_int_type(int_type a, int_type b) noexcept { return a == b; }

    static inline int_type eof() noexcept { return int_type(EOF); }
};

} // namespace ignite::detail
