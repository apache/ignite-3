/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#pragma once

#include <cstddef>
#include <cstring>
#include <string>

namespace ignite::detail {

struct byte_traits : std::char_traits<char> {
    using char_type = std::byte;

    static void assign(char_type &a, const char_type &b) noexcept { a = b; }

    static bool eq(char_type a, char_type b) noexcept { return a == b; }

    static bool lt(char_type a, char_type b) noexcept { return a < b; }

    static int compare(const char_type *a, const char_type *b, std::size_t size) noexcept {
        return std::memcmp(a, b, size);
    }

    // We cannot determine length of the byte sequence only from the pointer, because there is no 'stop'-byte.
    // So implementing find functions we should not rely upon this implementation of length().
    static size_t length(const char_type *data) noexcept { return std::strlen(reinterpret_cast<const char *>(data)); }

    static const char_type *find(const char_type *data, std::size_t size, const char_type &value) noexcept {
        return static_cast<const char_type *>(std::memchr(data, static_cast<int>(value), size));
    }

    static char_type *move(char_type *dest, const char_type *src, std::size_t size) noexcept {
        return static_cast<char_type *>(std::memmove(dest, src, size));
    }

    static char_type *copy(char_type *dest, const char_type *src, std::size_t size) noexcept {
        return static_cast<char_type *>(std::memcpy(dest, src, size));
    }

    static char_type *assign(char_type *dest, std::size_t size, char_type value) noexcept {
        return static_cast<char_type *>(std::memset(dest, static_cast<int>(value), size));
    }

    static int_type not_eof(int_type value) noexcept { return eq_int_type(value, eof()) ? ~eof() : value; }

    static char_type to_char_type(int_type value) noexcept { return char_type(value); }

    static int_type to_int_type(char_type value) noexcept { return int_type(value); }

    static bool eq_int_type(int_type a, int_type b) noexcept { return a == b; }

    static int_type eof() noexcept { return int_type(EOF); }
};

} // namespace ignite::detail
