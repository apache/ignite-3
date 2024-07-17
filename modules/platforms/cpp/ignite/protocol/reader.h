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

#include <ignite/common/bytes_view.h>
#include <ignite/common/ignite_error.h>
#include <ignite/common/uuid.h>
#include <ignite/protocol/utils.h>

#include <msgpack.h>

#include <cstdint>
#include <functional>

namespace ignite::protocol {

/**
 * Reader.
 */
class reader {
public:
    // Deleted
    reader() = delete;
    reader(reader &&) = delete;
    reader(const reader &) = delete;
    reader &operator=(reader &&) = delete;
    reader &operator=(const reader &) = delete;

    /**
     * Constructor.
     *
     * @param buffer Buffer.
     */
    explicit reader(bytes_view buffer);

    /**
     * Destructor.
     */
    ~reader() { msgpack_unpacked_destroy(&m_current_val); }

    /**
     * Read object of type T from msgpack stream.
     *
     * @tparam T Type of the object to read.
     * @return Object of type T.
     * @throw ignite_error if there is no object of specified type in the stream.
     */
    template<typename T>
    [[nodiscard]] T read_object() {
        check_data_in_stream();

        auto res = unpack_object<T>(m_current_val.data);
        next();

        return res;
    }

    /**
     * Read object of type T from msgpack stream.
     *
     * @tparam T Type of the object to read.
     * @return Object of type T or @c nullopt if there is object of other type in the stream.
     * @throw ignite_error if there is no data left in the stream.
     */
    template<typename T>
    [[nodiscard]] std::optional<T> try_read_object() {
        check_data_in_stream();

        auto res = try_unpack_object<T>(m_current_val.data);
        if (res)
            next();

        return res;
    }

    /**
     * Read object of type T from msgpack stream or nil.
     *
     * @tparam T Type of the object to read.
     * @return Object of type T or std::nullopt if there is nil in the stream.
     * @throw ignite_error if there is no object of specified type in the stream.
     */
    template<typename T>
    [[nodiscard]] std::optional<T> read_object_nullable() {
        if (try_read_nil())
            return std::nullopt;

        return read_object<T>();
    }

    /**
     * Read object of type T from msgpack stream or returns default value if the value in stream is nil.
     *
     * @tparam T Type of the object to read.
     * @param on_nil Object to be returned on nil.
     * @return Object of type T or @c on_nil if there is nil in stream.
     * @throw ignite_error if there is no object of specified type in the stream.
     */
    template<typename T>
    [[nodiscard]] T read_object_or_default(T &&on_nil) {
        if (try_read_nil())
            return std::forward<T>(on_nil);

        return read_object<T>();
    }

    /**
     * Read int8.
     *
     * @return Value.
     */
    [[nodiscard]] std::int8_t read_int8() { return read_object<std::int8_t>(); }

    /**
     * Read uint8.
     *
     * @return Value.
     */
    [[nodiscard]] std::uint8_t read_uint8() { return read_object<std::uint8_t>(); }

    /**
     * Read int16.
     *
     * @return Value.
     */
    [[nodiscard]] std::int16_t read_int16() { return read_object<std::int16_t>(); }

    /**
     * Read uint16.
     *
     * @return Value.
     */
    [[nodiscard]] std::uint16_t read_uint16() { return read_object<std::uint16_t>(); }

    /**
     * Read int32.
     *
     * @return Value.
     */
    [[nodiscard]] std::int32_t read_int32() { return read_object<std::int32_t>(); }

    /**
     * Read timestamp.
     *
     * @return Timestamp.
     */
    [[nodiscard]] ignite_timestamp read_timestamp() {
        auto seconds = read_int64();
        auto nanos = read_int32();
        return {seconds, nanos};
    }

    /**
     * Read timestamp or null.
     *
     * @return Timestamp or std::nullopt.
     */
    [[nodiscard]] std::optional<ignite_timestamp> read_timestamp_opt() {
        if (try_read_nil())
            return std::nullopt;

        return {read_timestamp()};
    }

    /**
     * Read array of int32.
     *
     * @return Value.
     */
    [[nodiscard]] std::vector<std::int32_t> read_int32_array() {
        auto length = read_int32();
        std::vector<std::int32_t> values(length);

        for (auto i = 0; i < length; i++) {
            values[i] = read_int32();
        }

        return values;
    }

    /**
     * Read array of int32.
     *
     * @return Value or nullopt.
     */
    [[nodiscard]] std::optional<std::vector<std::int32_t>> read_int32_array_nullable() {
        if (try_read_nil()) {
            return std::nullopt;
        }

        return read_int32_array();
    }

    /**
     * Read array of int64.
     *
     * @return Value or nullopt.
     */
    [[nodiscard]] std::vector<std::int64_t> read_int64_array() {
        auto length = read_int32();
        std::vector<std::int64_t> values(length);

        for (auto i = 0; i < length; i++) {
            values[i] = read_int64();
        }

        return values;
    }

    /**
     * Read array of int64.
     *
     * @return Value or nullopt.
     */
    [[nodiscard]] std::optional<std::vector<std::int64_t>> read_int64_array_nullable() {
        if (try_read_nil()) {
            return std::nullopt;
        }

        return read_int64_array();
    }

    /**
     * Read int32 or nullopt.
     *
     * @return Value or nullopt if the next value in stream is not integer.
     */
    [[nodiscard]] std::optional<std::int32_t> try_read_int32() { return try_read_object<std::int32_t>(); }

    /**
     * Read int32 or nullopt.
     *
     * @return Value or nullopt if the next value in stream is nil.
     */
    [[nodiscard]] std::optional<std::int32_t> read_int32_nullable() { return read_object_nullable<std::int32_t>(); }

    /**
     * Read uint8 or nullopt.
     *
     * @return Value or nullopt if the next value in stream is nil.
     */
    [[nodiscard]] std::optional<std::uint8_t> read_uint8_nullable() { return read_object_nullable<std::uint8_t>(); }

    /**
     * Read int16 or nullopt.
     *
     * @return Value or nullopt if the next value in stream is nil.
     */
    [[nodiscard]] std::optional<std::int16_t> read_int16_nullable() { return read_object_nullable<std::int16_t>(); }

    /**
     * Read int64 number.
     *
     * @return Value.
     */
    [[nodiscard]] std::int64_t read_int64() { return read_object<int64_t>(); }

    /**
     * Read bool.
     *
     * @return Value.
     */
    [[nodiscard]] bool read_bool() { return read_object<bool>(); }

    /**
     * Read string.
     *
     * @return String value.
     */
    [[nodiscard]] std::string read_string() { return read_object<std::string>(); }

    /**
     * Read string.
     *
     * @return String value or nullopt.
     */
    [[nodiscard]] std::optional<std::string> read_string_nullable() { return read_object_nullable<std::string>(); }

    /**
     * Read UUID.
     *
     * @return UUID value.
     */
    [[nodiscard]] uuid read_uuid() { return read_object<uuid>(); }

    /**
     * Read array.
     *
     * @return Binary data view.
     */
    [[nodiscard]] bytes_view read_binary() {
        auto res = unpack_binary(m_current_val.data);
        next();
        return res;
    }

    /**
     * If the next value is Nil, read it and move reader to the next position.
     *
     * @return @c true if the value was nil.
     */
    bool try_read_nil();

    /**
     * Skip next value.
     */
    void skip() { next(); }

    /**
     * Skip next value.
     */
    void skip(int count) {
        for (int i = 0; i < count; i++) {
            skip();
        }
    }

    /**
     * Position.
     *
     * @return Current position in memory.
     */
    [[nodiscard]] size_t position() const { return m_offset; }

private:
    /**
     * Move to the next value.
     */
    void next();

    /**
     * Check whether there is a data in stream and throw ignite_error if there is none.
     */
    void check_data_in_stream() const {
        if (m_move_res < 0)
            throw ignite_error("No more data in stream");
    }

    /** Buffer. */
    bytes_view m_buffer;

    /** Current value. */
    msgpack_unpacked m_current_val;

    /** Result of the last move operation. */
    msgpack_unpack_return m_move_res;

    /** Offset to next value. */
    std::size_t m_offset_next{0};

    /** Offset. */
    std::size_t m_offset{0};
};

} // namespace ignite::protocol
