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

#include "ignite/common/bytes_view.h"
#include "ignite/common/detail/bytes.h"
#include "ignite/common/uuid.h"
#include "ignite/protocol/bitset_span.h"
#include "ignite/protocol/buffer_adapter.h"
#include "ignite/protocol/extension_types.h"

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <msgpack.h>
#include <string_view>

namespace ignite::protocol {

/**
 * Writer.
 */
class writer {
public:
    // Default
    ~writer() = default;

    // Deleted
    writer() = delete;
    writer(writer &&) = delete;
    writer(const writer &) = delete;
    writer &operator=(writer &&) = delete;
    writer &operator=(const writer &) = delete;

    /**
     * Constructor.
     *
     * @param buffer Buffer.
     */
    explicit writer(buffer_adapter &buffer)
        : m_buffer(buffer)
        , m_packer(msgpack_packer_new(&m_buffer, write_callback), msgpack_packer_free) {
        assert(m_packer.get());
    }

    /**
     * Write int value.
     *
     * @param value Value to write.
     */
    void write(std::int8_t value) { msgpack_pack_int8(m_packer.get(), value); }

    /**
     * Write int value.
     *
     * @param value Value to write.
     */
    void write(std::int16_t value) { msgpack_pack_int16(m_packer.get(), value); }

    /**
     * Write int value.
     *
     * @param value Value to write.
     */
    void write(std::int32_t value) { msgpack_pack_int32(m_packer.get(), value); }

    /**
     * Write int value.
     *
     * @param value Value to write.
     */
    void write(std::int64_t value) { msgpack_pack_int64(m_packer.get(), value); }

    /**
     * Write string value.
     *
     * @param value Value to write.
     */
    void write(std::string_view value) { msgpack_pack_str_with_body(m_packer.get(), value.data(), value.size()); }

    /**
     * Write UUID value.
     *
     * @param value Value to write.
     */
    void write(uuid value) {
        std::byte data[16];
        detail::bytes::store<detail::endian::LITTLE, std::int64_t>(&data[0], value.get_most_significant_bits());
        detail::bytes::store<detail::endian::LITTLE, std::int64_t>(&data[8], value.get_least_significant_bits());

        msgpack_pack_ext_with_body(m_packer.get(), &data, 16, std::int8_t(extension_type::UUID));
    }

    /**
     * Write nil value.
     */
    void write_nil() { msgpack_pack_nil(m_packer.get()); }

    /**
     * Write empty binary data.
     */
    void write_binary_empty() { msgpack_pack_bin(m_packer.get(), 0); }

    /**
     * Write binary data.
     *
     * @param data Binary data to pack.
     */
    void write_binary(bytes_view data) { msgpack_pack_bin_with_body(m_packer.get(), data.data(), data.size()); }

    /**
     * Write empty map.
     */
    void write_map_empty() { msgpack_pack_map(m_packer.get(), 0); }

    /**
     * Write map.
     *
     * @param values Map.
     */
    void write_map(const std::map<std::string, std::string> &values) {
        write(std::int32_t(values.size()));
        for (const auto &pair : values) {
            write(pair.first);
            write(pair.second);
        }
    }

    /**
     * Write bitset.
     *
     * @param data Bitset to write.
     */
    void write_bitset(bytes_view data) {
        msgpack_pack_ext_with_body(m_packer.get(), data.data(), data.size(), std::int8_t(extension_type::BITMASK));
    }

    /**
     * Write boolean.
     *
     * @param value Value to write.
     */
    void write_bool(bool value) {
        if (value) {
            msgpack_pack_true(m_packer.get());
        } else {
            msgpack_pack_false(m_packer.get());
        }
    }

private:
    /**
     * Write callback.
     *
     * @param data Pointer to the instance.
     * @param buf Data to write.
     * @param len Data length.
     * @return Write result.
     */
    static int write_callback(void *data, const char *buf, size_t len);

    /** Buffer adapter. */
    buffer_adapter &m_buffer;

    /** Packer. */
    std::unique_ptr<msgpack_packer, void (*)(msgpack_packer *)> m_packer;
};

/**
 * Write message to buffer.
 *
 * @param buffer Buffer to use.
 * @param func Function.
 */
inline void write_message_to_buffer(buffer_adapter &buffer, const std::function<void(writer &)> &func) {
    buffer.reserve_length_header();

    protocol::writer writer(buffer);
    func(writer);

    buffer.write_length_header();
}

} // namespace ignite::protocol
