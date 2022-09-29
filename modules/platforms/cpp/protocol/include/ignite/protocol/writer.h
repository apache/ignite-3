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
#include <functional>
#include <memory>
#include <string_view>

#include <msgpack.h>

#include "common/Types.h"
#include "ignite/protocol/buffer_adapter.h"

namespace ignite::protocol {

/**
 * Writer.
 */
class Writer {
public:
    // Default
    ~Writer() = default;

    // Deleted
    Writer() = delete;
    Writer(Writer &&) = delete;
    Writer(const Writer &) = delete;
    Writer &operator=(Writer &&) = delete;
    Writer &operator=(const Writer &) = delete;

    /**
     * Write message to buffer.
     *
     * @param buffer Buffer to use.
     * @param script Function.
     */
    static void writeMessageToBuffer(BufferAdapter &buffer, const std::function<void(Writer &)> &script);

    /**
     * Constructor.
     *
     * @param buffer Buffer.
     */
    explicit Writer(BufferAdapter &buffer)
        : m_buffer(buffer)
        , m_packer(msgpack_packer_new(&m_buffer, writeCallback), msgpack_packer_free) {
        assert(m_packer.get());
    }

    /**
     * Write int value.
     *
     * @param value Value to write.
     */
    void write(int8_t value) { msgpack_pack_int8(m_packer.get(), value); }

    /**
     * Write int value.
     *
     * @param value Value to write.
     */
    void write(int16_t value) { msgpack_pack_int16(m_packer.get(), value); }

    /**
     * Write int value.
     *
     * @param value Value to write.
     */
    void write(int32_t value) { msgpack_pack_int32(m_packer.get(), value); }

    /**
     * Write int value.
     *
     * @param value Value to write.
     */
    void write(int64_t value) { msgpack_pack_int64(m_packer.get(), value); }

    /**
     * Write string value.
     *
     * @param value Value to write.
     */
    void write(std::string_view value) { msgpack_pack_str_with_body(m_packer.get(), value.data(), value.size()); }

    /**
     * Write empty binary data.
     */
    void writeBinaryEmpty() { msgpack_pack_bin(m_packer.get(), 0); }

    /**
     * Write empty map.
     */
    void writeMapEmpty() { msgpack_pack_map(m_packer.get(), 0); }

private:
    /**
     * Write callback.
     *
     * @param data Pointer to the instance.
     * @param buf Data to write.
     * @param len Data length.
     * @return Write result.
     */
    static int writeCallback(void *data, const char *buf, size_t len);

    /** Buffer adapter. */
    BufferAdapter &m_buffer;

    /** Packer. */
    std::unique_ptr<msgpack_packer, void (*)(msgpack_packer *)> m_packer;
};

} // namespace ignite::protocol
