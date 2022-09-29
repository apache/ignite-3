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
#include <cstdint>

#include <array>
#include <optional>

#include "common/Types.h"
#include "common/ignite_error.h"

#include "ignite/protocol/reader.h"

namespace ignite::protocol {
/** Magic bytes. */
static constexpr std::array<std::byte, 4> MAGIC_BYTES = {
    std::byte('I'), std::byte('G'), std::byte('N'), std::byte('I')};

/**
 * Read uint64 from bytes stream.
 *
 * @param data Data.
 * @param offset Offset.
 * @return Value
 */
inline uint64_t readUint64(const std::byte *data, size_t offset = 0) {
    // TODO IGNITE-17760: Replace read and write functions with ones from common/Bytes.h
    return (std::uint64_t(data[offset]) << 56) | (std::uint64_t(data[offset + 1]) << 48)
        | (std::uint64_t(data[offset + 2]) << 40) | (std::uint64_t(data[offset + 3]) << 32)
        | (std::uint64_t(data[offset + 4]) << 24) | (std::uint64_t(data[offset + 5]) << 16)
        | (std::uint64_t(data[offset + 6]) << 8) | std::uint64_t(data[offset + 7]);
}

/**
 * Read int64 from bytes stream.
 *
 * @param data Data.
 * @param offset Offset.
 * @return Value
 */
inline int64_t readInt64(const std::byte *data, size_t offset = 0) {
    return std::int64_t(readUint64(data, offset));
}

/**
 * Read uint32 from bytes stream.
 *
 * @param data Data.
 * @param offset Offset.
 * @return Value
 */
inline uint32_t readUint32(const std::byte *data, size_t offset = 0) {
    return (std::uint32_t(data[offset]) << 24) | (std::uint32_t(data[offset + 1]) << 16)
        | (std::uint32_t(data[offset + 2]) << 8) | std::uint32_t(data[offset + 3]);
}

/**
 * Read int32 from bytes stream.
 *
 * @param data Data.
 * @param offset Offset.
 * @return Value
 */
inline int32_t readInt32(const std::byte *data, size_t offset = 0) {
    return std::int32_t(readUint32(data, offset));
}

/**
 * Read uint16 from bytes stream.
 *
 * @param data Data.
 * @param offset Offset.
 * @return Value
 */
inline uint16_t readUint16(const std::byte *data, size_t offset = 0) {
    return std::uint16_t(data[offset + 1]) | (std::uint16_t(data[offset]) << 8);
}

/**
 * Read int16 from bytes stream.
 *
 * @param data Data.
 * @param offset Offset.
 * @return Value
 */
inline int16_t readInt16(const std::byte *data, size_t offset = 0) {
    return std::int16_t(readUint16(data, offset));
}

/**
 * Write uint64 to byte stream.
 *
 * @param value Value to write.
 * @param buffer Buffer.
 * @param offset Offset.
 * @return Value
 */
inline void writeUint64(uint64_t value, std::byte *buffer, size_t offset = 0) {
    buffer[offset] = std::byte((value & 0xFF000000'00000000) >> 56);
    buffer[offset + 1] = std::byte((value & 0x00FF0000'00000000) >> 48);
    buffer[offset + 2] = std::byte((value & 0x0000FF00'00000000) >> 40);
    buffer[offset + 3] = std::byte((value & 0x000000FF'00000000) >> 32);
    buffer[offset + 4] = std::byte((value & 0x00000000'FF000000) >> 24);
    buffer[offset + 5] = std::byte((value & 0x00000000'00FF0000) >> 16);
    buffer[offset + 6] = std::byte((value & 0x00000000'0000FF00) >> 8);
    buffer[offset + 7] = std::byte(value & 0x00000000'000000FF);
}

/**
 * Write int64 to byte stream.
 *
 * @param value Value to write.
 * @param buffer Buffer.
 * @param offset Offset.
 * @return Value
 */
inline void writeInt64(int64_t value, std::byte *buffer, size_t offset = 0) {
    return writeUint64(std::uint64_t(value), buffer, offset);
}

/**
 * Write uint32 to byte stream.
 *
 * @param value Value to write.
 * @param buffer Buffer.
 * @param offset Offset.
 * @return Value
 */
inline void writeUint32(uint32_t value, std::byte *buffer, size_t offset = 0) {
    buffer[offset] = std::byte((value & 0xFF000000) >> 24);
    buffer[offset + 1] = std::byte((value & 0x00FF0000) >> 16);
    buffer[offset + 2] = std::byte((value & 0x0000FF00) >> 8);
    buffer[offset + 3] = std::byte(value & 0x000000FF);
}

/**
 * Write int32 to byte stream.
 *
 * @param value Value to write.
 * @param buffer Buffer.
 * @param offset Offset.
 * @return Value
 */
inline void writeInt32(int32_t value, std::byte *buffer, size_t offset = 0) {
    return writeUint32(std::uint32_t(value), buffer, offset);
}

/**
 * Write uint16 to byte stream.
 *
 * @param value Value to write.
 * @param buffer Buffer.
 * @param offset Offset.
 * @return Value
 */
inline void writeUint16(uint16_t value, std::byte *buffer, size_t offset = 0) {
    buffer[offset] = std::byte((value & 0xFF00) >> 8);
    buffer[offset + 1] = std::byte(value & 0x00FF);
}

/**
 * Write int16 to byte stream.
 *
 * @param value Value to write.
 * @param buffer Buffer.
 * @param offset Offset.
 * @return Value
 */
inline void writeInt16(int16_t value, std::byte *buffer, size_t offset = 0) {
    return writeUint16(std::uint16_t(value), buffer, offset);
}

/**
 * Make random GUID.
 *
 * @return Random GUID instance.
 */
ignite::Guid makeRandomGuid();

/**
 * Read error.
 *
 * @param reader Reader.
 * @return Error.
 */
std::optional<IgniteError> readError(protocol::Reader &reader);

} // namespace ignite::protocol
