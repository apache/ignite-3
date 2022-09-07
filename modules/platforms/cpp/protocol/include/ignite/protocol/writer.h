/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

#include <msgpack.h>

#include "common/Types.h"
#include "ignite/protocol/buffer.h"

namespace ignite::protocol
{

/**
 * Writer.
 */
class Writer
{
public:
    // Deleted
    Writer() = delete;
    Writer(Writer&&) = delete;
    Writer(const Writer&) = delete;
    Writer& operator=(Writer&&) = delete;
    Writer& operator=(const Writer&) = delete;

    /**
     * Write message to buffer.
     *
     * @param buffer Buffer to use.
     * @param script Function.
     */
    static void writeMessageToBuffer(Buffer& buffer, const std::function<void(Writer&)>& script);

    /**
     * Constructor.
     *
     * @param buffer Buffer.
     */
    explicit Writer(Buffer& buffer);

    /**
     * Destructor.
     */
    virtual ~Writer();

    /**
     * Write int value.
     *
     * @param value Value to write.
     */
    void write(int8_t value);

    /**
     * Write int value.
     *
     * @param value Value to write.
     */
    void write(int16_t value);

    /**
     * Write int value.
     *
     * @param value Value to write.
     */
    void write(int32_t value);

    /**
     * Write int value.
     *
     * @param value Value to write.
     */
    void write(int64_t value);

    /**
     * Write empty binary data.
     */
    void writeBinaryEmpty();

    /**
     * Write empty map.
     */
    void writeMapEmpty();

private:
    /** Buffer. */
    Buffer& m_buffer;

    /** Packer. */
    msgpack_packer* m_packer;
};

} // namespace ignite::protocol
