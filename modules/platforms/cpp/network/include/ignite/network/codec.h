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

#include <common/ignite_error.h>
#include <common/factory.h>

#include <ignite/network/data_buffer.h>

namespace ignite::network
{

/**
 * Codec class.
 * Encodes and decodes data.
 */
class Codec
{
public:
    // Default
    virtual ~Codec() = default;

    /**
     * Encode provided data.
     *
     * @param data Data to encode.
     * @return Encoded data. Returning null is ok.
     *
     * @throw IgniteError on error.
     */
    virtual DataBuffer encode(DataBuffer& data) = 0;

    /**
     * Decode provided data.
     *
     * @param data Data to decode.
     * @return Decoded data. Returning null means data is not yet ready.
     *
     * @throw IgniteError on error.
     */
    virtual DataBuffer decode(DataBuffer& data) = 0;
};

} // namespace ignite::network
