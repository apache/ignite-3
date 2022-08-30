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

/**
 * Macro SWITCH_WIN_OTHER that uses first option on Windows and second on any other OS.
 */
#ifdef WIN32
#   define SWITCH_WIN_OTHER(x, y) (x)
#else
#   define SWITCH_WIN_OTHER(x, y) (y)
#endif

namespace ignite::platform
{

/**
 * Byte order utility class.
 */
class ByteOrder
{
private:
    static constexpr uint32_t fourBytes = 0x01020304;
    static constexpr uint8_t lesserByte = (const uint8_t&)fourBytes;

public:
    ByteOrder() = delete;

    static constexpr bool littleEndian = lesserByte == 0x04;
    static constexpr bool bigEndian = lesserByte == 0x01;

    static_assert(littleEndian || bigEndian, "Unknown byte order");
};

} // ignite::platform
