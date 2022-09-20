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

#include "BinaryTupleParser.h"
#include "common/platform.h"

#include "common/bytes.h"

#include <cassert>
#include <cstring>
#include <stdexcept>

namespace ignite {

namespace {

template <typename T>
T loadBytesAs(bytes_view bytes, std::size_t offset = 0) noexcept {
    return bytes::load<Endian::LITTLE, T>(bytes.data() + offset);
}

Date loadBytesAsDate(bytes_view bytes) {
    std::int32_t date = loadBytesAs<std::uint16_t>(bytes);
    date |= std::int32_t(loadBytesAs<std::int8_t>(bytes, 2)) << 16;

    std::int32_t day = date & 31;
    std::int32_t month = (date >> 5) & 15;
    std::int32_t year = (date >> 9); // Sign matters.

    return Date(year, month, day);
}

Time loadBytesAsTime(bytes_view bytes) {
    uint64_t time = loadBytesAs<std::uint32_t>(bytes);

    int nano;
    switch (bytes.size()) {
        case 4:
            nano = ((int)time & ((1 << 10) - 1)) * 1000 * 1000;
            time >>= 10;
            break;
        case 5:
            time |= std::uint64_t(loadBytesAs<std::uint8_t>(bytes, 4)) << 32;
            nano = ((int)time & ((1 << 20) - 1)) * 1000;
            time >>= 20;
            break;
        case 6:
            time |= std::uint64_t(loadBytesAs<std::uint16_t>(bytes, 4)) << 32;
            nano = ((int)time & ((1 << 30) - 1));
            time >>= 30;
            break;
    }

    int second = ((int)time) & 63;
    int minute = ((int)time >> 6) & 63;
    int hour = ((int)time >> 12) & 31;

    return Time(hour, minute, second, nano);
}

} // namespace

BinaryTupleParser::BinaryTupleParser(IntT numElements, bytes_view data)
    : binaryTuple(data)
    , elementCount(numElements)
    , elementIndex(0)
    , hasNullmap(false) {

    BinaryTupleHeader header;
    header.flags = binaryTuple[0];

    entrySize = header.getVarLenEntrySize();
    hasNullmap = header.getNullMapFlag();

    size_t nullmapSize = 0;
    if (hasNullmap) {
        nullmapSize = BinaryTupleSchema::getNullMapSize(elementCount);
    }

    size_t tableSize = entrySize * elementCount;
    nextEntry = binaryTuple.data() + BinaryTupleHeader::SIZE + nullmapSize;
    valueBase = nextEntry + tableSize;

    nextValue = valueBase;

    // Fix tuple size if needed.
    uint64_t offset = 0;
    static_assert(platform::ByteOrder::littleEndian);
    memcpy(&offset, nextEntry + tableSize - entrySize, entrySize);
    const std::byte *tupleEnd = valueBase + offset;
    const std::byte *currentEnd = &(*binaryTuple.end());
    if (currentEnd > tupleEnd) {
        binaryTuple.remove_suffix(currentEnd - tupleEnd);
    }
}

element_view BinaryTupleParser::getNext() {
    assert(numParsedElements() < numElements());

    ++elementIndex;

    uint64_t offset = 0;
    static_assert(platform::ByteOrder::littleEndian);
    memcpy(&offset, nextEntry, entrySize);
    nextEntry += entrySize;

    const std::byte *value = nextValue;
    nextValue = valueBase + offset;

    const size_t length = nextValue - value;
    if (length == 0 && hasNullmap && BinaryTupleSchema::hasNull(binaryTuple, elementIndex - 1)) {
        return {};
    }

    return bytes_view(value, length);
}

tuple_view BinaryTupleParser::parse(IntT num) {
    assert(elementIndex == 0);

    if (num == NO_NUM) {
        num = numElements();
    }

    tuple_view tuple;
    tuple.reserve(num);
    while (elementIndex < num) {
        tuple.emplace_back(getNext());
    }

    return tuple;
}

key_tuple_view BinaryTupleParser::parseKey(IntT num) {
    assert(elementIndex == 0);

    if (num == NO_NUM) {
        num = numElements();
    }

    key_tuple_view key;
    key.reserve(num);
    while (elementIndex < num) {
        key.emplace_back(getNext().value());
    }

    return key;
}

std::int8_t BinaryTupleParser::getInt8(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return 0;
        case 1:
            return loadBytesAs<std::int8_t>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

std::int16_t BinaryTupleParser::getInt16(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return 0;
        case 1:
            return loadBytesAs<std::int8_t>(bytes);
        case 2:
            return loadBytesAs<std::int16_t>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

std::int32_t BinaryTupleParser::getInt32(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return 0;
        case 1:
            return loadBytesAs<std::int8_t>(bytes);
        case 2:
            return loadBytesAs<std::int16_t>(bytes);
        case 4:
            return loadBytesAs<std::int32_t>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

std::int64_t BinaryTupleParser::getInt64(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return 0;
        case 1:
            return loadBytesAs<std::int8_t>(bytes);
        case 2:
            return loadBytesAs<std::int16_t>(bytes);
        case 4:
            return loadBytesAs<std::int32_t>(bytes);
        case 8:
            return loadBytesAs<std::int64_t>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

float BinaryTupleParser::getFloat(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return 0.0f;
        case 4:
            return loadBytesAs<float>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

double BinaryTupleParser::getDouble(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return 0.0f;
        case 4:
            return loadBytesAs<float>(bytes);
        case 8:
            return loadBytesAs<double>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

BigInteger BinaryTupleParser::getNumber(bytes_view bytes) {
    return BigInteger(bytes.data(), bytes.size());
}

uuid BinaryTupleParser::getUuid(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return uuid();
        case 16:
            return uuid(loadBytesAs<std::int64_t>(bytes), loadBytesAs<std::int64_t>(bytes, 8));
        default:
            throw std::out_of_range("Bad element size");
    }
}

Date BinaryTupleParser::getDate(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return Date();
        case 3:
            return loadBytesAsDate(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

Time BinaryTupleParser::getTime(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return Time();
        case 4:
        case 5:
        case 6:
            return loadBytesAsTime(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

DateTime BinaryTupleParser::getDateTime(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return DateTime();
        case 7:
        case 8:
        case 9:
            return DateTime(loadBytesAsDate(bytes), loadBytesAsTime(bytes.substr(3)));
        default:
            throw std::out_of_range("Bad element size");
    }
}

Timestamp BinaryTupleParser::getTimestamp(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return Timestamp();
        case 8: {
            std::int64_t seconds = loadBytesAs<std::int64_t>(bytes);
            return Timestamp(seconds, 0);
        }
        case 12: {
            std::int64_t seconds = loadBytesAs<std::int64_t>(bytes);
            std::int32_t nanos = loadBytesAs<std::int32_t>(bytes);
            return Timestamp(seconds, nanos);
        }
        default:
            throw std::out_of_range("Bad element size");
    }
}

} // namespace ignite
