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

#include <cassert>
#include <cstring>
#include <stdexcept>

namespace {

template <typename T>
T as(ignite::BytesView bytes) noexcept {
    T value;
    std::memcpy(&value, bytes.data(), sizeof(T));
    return value;
}

} // namespace

namespace ignite {

BinaryTupleParser::BinaryTupleParser(IntT numElements, BytesView data)
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

ElementView BinaryTupleParser::getNext() {
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

    return BytesView(value, length);
}

TupleView BinaryTupleParser::parse(IntT num) {
    assert(elementIndex == 0);

    if (num == NO_NUM) {
        num = numElements();
    }

    TupleView tuple;
    tuple.reserve(num);
    while (elementIndex < num) {
        tuple.emplace_back(getNext());
    }

    return tuple;
}

KeyView BinaryTupleParser::parseKey(IntT num) {
    assert(elementIndex == 0);

    if (num == NO_NUM) {
        num = numElements();
    }

    KeyView key;
    key.reserve(num);
    while (elementIndex < num) {
        key.emplace_back(getNext().value());
    }

    return key;
}

std::int8_t BinaryTupleParser::getInt8(BytesView bytes) {
    switch (bytes.size()) {
        case 0:
            return 0;
        case 1:
            return as<std::int8_t>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

std::int16_t BinaryTupleParser::getInt16(BytesView bytes) {
    switch (bytes.size()) {
        case 0:
            return 0;
        case 1:
            return as<std::int8_t>(bytes);
        case 2:
            return as<std::int16_t>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

std::int32_t BinaryTupleParser::getInt32(BytesView bytes) {
    switch (bytes.size()) {
        case 0:
            return 0;
        case 1:
            return as<std::int8_t>(bytes);
        case 2:
            return as<std::int16_t>(bytes);
        case 4:
            return as<std::int32_t>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

std::int64_t BinaryTupleParser::getInt64(BytesView bytes) {
    switch (bytes.size()) {
        case 0:
            return 0;
        case 1:
            return as<std::int8_t>(bytes);
        case 2:
            return as<std::int16_t>(bytes);
        case 4:
            return as<std::int32_t>(bytes);
        case 8:
            return as<std::int64_t>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

float BinaryTupleParser::getFloat(BytesView bytes) {
    switch (bytes.size()) {
        case 0:
            return 0.0f;
        case 4:
            return as<float>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

double BinaryTupleParser::getDouble(BytesView bytes) {
    switch (bytes.size()) {
        case 0:
            return 0.0f;
        case 4:
            return as<float>(bytes);
        case 8:
            return as<double>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

} // namespace ignite
