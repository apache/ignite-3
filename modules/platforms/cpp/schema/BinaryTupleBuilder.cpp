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

#include "BinaryTupleBuilder.h"
#include "BinaryTupleParser.h"

#include <stdexcept>
#include <string>

namespace ignite {

BinaryTupleBuilder::BinaryTupleBuilder(IntT elementCount) noexcept
    : elementCount(elementCount) {
}

void BinaryTupleBuilder::start() noexcept {
    elementIndex = 0;
    nullElements = 0;
    valueAreaSize = 0;
    entrySize = 0;
}

void BinaryTupleBuilder::layout() {
    assert(elementIndex == elementCount);

    BinaryTupleHeader header;

    size_t nullmapSize = 0;
    if (nullElements) {
        header.setNullMapFlag();
        nullmapSize = BinaryTupleSchema::getNullMapSize(elementCount);
    }

    entrySize = header.setVarLenEntrySize(valueAreaSize);

    std::size_t tableSize = entrySize * elementCount;

    binaryTuple.clear();
    binaryTuple.resize(BinaryTupleHeader::SIZE + nullmapSize + tableSize + valueAreaSize);

    binaryTuple[0] = header.flags;

    nextEntry = binaryTuple.data() + BinaryTupleHeader::SIZE + nullmapSize;
    valueBase = nextEntry + tableSize;
    nextValue = valueBase;

    elementIndex = 0;
}

SizeT BinaryTupleBuilder::sizeOf(DATA_TYPE type, BytesView bytes) {
    switch (type) {
        case DATA_TYPE::INT8:
            return sizeOfInt8(BinaryTupleParser::getInt8(bytes));
        case DATA_TYPE::INT16:
            return sizeOfInt16(BinaryTupleParser::getInt16(bytes));
        case DATA_TYPE::INT32:
            return sizeOfInt32(BinaryTupleParser::getInt32(bytes));
        case DATA_TYPE::INT64:
            return sizeOfInt64(BinaryTupleParser::getInt64(bytes));
        case DATA_TYPE::FLOAT:
            return sizeOfFloat(BinaryTupleParser::getFloat(bytes));
        case DATA_TYPE::DOUBLE:
            return sizeOfDouble(BinaryTupleParser::getDouble(bytes));
        case DATA_TYPE::STRING:
        case DATA_TYPE::BINARY:
            return static_cast<SizeT>(bytes.size());

        case DATA_TYPE::UUID:
        case DATA_TYPE::DATE:
        case DATA_TYPE::TIME:
        case DATA_TYPE::DATETIME:
        case DATA_TYPE::TIMESTAMP:
        case DATA_TYPE::BITMASK:
        case DATA_TYPE::NUMBER:
        case DATA_TYPE::DECIMAL:
            // TODO: support the types above with IGNITE-17401.
        default:
            throw std::logic_error("Unsupported type " + std::to_string(static_cast<int>(type)));
    }
}

void BinaryTupleBuilder::putBytes(BytesView bytes) {
    assert(elementIndex < elementCount);
    assert(nextValue + bytes.size() <= valueBase + valueAreaSize);
    std::memcpy(nextValue, bytes.data(), bytes.size());
    nextValue += bytes.size();
    appendEntry();
}

void BinaryTupleBuilder::putInt8(BytesView bytes) {
    SizeT size = sizeOfInt8(BinaryTupleParser::getInt8(bytes));
    assert(size <= bytes.size());
    putBytes(BytesView{bytes.data(), size});
}

void BinaryTupleBuilder::putInt16(BytesView bytes) {
    SizeT size = sizeOfInt16(BinaryTupleParser::getInt16(bytes));
    assert(size <= bytes.size());
    static_assert(platform::ByteOrder::littleEndian);
    putBytes(BytesView{bytes.data(), size});
}

void BinaryTupleBuilder::putInt32(BytesView bytes) {
    SizeT size = sizeOfInt32(BinaryTupleParser::getInt32(bytes));
    assert(size <= bytes.size());
    static_assert(platform::ByteOrder::littleEndian);
    putBytes(BytesView{bytes.data(), size});
}

void BinaryTupleBuilder::putInt64(BytesView bytes) {
    SizeT size = sizeOfInt64(BinaryTupleParser::getInt64(bytes));
    assert(size <= bytes.size());
    static_assert(platform::ByteOrder::littleEndian);
    putBytes(BytesView{bytes.data(), size});
}

void BinaryTupleBuilder::putFloat(BytesView bytes) {
    SizeT size = sizeOfFloat(BinaryTupleParser::getFloat(bytes));
    assert(size <= bytes.size());
    putBytes(BytesView{bytes.data(), size});
}

void BinaryTupleBuilder::putDouble(BytesView bytes) {
    double value = BinaryTupleParser::getDouble(bytes);
    SizeT size = sizeOfDouble(value);
    assert(size <= bytes.size());
    if (size != sizeof(float)) {
        putBytes(BytesView{bytes.data(), size});
    } else {
        float floatValue = static_cast<float>(value);
        putBytes(BytesView{reinterpret_cast<std::byte *>(&floatValue), sizeof(float)});
    }
}

void BinaryTupleBuilder::append(DATA_TYPE type, const BytesView &bytes) {
    switch (type) {
        case DATA_TYPE::INT8:
            return putInt8(bytes);
        case DATA_TYPE::INT16:
            return putInt16(bytes);
        case DATA_TYPE::INT32:
            return putInt32(bytes);
        case DATA_TYPE::INT64:
            return putInt64(bytes);
        case DATA_TYPE::FLOAT:
            return putFloat(bytes);
        case DATA_TYPE::DOUBLE:
            return putDouble(bytes);
        case DATA_TYPE::STRING:
        case DATA_TYPE::BINARY:
            return putBytes(bytes);

        case DATA_TYPE::UUID:
        case DATA_TYPE::DATE:
        case DATA_TYPE::TIME:
        case DATA_TYPE::DATETIME:
        case DATA_TYPE::TIMESTAMP:
        case DATA_TYPE::BITMASK:
        case DATA_TYPE::NUMBER:
        case DATA_TYPE::DECIMAL:
            // TODO: support the types above with IGNITE-17401.
        default:
            throw std::logic_error("Unsupported type " + std::to_string(static_cast<int>(type)));
    }
}

} // namespace ignite
