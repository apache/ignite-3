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

#include "Config.h"
#include "Bits.h"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <utility>

namespace ignite {

/**
 * This is the analogue to std::vector. It is needed to be used in exported classes as
 * we can't export standard library classes.
 *
 * TODO: Modernize this code to C++17.
 *
 * TODO: The whole thing is borrowed from ignite-2. Investigate if we really need it
 * or std::vector can work for us.
 *
 * TODO: This class uses STL-like conventions but all the names are capitalized. So it
 * is not really compatible with STL. Probably it should be either made fully compatible
 * or be simplified.
 */
template <typename T>
class IGNITE_API Vector {
public:
    using ValueType = T;
    using AllocatorType = std::allocator<T>;

    typedef typename AllocatorType::size_type SizeType;
    typedef typename AllocatorType::pointer PointerType;
    typedef typename AllocatorType::const_pointer ConstPointerType;
    typedef typename AllocatorType::reference ReferenceType;
    typedef typename AllocatorType::const_reference ConstReferenceType;

    /**
     * Default constructor.
     *
     * Constructs zero-size and zero-capacity array.
     */
    Vector(const AllocatorType &allocator = AllocatorType())
        : alloc(allocator)
        , size(0)
        , capacity(0)
        , data(0) {
        // No-op.
    }

    /**
     * Constructor.
     * Constructs empty array with the specified capacity.
     *
     * @param len Array length.
     * @param alloc Allocator.
     */
    Vector(SizeType len, const AllocatorType &allocator = AllocatorType())
        : alloc(allocator)
        , size(0)
        , capacity(getCapacityForSize(len))
        , data(alloc.allocate(capacity)) {
        // No-op.
    }

    /**
     * Raw array constructor.
     *
     * @param arr Raw array.
     * @param len Array length in elements.
     */
    Vector(ConstPointerType arr, SizeType len, const AllocatorType &allocator = AllocatorType())
        : alloc(allocator)
        , size(0)
        , capacity(0)
        , data(0) {
        Assign(arr, len);
    }

    /**
     * Copy constructor.
     *
     * @param other Other instance.
     */
    Vector(const Vector<T> &other)
        : alloc()
        , size(0)
        , capacity(0)
        , data(0) {
        Assign(other);
    }

    /**
     * Destructor.
     */
    ~Vector() {
        for (PointerType it = data; it != data + size; ++it)
            alloc.destroy(it);

        alloc.deallocate(data, capacity);
    }

    /**
     * Assignment operator.
     *
     * @param other Other instance.
     * @return Reference to this instance.
     */
    Vector<T> &operator=(const Vector<T> &other) {
        Assign(other);

        return *this;
    }

    /**
     * Assign new value to the array.
     *
     * @param other Another array instance.
     */
    void Assign(const Vector<T> &other) {
        if (this != &other) {
            alloc = other.alloc;

            Assign(other.GetData(), other.GetSize());
        }
    }

    /**
     * Assign new value to the array.
     *
     * @param src Raw array.
     * @param len Array length in elements.
     */
    void Assign(ConstPointerType src, SizeType len) {
        for (PointerType it = data; it != data + size; ++it)
            alloc.destroy(it);

        if (capacity < len) {
            alloc.deallocate(data, capacity);

            capacity = getCapacityForSize(len);
            data = alloc.allocate(capacity);
        }

        size = len;

        for (SizeType i = 0; i < size; ++i)
            alloc.construct(data + i, src[i]);
    }

    /**
     * Append several values to the array.
     *
     * @param src Raw array.
     * @param len Array length in elements.
     */
    void Append(ConstPointerType src, SizeType len) {
        Reserve(size + len);

        for (SizeType i = 0; i < len; ++i)
            alloc.construct(data + size + i, src[i]);

        size += len;
    }

    /**
     * Swap contents of the array with another instance.
     *
     * @param other Instance to swap with.
     */
    void Swap(Vector<T> &other) {
        if (this != &other) {
            std::swap(alloc, other.alloc);
            std::swap(size, other.size);
            std::swap(capacity, other.capacity);
            std::swap(data, other.data);
        }
    }

    /**
     * Get data pointer.
     *
     * @return Data pointer.
     */
    PointerType GetData() { return data; }

    /**
     * Get data pointer.
     *
     * @return Data pointer.
     */
    ConstPointerType GetData() const { return data; }

    /**
     * Get array size.
     *
     * @return Array size.
     */
    SizeType GetSize() const { return size; }

    /**
     * Get capacity.
     *
     * @return Array capacity.
     */
    SizeType GetCapacity() const { return capacity; }

    /**
     * Element access operator.
     *
     * @param idx Element index.
     * @return Element reference.
     */
    ReferenceType operator[](SizeType idx) {
        assert(idx < size);

        return data[idx];
    }

    /**
     * Element access operator.
     *
     * @param idx Element index.
     * @return Element reference.
     */
    ConstReferenceType operator[](SizeType idx) const {
        assert(idx < size);

        return data[idx];
    }

    /**
     * Check if the array is empty.
     *
     * @return True if the array is empty.
     */
    bool IsEmpty() const { return size == 0; }

    /**
     * Clears the array.
     */
    void Clear() {
        for (PointerType it = data; it != data + size; ++it)
            alloc.destroy(it);

        size = 0;
    }

    /**
     * Reserves not less than specified elements number so array is not
     * going to grow on append.
     *
     * @param newCapacity Desired capacity.
     */
    void Reserve(SizeType newCapacity) {
        if (capacity < newCapacity) {
            Vector<T> tmp(newCapacity);

            tmp.Assign(*this);

            Swap(tmp);
        }
    }

    /**
     * Resizes array. Destructs elements if the specified size is less
     * than the array's size. Default-constructs elements if the
     * specified size is more than the array's size.
     *
     * @param newSize Desired size.
     */
    void Resize(SizeType newSize) {
        if (capacity < newSize)
            Reserve(newSize);

        if (newSize > size) {
            for (PointerType it = data + size; it < data + newSize; ++it)
                alloc.construct(it, ValueType());
        } else {
            for (PointerType it = data + newSize; it < data + size; ++it)
                alloc.destroy(it);
        }

        size = newSize;
    }

    /**
     * Get last element.
     *
     * @return Last element reference.
     */
    const ValueType &Back() const {
        assert(size > 0);

        return data[size - 1];
    }

    /**
     * Get last element.
     *
     * @return Last element reference.
     */
    ValueType &Back() {
        assert(size > 0);

        return data[size - 1];
    }

    /**
     * Get first element.
     *
     * @return First element reference.
     */
    const ValueType &Front() const {
        assert(size > 0);

        return data[0];
    }

    /**
     * Get first element.
     *
     * @return First element reference.
     */
    ValueType &Front() {
        assert(size > 0);

        return data[0];
    }

    /**
     * Pushes new value to the back of the array, effectively increasing
     * array size by one.
     *
     * @param val Value to push.
     */
    void PushBack(ConstReferenceType val) {
        Resize(size + 1);

        Back() = val;
    }

private:
    /**
     * Calcutale capacity for required size.
     * Rounds up to the nearest power of two.
     *
     * @param size Needed capacity.
     * @return Recomended capacity to allocate.
     */
    SizeType getCapacityForSize(SizeType size) {
        if (size <= 8)
            return 8;

        constexpr SizeType threshold = std::numeric_limits<SizeType>::max() / 2 + 1u;
        if (size >= threshold)
            return std::numeric_limits<SizeType>::max();

        return bitCeil(size);
    }

    /** Allocator */
    AllocatorType alloc;

    /** Array size. */
    SizeType size;

    /** Array capacity. */
    SizeType capacity;

    /** Data. */
    PointerType data;
};

} // namespace ignite
