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

package org.apache.ignite.internal.schema;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.binarytuple.BinaryTupleParser;
import org.apache.ignite.internal.binarytuple.ByteBufferAccessor;
import org.apache.ignite.internal.util.GridUnsafe;
import org.jetbrains.annotations.Nullable;

/**
 * The `UnsafeByteBufferAccessor` class provides low-level access to the contents of a `ByteBuffer`.
 * It implements the `ByteBufferAccessor` interface and uses unsafe operations to read data
 * directly from the buffer, either from a direct memory address or from a backing array.
 * This class supports reading various primitive types (e.g., `byte`, `int`, `long`, etc.)
 * and handles byte order differences between the buffer and the native system.
 */
public class UnsafeByteBufferAccessor implements ByteBufferAccessor {
    /** Whether the byte order of the underlying buffer is reversed compared to the native byte order. */
    private static final boolean REVERSE_BYTE_ORDER = GridUnsafe.NATIVE_BYTE_ORDER != BinaryTupleParser.ORDER;

    private byte @Nullable [] bytes;
    private long addr;
    private int capacity;

    /**
     * Constructor that initializes the accessor with a {@link ByteBuffer}.
     */
    public UnsafeByteBufferAccessor(ByteBuffer buff) {
        if (buff.isDirect()) {
            bytes = null;
            addr = GridUnsafe.bufferAddress(buff);
        } else {
            bytes = buff.array();
            addr = GridUnsafe.BYTE_ARR_OFF + buff.arrayOffset();
        }

        capacity = buff.capacity();
    }

    /**
     * Constructor that initializes the accessor with a memory address and capacity.
     *
     * @param addr Memory address.
     * @param capacity Capacity in bytes.
     */
    public UnsafeByteBufferAccessor(long addr, int capacity) {
        this.bytes = null;
        this.addr = addr;
        this.capacity = capacity;
    }

    /**
     * Initializes the accessor with a memory address and capacity.
     */
    public void reinit(long addr, int capacity) {
        this.bytes = null;
        this.addr = addr;
        this.capacity = capacity;
    }

    /**
     * Initializes the accessor with a byte array, offset, and length.
     */
    public void reinit(byte[] bytes, int from, int length) {
        this.bytes = bytes;
        this.addr = GridUnsafe.BYTE_ARR_OFF + from;
        this.capacity = length;
    }

    @Override
    public byte get(int p) {
        return GridUnsafe.getByte(bytes, addr + p);
    }

    @Override
    public int getInt(int p) {
        int value = GridUnsafe.getInt(bytes, addr + p);

        return REVERSE_BYTE_ORDER ? Integer.reverseBytes(value) : value;
    }

    @Override
    public long getLong(int p) {
        long value = GridUnsafe.getLong(bytes, addr + p);

        return REVERSE_BYTE_ORDER ? Long.reverseBytes(value) : value;
    }

    @Override
    public short getShort(int p) {
        short value = GridUnsafe.getShort(bytes, addr + p);

        return REVERSE_BYTE_ORDER ? Short.reverseBytes(value) : value;
    }

    @Override
    public float getFloat(int p) {
        float value = GridUnsafe.getFloat(bytes, addr + p);

        return REVERSE_BYTE_ORDER ? Float.intBitsToFloat(Integer.reverseBytes(Float.floatToIntBits(value))) : value;
    }

    @Override
    public double getDouble(int p) {
        double value = GridUnsafe.getDouble(bytes, addr + p);

        return REVERSE_BYTE_ORDER ? Double.longBitsToDouble(Long.reverseBytes(Double.doubleToLongBits(value))) : value;
    }

    @Override
    public int capacity() {
        return capacity;
    }

    /**
     * Returns the underlying byte array instance.
     */
    public byte[] getArray() {
        return bytes;
    }

    /**
     * Returns the underlying address.
     */
    public long getAddress() {
        return addr;
    }
}
