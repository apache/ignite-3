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

package org.apache.ignite.internal.binarytuple;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Expandable wrapper over {@link ByteBuffer}.
 */
public class ExpandableByteBuffer {
    /** Minimum capacity. */
    public static int MINIMUM_CAPACITY = 16;

    /** Wrapped array. */
    private ByteBuffer buffer;

    /**
     * Creates expandable row buffer.
     *
     * @param size Initial buffer size.
     */
    public ExpandableByteBuffer(int size) {
        if (size <= 0) {
            size = MINIMUM_CAPACITY;
        }

        buffer = ByteBuffer.allocate(size);
    }

    /**
     * Writes {@code byte}  value to the buffer.
     *
     * @param val Value.
     */
    public void put(byte val) {
        ensure(Byte.BYTES);

        buffer.put(val);
    }

    /**
     * Writes {@code byte} value to the buffer.
     *
     * @param off Buffer offset.
     * @param val Value.
     */
    public void put(int off, byte val) {
        buffer.put(off, val);
    }

    /**
     * Writes {@code byte[]} value to the buffer.
     *
     * @param val Value.
     */
    public void put(byte[] val) {
        ensure(val.length);

        buffer.put(val);
    }

    /**
     * Writes {@code byte[]} value to the buffer.
     *
     * @param off Buffer offset.
     * @param val Value.
     */
    public void put(int off, byte[] val) {
        buffer.put(val, off, val.length);
    }

    /**
     * Transfers the bytes remaining in the given source buffer into this buffer.
     *
     * @param buffer Source buffer.
     * @param len Length.
     */
    public void put(ByteBuffer buffer, int len) {
        ensure(len);

        this.buffer.put(buffer);
    }

    /**
     * Writes {@code short} value to the buffer.
     *
     * @param val Value.
     */
    public void putShort(short val) {
        ensure(Short.BYTES);

        buffer.putShort(val);
    }

    /**
     * Writes {@code short} value to the buffer.
     *
     * @param off Buffer offset.
     * @param val Value.
     */
    public void putShort(int off, short val) {
        buffer.putShort(off, val);
    }

    /**
     * Writes {@code int} value to the buffer.
     *
     * @param val Value.
     */
    public void putInt(int val) {
        ensure(Integer.BYTES);

        buffer.putInt(val);
    }

    /**
     * Writes {@code int} value to the buffer.
     *
     * @param off Buffer offset.
     * @param val Value.
     */
    public void putInt(int off, int val) {
        buffer.putInt(off, val);
    }

    /**
     * Writes {@code long} value to the buffer.
     *
     * @param val Value.
     */
    public void putLong(long val) {
        ensure(Long.BYTES);

        buffer.putLong(val);
    }

    /**
     * Writes {@code long} value to the buffer.
     *
     * @param off Buffer offset.
     * @param val Value.
     */
    public void putLong(int off, long val) {
        buffer.putLong(off, val);
    }

    /**
     * Writes {@code float} value to the buffer.
     *
     * @param val Value.
     */
    public void putFloat(float val) {
        ensure(Float.BYTES);

        buffer.putFloat(val);
    }

    /**
     * Writes {@code float} value to the buffer.
     *
     * @param val Value.
     */
    public void putFloat(int off, float val) {
        buffer.putFloat(off, val);
    }

    /**
     * Writes {@code double} value to the buffer.
     *
     * @param val Value.
     */
    public void putDouble(double val) {
        ensure(Double.BYTES);

        buffer.putDouble(val);
    }

    /**
     * Writes {@code double} value to the buffer.
     *
     * @param off Buffer offset.
     * @param val Value.
     */
    public void putDouble(int off, double val) {
        ensure(Double.BYTES);

        buffer.putDouble(val);
    }

    /**
     * Reads {@code byte} value from buffer.
     *
     * @param off Buffer offset.
     * @return Value.
     */
    public byte get(int off) {
        return buffer.get(off);
    }

    /**
     * Reads {@code short} value from buffer.
     *
     * @param off Buffer offset.
     * @return Value.
     */
    public short getShort(int off) {
        return buffer.getShort(off);
    }

    /**
     * Absolute <i>get</i> method for reading an int value.
     *
     * <p>Reads four bytes at the given index, composing them into a
     * int value according to the current byte order.
     *
     * @param index The index from which the bytes will be read
     * @return The int value at the given index
     */
    public int getInt(int index) {
        return buffer.getInt(index);
    }

    /**
     * Returns this buffer's position.
     *
     * @return The position of this buffer
     */
    public int position() {
        return buffer.position();
    }

    /**
     * Sets this buffer's position. If the mark is defined and larger than the new position then it is discarded.
     *
     * @param newPosition The new position value.
     * @return This buffer
     * @throws IllegalArgumentException If the preconditions on {@code newPosition} do not hold.
     */
    public ExpandableByteBuffer position(int newPosition) {
        buffer.position(newPosition);

        return this;
    }

    /**
     * Returns the number of elements between the current position and the
     * limit.
     *
     * @return The number of elements remaining in this buffer
     */
    public int remaining() {
        return buffer.remaining();
    }

    /**
     * Returns this buffer's capacity.
     *
     * @return The capacity of this buffer
     */
    public int capacity() {
        return buffer.capacity();
    }

    /**
     * Flips this buffer. The limit is set to the current position and then
     * the position is set to zero. If the mark is defined then it is discarded.
     *
     * @return This buffer
     */
    public ExpandableByteBuffer flip() {
        buffer.flip();

        return this;
    }

    /**
     * Modifies this buffer's byte order.
     *
     * @param  bo
     *         The new byte order,
     *         either {@link ByteOrder#BIG_ENDIAN BIG_ENDIAN}
     *         or {@link ByteOrder#LITTLE_ENDIAN LITTLE_ENDIAN}
     *
     * @return  This buffer
     */
    public ExpandableByteBuffer order(ByteOrder bo) {
        buffer.order(bo);

        return this;
    }

    /**
     * Rewinds this buffer. The position is set to zero and the mark is discarded.
     *
     * @return This buffer.
     */
    public ExpandableByteBuffer rewind() {
        buffer.rewind();

        return this;
    }

    /**
     * Clears this buffer. The position is set to zero, the limit is set to the capacity, and the mark is discarded.
     *
     * @return This buffer.
     */
    public ExpandableByteBuffer clear() {
        buffer.clear();

        return this;
    }

    /**
     * Returns wrapped {@link ByteBuffer}.
     *
     * @return Wrapped buffer.
     */
    public ByteBuffer unwrap() {
        return buffer;
    }

    /**
     * Ensure that the buffer can fit the required size.
     *
     * @param size Required size.
     */
    void ensure(int size) {
        if (buffer.remaining() < size) {
            grow(size);
        }
    }

    private void grow(int size) {
        int capacity = buffer.capacity();
        do {
            capacity *= 2;
            if (capacity < 0) {
                throw new BinaryTupleFormatException("Buffer overflow in binary tuple builder");
            }
        } while ((capacity - buffer.position()) < size);

        ByteBuffer newBuffer = ByteBuffer.allocate(capacity);
        newBuffer.order(ByteOrder.LITTLE_ENDIAN);
        newBuffer.put(buffer.flip());

        buffer = newBuffer;
    }
}
