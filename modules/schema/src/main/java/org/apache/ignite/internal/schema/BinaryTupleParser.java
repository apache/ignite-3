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

package org.apache.ignite.internal.schema;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Binary tuple parser allows to get bytes of individual elements from entirety of tuple bytes.
 */
public class BinaryTupleParser {
    /**
     * Receiver of parsed data.
     */
    public interface Sink {
        /**
         * Provides the location of the next tuple value.
         *
         * @param index Value index.
         * @param begin Start offset of the value, 0 for NULL.
         * @param end End offset of the value, 0 for NULL.
         */
        void nextElement(int index, int begin, int end);
    }

    /** Number of elements in the tuple. */
    private final int numElements;

    /** Size of an offset table entry. */
    private final int entrySize;

    /** Position of the varlen offset table. */
    private final int entryBase;

    /** Starting position of variable-length values. */
    private final int valueBase;

    /** Binary tuple. */
    protected final ByteBuffer buffer;

    /**
     * Constructor.
     *
     * @param numElements Number of tuple elements.
     * @param buffer Buffer with a binary tuple.
     */
    public BinaryTupleParser(int numElements, ByteBuffer buffer) {
        this.numElements = numElements;

        assert buffer.order() == ByteOrder.LITTLE_ENDIAN;
        assert buffer.position() == 0;
        this.buffer = buffer;

        byte flags = buffer.get(0);

        int base = BinaryTupleSchema.HEADER_SIZE;
        if ((flags & BinaryTupleSchema.NULLMAP_FLAG) != 0) {
            base += BinaryTupleSchema.nullMapSize(numElements);
        }

        entryBase = base;
        entrySize = 1 << (flags & BinaryTupleSchema.VARSIZE_MASK);
        valueBase = base + entrySize * numElements;
    }

    /**
     * Returns the binary tuple size in bytes.
     */
    public int size() {
        return valueBase + getOffset(valueBase - entrySize);
    }

    /**
     * Returns the number of elements in the tuple.
     */
    public int elementCount() {
        return numElements;
    }

    /**
     * Check if the binary tuple contains a null map.
     */
    public boolean hasNullMap() {
        return entryBase > BinaryTupleSchema.HEADER_SIZE;
    }

    /**
     * Get buffer positioned at the specified tuple element.
     *
     * <p>If the element has NULL value then null is returned.
     *
     * @param index Index of the element.
     * @return ByteBuffer with element bytes between current position and limit or null.
     */
    public ByteBuffer element(int index) {
        var sink = new Sink() {
            int begin;
            int end;

            @Override
            public void nextElement(int index, int begin, int end) {
                this.begin = begin;
                this.end = end;
            }
        };

        fetch(index, sink);
        if (sink.begin == 0) {
            return null;
        }

        return buffer.asReadOnlyBuffer().position(sink.begin).limit(sink.end).order(ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Locate the specified tuple element.
     *
     * @param index Index of the element.
     * @param sink Receiver.
     */
    public void fetch(int index, Sink sink) {
        assert index >= 0;
        assert index < numElements;

        int entry = entryBase + index * entrySize;

        int offset = valueBase;
        if (index > 0) {
            offset += getOffset(entry - entrySize);
        }

        int nextOffset = valueBase + getOffset(entry);
        assert nextOffset >= offset;

        if (offset == nextOffset && hasNullMap()) {
            int nullIndex = BinaryTupleSchema.HEADER_SIZE + index / 8;
            byte nullMask = (byte) (1 << (index % 8));
            if ((buffer.get(nullIndex) & nullMask) != 0) {
                sink.nextElement(index, 0, 0);
                return;
            }
        }

        sink.nextElement(index, offset, nextOffset);
    }

    /**
     * Feeds the receiver with all tuple elements.
     *
     * @param sink Receiver.
     */
    public void parse(Sink sink) {
        int entry = entryBase;
        int offset = valueBase;

        for (int i = 0; i < numElements; i++) {
            int nextOffset = valueBase + getOffset(entry);
            assert nextOffset >= offset;

            if (offset == nextOffset && hasNullMap()) {
                int nullIndex = BinaryTupleSchema.HEADER_SIZE + i / 8;
                byte nullMask = (byte) (1 << (i % 8));
                if ((buffer.get(nullIndex) & nullMask) != 0) {
                    sink.nextElement(i, 0, 0);
                    continue;
                }
            }

            sink.nextElement(i, offset, nextOffset);
            offset = nextOffset;
            entry += entrySize;
        }
    }

    /**
     * Get an entry from the value offset table.
     *
     * @param index Byte index of the table entry.
     * @return Entry value.
     */
    private int getOffset(int index) {
        switch (entrySize) {
            case Byte.BYTES:
                return Byte.toUnsignedInt(buffer.get(index));
            case Short.BYTES:
                return Short.toUnsignedInt(buffer.getShort(index));
            case Integer.BYTES: {
                int offset = buffer.getInt(index);
                if (offset < 0) {
                    throw new IgniteInternalException("Unsupported offset table size");
                }
                return offset;
            }
            case Long.BYTES:
                throw new IgniteInternalException("Unsupported offset table size");
            default:
                throw new IgniteInternalException("Invalid offset table size");
        }
    }
}
