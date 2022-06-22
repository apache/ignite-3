/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.schema;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Binary tuple parser.
 */
public class BinaryTupleParser {
    /**
     * Receiver of parsed data.
     */
    interface Sink {
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
    BinaryTupleParser(int numElements, ByteBuffer buffer) {
        this.numElements = numElements;

        assert buffer.order() == ByteOrder.LITTLE_ENDIAN;
        assert buffer.position() == 0;
        this.buffer = buffer;

        byte flags = buffer.get(0);
        entrySize = 1 << (flags & BinaryTupleSchema.VARSIZE_MASK);

        int nullMapSize = 0;
        if ((flags & BinaryTupleSchema.NULLMAP_FLAG) != 0) {
            nullMapSize = BinaryTupleSchema.getNullMapSize(numElements);
        }

        entryBase = BinaryTupleSchema.HEADER_SIZE + nullMapSize;
        valueBase = entryBase + entrySize * numElements;
    }

    /**
     * Returns the binary tuple size in bytes.
     */
    int size() {
        return valueBase + getOffset(valueBase - entrySize);
    }

    /**
     * Returns the number of elements in the tuple.
     */
    int elementCount() {
        return numElements;
    }

    /**
     * Check if the binary tuple contains a null map.
     */
    boolean hasNullMap() {
        return entryBase > BinaryTupleSchema.HEADER_SIZE;
    }

    /**
     * Get buffer positioned at the specified tuple element.
     *
     * If the element has NULL value then null is returned.
     *
     * @param index Index of the element.
     * @return ByteBuffer with element bytes between current position and limit or null.
     */
    ByteBuffer element(int index) {
        var sink = new Sink() {
            int begin, end;

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
    void fetch(int index, Sink sink) {
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
    void parse(Sink sink) {
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
            case 1:
                return Byte.toUnsignedInt(buffer.get(index));
            case 2:
                return Short.toUnsignedInt(buffer.getShort(index));
            case 4: {
                int offset = buffer.getInt(index);
                if (offset < 0) {
                    throw new IgniteInternalException("Unsupported offset table size");
                }
                return offset;
            }
            case 8:
                throw new IgniteInternalException("Unsupported offset table size");
            default:
                throw new IgniteInternalException("Invalid offset table size");
        }
    }
}
