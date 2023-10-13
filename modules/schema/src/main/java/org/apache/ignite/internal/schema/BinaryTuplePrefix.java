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

import static org.apache.ignite.internal.binarytuple.BinaryTupleCommon.PREFIX_FLAG;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.binarytuple.BinaryTuplePrefixBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.lang.InternalTuple;

/**
 * Class that represents a Binary Tuple Prefix.
 *
 * @see BinaryTuplePrefixBuilder BinaryTuplePrefixBuilder for information about the Binary Tuple Prefix format.
 */
public class BinaryTuplePrefix extends BinaryTupleReader implements InternalTuple {

    /**
     * Constructor.
     *
     * @param elementCount Number of tuple elements.
     * @param bytes Serialized representation of a Binary Tuple Prefix.
     */
    public BinaryTuplePrefix(int elementCount, byte[] bytes) {
        super(elementCount, bytes);
    }

    /**
     * Constructor.
     *
     * @param elementCount Number of tuple elements.
     * @param buffer Serialized representation of a Binary Tuple Prefix.
     */
    public BinaryTuplePrefix(int elementCount, ByteBuffer buffer) {
        super(elementCount, buffer);
    }

    /**
     * Creates a prefix from provided {@link BinaryTuple}. If given tuple has lesser or equal number of
     * columns, then all elements will be used in resulting prefix. If given tuple has more columns, then
     * excess columns will be truncated.
     *
     * @param size The size of the complete tuple.
     * @param tuple Tuple to create a prefix from.
     * @return Prefix, created from provided tuple with regards to desired size.
     */
    public static BinaryTuplePrefix fromBinaryTuple(int size, BinaryTuple tuple) {
        if (size == tuple.elementCount()) {
            return entireTuple(tuple);
        } else if (size > tuple.elementCount()) {
            return expandTuple(size, tuple);
        } else {
            return truncateTuple(size, tuple);
        }
    }

    /**
     * Creates a prefix that contains all columns from the provided {@link BinaryTuple}.
     *
     * @param tuple Tuple to create a prefix from.
     * @return Prefix, equivalent to the tuple.
     */
    public static BinaryTuplePrefix fromBinaryTuple(BinaryTuple tuple) {
        return entireTuple(tuple);
    }

    private static BinaryTuplePrefix entireTuple(BinaryTuple tuple) {
        ByteBuffer tupleBuffer = tuple.byteBuffer();

        ByteBuffer prefixBuffer = ByteBuffer.allocate(tupleBuffer.remaining() + Integer.BYTES)
                .order(ORDER)
                .put(tupleBuffer)
                .putInt(tuple.elementCount())
                .flip();

        byte flags = prefixBuffer.get(0);

        prefixBuffer.put(0, (byte) (flags | PREFIX_FLAG));

        return new BinaryTuplePrefix(tuple.elementCount(), prefixBuffer);
    }

    private static BinaryTuplePrefix expandTuple(int size, BinaryTuple tuple) {
        assert size > tuple.elementCount();

        var stats = new Sink() {
            int dataBeginOffset = 0;
            int dataEndOffset = 0;

            @Override
            public void nextElement(int index, int begin, int end) {
                if (index == 0) {
                    dataBeginOffset = begin;
                }

                dataEndOffset = end;
            }
        };

        tuple.parse(stats);

        ByteBuffer tupleBuffer = tuple.byteBuffer();

        byte flags = tupleBuffer.get(0);
        int entrySize = BinaryTupleCommon.flagsToEntrySize(flags);

        ByteBuffer prefixBuffer = ByteBuffer.allocate(
                        tupleBuffer.remaining()
                                + (entrySize * (size - tuple.elementCount()))
                                + Integer.BYTES)
                .order(ORDER)
                .put(tupleBuffer.slice().limit(stats.dataBeginOffset)); // header

        int payloadEndPosition = stats.dataEndOffset - stats.dataBeginOffset;
        for (int idx = tuple.elementCount(); idx < size; idx++) {
            switch (entrySize) {
                case Byte.BYTES:
                    prefixBuffer.put((byte) payloadEndPosition);
                    break;
                case Short.BYTES:
                    prefixBuffer.putShort((short) payloadEndPosition);
                    break;
                case Integer.BYTES:
                    prefixBuffer.putInt(payloadEndPosition);
                    break;
                default:
                    assert false;
            }
        }

        prefixBuffer
                .put(tupleBuffer.slice().position(stats.dataBeginOffset).limit(stats.dataEndOffset)) // payload
                .putInt(tuple.elementCount())
                .flip();

        prefixBuffer.put(0, (byte) (flags | PREFIX_FLAG));

        return new BinaryTuplePrefix(size, prefixBuffer);
    }

    private static BinaryTuplePrefix truncateTuple(int size, BinaryTuple tuple) {
        assert size < tuple.elementCount();

        var stats = new Sink() {
            int dataBeginOffset = 0;
            int dataEndOffset = 0;

            @Override
            public void nextElement(int index, int begin, int end) {
                if (index == 0) {
                    dataBeginOffset = begin;
                }

                if (index < size) {
                    dataEndOffset = end;
                }
            }
        };

        tuple.parse(stats);

        BinaryTuplePrefixBuilder builder = new BinaryTuplePrefixBuilder(size, size, stats.dataEndOffset - stats.dataBeginOffset);

        tuple.parse((index, begin, end) -> {
            if (index < size) {
                byte[] valueBytes = tuple.bytesValue(index);

                if (valueBytes == null) {
                    builder.appendNull();
                } else {
                    builder.appendBytes(valueBytes);
                }
            }
        });

        return new BinaryTuplePrefix(size, builder.build());
    }

    @Override
    public int elementCount() {
        ByteBuffer buffer = byteBuffer();

        return buffer.getInt(buffer.limit() - Integer.BYTES);
    }
}
