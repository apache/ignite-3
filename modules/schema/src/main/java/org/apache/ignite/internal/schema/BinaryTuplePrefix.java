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
     * Creates a prefix that contains all columns from the provided {@link BinaryTuple}.
     *
     * @param tuple Tuple to create a prefix from.
     * @return Prefix, equivalent to the tuple.
     */
    public static BinaryTuplePrefix fromBinaryTuple(BinaryTuple tuple) {
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

    @Override
    public int elementCount() {
        ByteBuffer buffer = byteBuffer();

        return buffer.getInt(buffer.limit() - Integer.BYTES);
    }
}
