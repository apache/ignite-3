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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import org.apache.ignite.internal.binarytuple.BinaryTuplePrefixBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.schema.row.InternalTuple;

/**
 * Class that represents a Binary Tuple Prefix.
 *
 * @see BinaryTuplePrefixBuilder BinaryTuplePrefixBuilder for information about the Binary Tuple Prefix format.
 */
public class BinaryTuplePrefix extends BinaryTupleReader implements InternalTuple {
    /** Tuple schema. */
    private final BinaryTupleSchema schema;

    /**
     * Constructor.
     *
     * @param schema Full Tuple schema.
     * @param bytes Serialized representation of a Binary Tuple Prefix.
     */
    public BinaryTuplePrefix(BinaryTupleSchema schema, byte[] bytes) {
        super(schema.elementCount(), bytes);
        this.schema = schema;
    }

    /**
     * Constructor.
     *
     * @param schema Full Tuple schema.
     * @param buffer Serialized representation of a Binary Tuple Prefix.
     */
    public BinaryTuplePrefix(BinaryTupleSchema schema, ByteBuffer buffer) {
        super(schema.elementCount(), buffer);
        this.schema = schema;
    }

    /**
     * Creates a prefix that contains all columns from the provided {@link BinaryTuple}.
     *
     * @param tuple Tuple to create a prefix from.
     * @return Prefix, equivalent to the tuple.
     */
    public static BinaryTuplePrefix fromBinaryTuple(BinaryTuple tuple) {
        //TODO: Create ticket to avoid this convertion.
        // Restore original prefix. Explicit 'null' value is not supported for now.
        int count = (int) IntStream.range(0, tuple.count())
                .takeWhile(i -> !tuple.hasNullValue(i))
                .count();

        ByteBuffer tupleBuffer = tuple.byteBuffer();

        ByteBuffer prefixBuffer = ByteBuffer.allocate(tupleBuffer.remaining() + Integer.BYTES)
                .order(ORDER)
                .put(tupleBuffer)
                .putInt(count)
                .flip();

        byte flags = prefixBuffer.get(0);

        prefixBuffer.put(0, (byte) (flags | PREFIX_FLAG));

        return new BinaryTuplePrefix(tuple.schema(), prefixBuffer);
    }

    @Override
    public int count() {
        return elementCount();
    }

    @Override
    public BigDecimal decimalValue(int index) {
        return decimalValue(index, schema.element(index).decimalScale);
    }

    @Override
    public int elementCount() {
        ByteBuffer buffer = byteBuffer();

        return buffer.getInt(buffer.limit() - Integer.BYTES);
    }
}
