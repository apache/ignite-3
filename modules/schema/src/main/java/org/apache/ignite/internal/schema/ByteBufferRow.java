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
import java.nio.ByteOrder;

/**
 * Heap byte buffer-based row.
 */
public class ByteBufferRow implements BinaryRow {
    public static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;

    /** Row buffer. */
    private final ByteBuffer buf;

    /**
     * Constructor.
     *
     * @param data Array representation of the row.
     */
    public ByteBufferRow(byte[] data) {
        this(ByteBuffer.wrap(data).order(ORDER));
    }

    /**
     * Constructor.
     *
     * @param buf Buffer representing the row.
     */
    public ByteBufferRow(ByteBuffer buf) {
        assert buf.order() == ORDER;
        assert buf.position() == 0;

        this.buf = buf;
    }

    /** {@inheritDoc} */
    @Override
    public int schemaVersion() {
        return Short.toUnsignedInt(buf.getShort(SCHEMA_VERSION_OFFSET));
    }

    @Override
    public boolean hasValue() {
        byte schemaVer = buf.get(HAS_VALUE_OFFSET);

        return schemaVer == 1;
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer tupleSlice() {
        try {
            return buf.position(TUPLE_OFFSET).slice().order(ORDER);
        } finally {
            buf.position(0); // Reset bounds.
        }
    }

    /** {@inheritDoc} */
    @Override
    public byte[] bytes() {
        // TODO IGNITE-15934 avoid copy.
        byte[] tmp = new byte[buf.limit()];

        buf.get(tmp);
        buf.rewind();

        return tmp;
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer byteBuffer() {
        return buf.slice().order(ORDER);
    }
}
