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

    /** {@inheritDoc} */
    @Override
    public boolean hasValue() {
        short schemaVer = buf.getShort(SCHEMA_VERSION_OFFSET);

        return schemaVer > 0;
    }

    /** {@inheritDoc} */
    @Override
    public int hash() {
        return buf.getInt(KEY_HASH_FIELD_OFFSET);
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer keySlice() {
        int off = KEY_CHUNK_OFFSET;
        int len = buf.getInt(off);
        int limit = buf.limit();

        try {
            return buf.limit(off + len).position(off).slice().order(ORDER);
        } finally {
            buf.position(0); // Reset bounds.
            buf.limit(limit);
        }
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer valueSlice() {
        int off = KEY_CHUNK_OFFSET + buf.getInt(KEY_CHUNK_OFFSET);
        int len = hasValue() ? buf.getInt(off) : 0;
        int limit = buf.limit();

        try {
            return buf.limit(off + len).position(off).slice().order(ORDER);
        } finally {
            buf.position(0); // Reset bounds.
            buf.limit(limit);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] bytes() {
        // TODO IGNITE-15934 avoid copy.
        byte[] tmp = new byte[buf.limit()];

        buf.get(tmp);
        buf.rewind();

        return tmp;
    }

    @Override
    public ByteBuffer byteBuffer() {
        return buf.slice().order(ORDER);
    }
}
