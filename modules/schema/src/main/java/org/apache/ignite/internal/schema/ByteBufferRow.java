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
import java.nio.charset.StandardCharsets;

/**
 * Heap byte buffer-based row.
 */
public class ByteBufferRow extends Row {
    /** */
    private final ByteBuffer buf;

    /**
     * @param schema Row schema.
     * @param data Array representation of the row.
     */
    public ByteBufferRow(SchemaDescriptor schema, byte[] data) {
        super(schema);

        buf = ByteBuffer.wrap(data);
        buf.order(ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Constructor.
     *
     * @param schema Schema.
     * @param buf Buffer representing the row.
     */
    public ByteBufferRow(SchemaDescriptor schema, ByteBuffer buf) {
        super(schema);

        assert buf.order() == ByteOrder.LITTLE_ENDIAN;
        this.buf = buf;
    }

    /** {@inheritDoc} */
    @Override protected byte readByte(int off) {
        return (byte)(buf.get(off) & 0xFF);
    }

    /** {@inheritDoc} */
    @Override protected short readShort(int off) {
        return (short)(buf.getShort(off) & 0xFFFF);
    }

    /** {@inheritDoc} */
    @Override protected int readInteger(int off) {
        return buf.getInt(off);
    }

    /** {@inheritDoc} */
    @Override protected long readLong(int off) {
        return buf.getLong(off);
    }

    /** {@inheritDoc} */
    @Override protected float readFloat(int off) {
        return buf.getFloat(off);
    }

    /** {@inheritDoc} */
    @Override protected double readDouble(int off) {
        return buf.getDouble(off);
    }

    /** {@inheritDoc} */
    @Override protected byte[] readBytes(int off, int len) {
        try {
            byte[] res = new byte[len];

            buf.position(off);

            buf.get(res, 0, res.length);

            return res;
        }
        finally {
            buf.position(0);
        }
    }

    /** {@inheritDoc} */
    @Override protected String readString(int off, int len) {
        return new String(buf.array(), off, len, StandardCharsets.UTF_8);
    }

    /** {@inheritDoc} */
    @Override public byte[] rowBytes() {
        return buf.array();
    }

    /** {@inheritDoc} */
    @Override public byte[] keyChunkBytes() {
        final int len = readInteger(KEY_CHUNK_OFFSET);

        return readBytes(KEY_HASH_FIELD_OFFSET, len); // Includes key-hash.
    }

    /** {@inheritDoc} */
    @Override public byte[] valueChunkBytes() {
        int off = KEY_CHUNK_OFFSET + readInteger(KEY_CHUNK_OFFSET);
        int len = readInteger(off);

        return readBytes(off, len);
    }
}
