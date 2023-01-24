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

/** Heap byte buffer-based row. */
public class TableRow {
    public static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;

    /** Size of schema version length field. */
    private static final int SCHEMA_VERSION_FLD_LEN = Short.BYTES;

    /** Row schema version field offset. */
    private static final int SCHEMA_VERSION_OFFSET = 0;

    /** Row binary tuple field offset. */
    private static final int TUPLE_OFFSET = SCHEMA_VERSION_OFFSET + SCHEMA_VERSION_FLD_LEN;

    /** Row buffer. */
    private final ByteBuffer buf;

    /**
     * Constructor.
     *
     * @param data Array representation of the row.
     */
    public TableRow(byte[] data) {
        this(ByteBuffer.wrap(data).order(ORDER));
    }

    /**
     * Constructor.
     *
     * @param buf Buffer representing the row.
     */
    public TableRow(ByteBuffer buf) {
        assert buf.order() == ORDER;
        assert buf.position() == 0;

        this.buf = buf;
    }

    /**
     * Factory method which creates an instance using a schema version and an existing {@link BinaryTuple}.
     *
     * @param schemaVersion Schema version.
     * @param binaryTuple Binary tuple.
     * @return Created TableRow instance.
     */
    public static TableRow createFromTuple(int schemaVersion, BinaryTuple binaryTuple) {
        ByteBuffer tupleBuffer = binaryTuple.byteBuffer();
        ByteBuffer buffer = ByteBuffer.allocate(SCHEMA_VERSION_FLD_LEN + tupleBuffer.limit()).order(ORDER);
        buffer.putShort((short) schemaVersion);
        buffer.put(tupleBuffer);
        buffer.position(0);
        return new TableRow(buffer);
    }

    /** Get row schema version. */
    public int schemaVersion() {
        return Short.toUnsignedInt(buf.getShort(SCHEMA_VERSION_OFFSET));
    }

    /** Get ByteBuffer slice representing the binary tuple. */
    public ByteBuffer tupleSlice() {
        try {
            return buf.position(TUPLE_OFFSET).slice().order(ORDER);
        } finally {
            buf.position(0); // Reset bounds.
        }
    }

    /** Get byte array of the row. */
    public byte[] bytes() {
        // TODO IGNITE-15934 avoid copy.
        byte[] tmp = new byte[buf.limit()];

        buf.get(tmp);
        buf.rewind();

        return tmp;
    }

    /** Returns the representation of this row as a Byte Buffer. */
    public ByteBuffer byteBuffer() {
        return buf.slice().order(ORDER);
    }
}
