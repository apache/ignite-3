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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Binary row interface.
 * The class contains low-level methods to read row data.
 */
public interface BinaryRow {
    /** Row schema version field offset. */
    int SCHEMA_VERSION_OFFSET = 0;

    /** Row flags field offset. */
    int FLAGS_FIELD_OFFSET = SCHEMA_VERSION_OFFSET + 2 /* version length */;

    /** Key hash field offset. */
    int KEY_HASH_FIELD_OFFSET = FLAGS_FIELD_OFFSET + 2 /* flags length */;

    /** Key chunk field offset. */
    int KEY_CHUNK_OFFSET = KEY_HASH_FIELD_OFFSET + 4 /* hash length */;

    /** Size of chunk length field. */
    int CHUNK_LEN_FLD_SIZE = Integer.BYTES;

    /** Row header size. */
    int HEADER_SIZE = KEY_CHUNK_OFFSET;

    /**
     * @return Row schema version.
     */
    int schemaVersion();

    /**
     * @return {@code True} if row has non-null value, {@code false} otherwise.
     */
    boolean hasValue();

    // TODO: IGNITE-14199. Add row version.
    //GridRowVersion version();

    /**
     * Row hash code is a result of hash function applied to the row affinity columns values.
     *
     * @return Row hash code.
     */
    int hash();

    /**
     * @return ByteBuffer slice representing the key chunk.
     */
    ByteBuffer keySlice();

    /**
     * @return ByteBuffer slice representing the value chunk.
     */
    ByteBuffer valueSlice();

    /**
     * Writes binary row to given stream.
     *
     * @param stream Stream to write to.
     * @throws IOException If write operation fails.
     */
    void writeTo(OutputStream stream) throws IOException;

    /**
     * @param off Offset.
     * @return Byte primitive value.
     */
    byte readByte(int off);

    /**
     * @param off Offset.
     * @return Short primitive value.
     */
    short readShort(int off);

    /**
     * @param off Offset.
     * @return Integer primitive value.
     */
     int readInteger(int off);

    /**
     * @param off Offset.
     * @return Long primitive value.
     */
     long readLong(int off);

    /**
     * @param off Offset.
     * @return Float primitive value.
     */
     float readFloat(int off);

    /**
     * @param off Offset.
     * @return Double primitive value.
     */
     double readDouble(int off);

    /**
     * @param off Offset.
     * @param len Length.
     * @return String value.
     */
     String readString(int off, int len);

    /**
     * @param off Offset.
     * @param len Length.
     * @return Byte array.
     */
     byte[] readBytes(int off, int len);

    /**
     * @return Byte array of the row.
     */
     byte[] bytes();

    /**
     * Row flags.
     */
    final class RowFlags {
        /** Flag indicates row has no value chunk. */
        public static final int NO_VALUE_FLAG = 1;

        /** Chunk flags mask. */
        public static final int CHUNK_FLAGS_MASK = 0x0F;

        /** Key specific flags. */
        public static final int KEY_FLAGS_OFFSET = 8;

        /** Value specific flags. */
        public static final int VAL_FLAGS_OFFSET = 12;

        /** Stub. */
        private RowFlags() {
            // No-op.
        }
    }
}
