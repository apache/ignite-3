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
    /** */
    int SCHEMA_VERSION_OFFSET = 0;
    /** */
    int FLAGS_FIELD_OFFSET = SCHEMA_VERSION_OFFSET + 2;
    /** */
    int KEY_HASH_FIELD_OFFSET = FLAGS_FIELD_OFFSET + 2;
    /** */
    int KEY_CHUNK_OFFSET = KEY_HASH_FIELD_OFFSET + 4;
    /** */
    int TOTAL_LEN_FIELD_SIZE = 4;
    /** */
    int VARLEN_TABLE_SIZE_FIELD_SIZE = 2;
    /** */
    int VARLEN_COLUMN_OFFSET_FIELD_SIZE = 2;

    /**
     * @return Row schema version.
     */
    int schemaVersion();

    /**
     * @return {@code True} if row has non-null value, {@code false} otherwise.
     */
    boolean hasValue();

    // TODO: add row version.
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
     * @throws IOException If write operation fails.
     */
    // TODO: support other target types in writeTo() if needed.
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
     * @return String value.
     */
     String readString(int off, int len);

    /**
     * @param off Offset.
     * @return Byte array.
     */
     byte[] readBytes(int off, int len);

    /**
     *
     */
    final class RowFlags {
        /** Tombstone flag. */
        public static final int TOMBSTONE = 1;

        /** Null-value flag. */
        public static final int NULL_VALUE = 1 << 1;

        /** Stub. */
        private RowFlags() {
        }
    }
}
