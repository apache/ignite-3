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

/**
 * Binary row interface. Data layout is described in packages' {@code README.md}.
 */
public interface BinaryRow {
    /** Size of chunk length field. */
    int CHUNK_LEN_FLD_SIZE = Integer.BYTES;

    /** Size of flags field. */
    int FLAGS_FLD_SIZE = Byte.BYTES;

    /** Size of schema version length field. */
    int SCHEMA_VERSION_FLD_LEN = Short.BYTES;

    /** Size of key hash field. */
    int HASH_FLD_SIZE = Integer.BYTES;

    /** Row schema version field offset. */
    int SCHEMA_VERSION_OFFSET = 0;

    /** Row flags field offset from the chunk start. */
    int FLAGS_FIELD_OFFSET = CHUNK_LEN_FLD_SIZE;

    /** Key hash field offset. */
    int KEY_HASH_FIELD_OFFSET = SCHEMA_VERSION_FLD_LEN;

    /** Key chunk field offset. */
    int KEY_CHUNK_OFFSET = KEY_HASH_FIELD_OFFSET + HASH_FLD_SIZE;

    /** Size of chunk header (offset for nullmap or vartable or data). */
    int CHUNK_HEADER_SIZE = CHUNK_LEN_FLD_SIZE + FLAGS_FLD_SIZE;

    /** Row header size. */
    int HEADER_SIZE = KEY_CHUNK_OFFSET;

    /**
     * Get row schema version.
     */
    int schemaVersion();

    /**
     * Get has value flag: {@code True} if row has non-null value, {@code false} otherwise.
     */
    boolean hasValue();

    /**
     * Row hash code is a result of hash function applied to the row affinity columns values.
     *
     * @return Row hash code.
     */
    int hash();

    /**
     * Get ByteBuffer slice representing the key chunk.
     */
    ByteBuffer keySlice();

    /**
     * Get ByteBuffer slice representing the value chunk.
     */
    ByteBuffer valueSlice();

    /**
     * Get byte array of the row.
     */
    byte[] bytes();

    /**
     * Returns the representation of this row as a Byte Buffer.
     */
    ByteBuffer byteBuffer();
}
