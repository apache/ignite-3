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
    /** Size of schema version field. */
    int SCHEMA_VERSION_FLD_LEN = Short.BYTES;

    /** Row schema version field offset. */
    int SCHEMA_VERSION_OFFSET = 0;

    /** Size of 'has value' field. */
    int HAS_VALUE_FLD_LEN = Byte.BYTES;

    /** Row 'has value' field offset. */
    int HAS_VALUE_OFFSET = SCHEMA_VERSION_OFFSET + SCHEMA_VERSION_FLD_LEN;

    /** Row binary tuple field offset. */
    int TUPLE_OFFSET = HAS_VALUE_OFFSET + HAS_VALUE_FLD_LEN;

    /** Get row schema version. */
    int schemaVersion();

    /** Get has value flag: {@code true} if row has non-null value, {@code false} otherwise. */
    boolean hasValue();

    /** Get ByteBuffer slice representing the binary tuple. */
    ByteBuffer tupleSlice();

    /** Get byte array of the row. */
    byte[] bytes();

    /** Returns the representation of this row as a Byte Buffer. */
    ByteBuffer byteBuffer();
}
