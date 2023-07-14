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
 * Binary row interface. Data layout is described in packages' {@code README.md}.
 */
public interface BinaryRow {
    ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;

    /** Size of schema version field. */
    int SCHEMA_VERSION_FLD_LEN = Short.BYTES;

    /** Row schema version field offset. */
    int SCHEMA_VERSION_OFFSET = 0;

    /** Row binary tuple field offset. */
    int TUPLE_OFFSET = SCHEMA_VERSION_OFFSET + SCHEMA_VERSION_FLD_LEN;

    /** Get row schema version. */
    int schemaVersion();

    /** Get has value flag: {@code true} if row has non-null value, {@code false} otherwise. */
    boolean hasValue();

    /** Length of the {@link #tupleSlice}. */
    int tupleSliceLength();

    /** Get ByteBuffer slice representing the binary tuple. */
    ByteBuffer tupleSlice();

    /** Returns the representation of this row as a Byte Buffer. */
    // TODO: remove this method, see https://issues.apache.org/jira/browse/IGNITE-19937
    ByteBuffer byteBuffer();
}
