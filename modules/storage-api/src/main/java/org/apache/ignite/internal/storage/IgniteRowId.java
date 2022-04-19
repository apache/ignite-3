/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage;

import java.nio.ByteBuffer;

/**
 * Interface that represents row id in primary index of the table.
 *
 * @see MvPartitionStorage
 */
public interface IgniteRowId extends Comparable<IgniteRowId> {
    /**
     * Maximum possible row id size in bytes. If PK columns exceed this size, then UUID-based row id should be used.
     */
    final int MAX_ROW_ID_SIZE = 16;

    /**
     * Writes row id into a byte buffer. Binary row representation should match natural order defined by {@link #compareTo(Object)} when
     * comparing lexicographically.
     *
     * @param buf Output byte buffer with {@link java.nio.ByteOrder#BIG_ENDIAN} byte order.
     * @param signedBytesCompare Defines properties of a target binary comparator. {@code true} if bytes are compared as signed values,
     *      {@code false} if unsigned.
     */
    void writeTo(ByteBuffer buf, boolean signedBytesCompare);

    /**
     * Compares row id with a byte buffer, previously written by a {@link #writeTo(ByteBuffer, boolean)} method.
     *
     * @param buf Input byte buffer with {@link java.nio.ByteOrder#BIG_ENDIAN} byte order.
     * @param signedBytesCompare Defines properties of a binary comparator. {@code true} if bytes are compared as signed values,
     *      {@code false} if unsigned.
     * @return A negative integer, zero, or a positive integer as this row id is less than, equal to, or greater than the specified row id.
     */
    int compareTo(ByteBuffer buf, boolean signedBytesCompare);
}
