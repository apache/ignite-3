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

package org.apache.ignite.internal.rocksdb.snapshot;

import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.jetbrains.annotations.Nullable;

/**
 * Class that represents a range of keys in a Column Family.
 */
public class ColumnFamilyRange {
    /** Column Family. */
    private final ColumnFamily columnFamily;

    /** Lower bound (inclusive) or {@code null} if this is a full range. */
    private final byte @Nullable [] lowerBound;

    /** Upper bound (exclusive) or {@code null} if this is a full range. */
    private final byte @Nullable [] upperBound;

    private ColumnFamilyRange(ColumnFamily columnFamily, byte @Nullable [] lowerBound, byte @Nullable [] upperBound) {
        this.columnFamily = columnFamily;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    /**
     * Creates a range that consists of all keys in a Column Family.
     *
     * @param columnFamily Column Family to create the range for.
     * @return New range.
     */
    public static ColumnFamilyRange fullRange(ColumnFamily columnFamily) {
        return new ColumnFamilyRange(columnFamily, null, null);
    }

    /**
     * Creates a range of keys in a Column Family.
     *
     * @param columnFamily Column Family to create the range for.
     * @param lowerBound Lower bound (inclusive).
     * @param upperBound Upper bound (exclusive).
     * @return New range.
     */
    public static ColumnFamilyRange range(ColumnFamily columnFamily, byte[] lowerBound, byte[] upperBound) {
        assert lowerBound != null && upperBound != null;

        return new ColumnFamilyRange(columnFamily, lowerBound, upperBound);
    }

    /**
     * Returns the Column Family instance this range was created for.
     *
     * @return Column Family instance this range was created for.
     */
    public ColumnFamily columnFamily() {
        return columnFamily;
    }

    /**
     * Returns the lower range bound or {@code null} if this range comprises the whole key set of a Column Family.
     *
     * @return The lower range bound or {@code null} if this range comprises the whole key set of a Column Family.
     * @see #isFullRange()
     */
    public byte @Nullable [] lowerBound() {
        return lowerBound;
    }

    /**
     * Returns the upper range bound or {@code null} if this range comprises the whole key set of a Column Family.
     *
     * @return The upper range bound or {@code null} if this range comprises the whole key set of a Column Family.
     * @see #isFullRange()
     */
    public byte @Nullable [] upperBound() {
        return upperBound;
    }

    /**
     * Returns {@code true} if this range covers the whole key set of a Column Family or {@code false} otherwise.
     *
     * @return {@code true} if this range covers the whole key set of a Column Family or {@code false} otherwise.
     */
    public boolean isFullRange() {
        return lowerBound == null && upperBound == null;
    }
}
