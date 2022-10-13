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

package org.apache.ignite.internal.storage.rocksdb.index;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.internal.storage.index.BinaryTupleComparator;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.rocksdb.AbstractComparator;
import org.rocksdb.ComparatorOptions;

/**
 * {@link AbstractComparator} implementation that compares Binary Tuples.
 */
public class RocksDbBinaryTupleComparator extends AbstractComparator {
    private final BinaryTupleComparator comparator;

    /** Options needed for resource management. */
    private final ComparatorOptions options;

    /**
     * Constructor.
     *
     * @param descriptor Sorted Index descriptor.
     */
    public RocksDbBinaryTupleComparator(SortedIndexDescriptor descriptor) {
        this(descriptor, new ComparatorOptions());
    }

    private RocksDbBinaryTupleComparator(SortedIndexDescriptor descriptor, ComparatorOptions options) {
        super(options);

        this.options = options;
        this.comparator = new BinaryTupleComparator(descriptor);
    }

    @Override
    public String name() {
        return getClass().getCanonicalName();
    }

    @Override
    public int compare(ByteBuffer a, ByteBuffer b) {
        int comparePartitionId = Short.compareUnsigned(a.getShort(), b.getShort());

        if (comparePartitionId != 0) {
            return comparePartitionId;
        }

        ByteBuffer firstBinaryTupleBuffer = a.slice().order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer secondBinaryTupleBuffer = b.slice().order(ByteOrder.LITTLE_ENDIAN);

        // Handle partition bounds.
        if (!firstBinaryTupleBuffer.hasRemaining()) {
            return -1;
        }

        if (!secondBinaryTupleBuffer.hasRemaining()) {
            return 1;
        }

        int compareTuples = comparator.compare(firstBinaryTupleBuffer, secondBinaryTupleBuffer);

        return compareTuples == 0 ? compareRowIds(a, b) : compareTuples;
    }

    private static int compareRowIds(ByteBuffer a, ByteBuffer b) {
        long firstMostSignBits = a.getLong(a.limit() - Long.BYTES * 2);
        long secondMostSignBits = b.getLong(b.limit() - Long.BYTES * 2);

        int compare = Long.compare(firstMostSignBits, secondMostSignBits);

        if (compare != 0) {
            return compare;
        }

        long firstLeastSignBits = a.getLong(a.limit() - Long.BYTES);
        long secondLeastSignBits = b.getLong(b.limit() - Long.BYTES);

        return Long.compare(firstLeastSignBits, secondLeastSignBits);
    }

    @Override
    public void close() {
        super.close();

        options.close();
    }
}
