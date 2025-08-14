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

package org.apache.ignite.internal.table.distributed;

import java.util.PrimitiveIterator.OfInt;
import java.util.stream.IntStream;
import org.apache.ignite.internal.tostring.S;

/**
 * Represents a collection of partition IDs.
 */
public interface PartitionSet {
    PartitionSet EMPTY_SET = new PartitionSet() {
        @Override
        public void set(int partitionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear(int partitionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean get(int partitionId) {
            return false;
        }

        @Override
        public IntStream stream() {
            return IntStream.empty();
        }

        @Override
        public PartitionSet copy() {
            return new BitSetPartitionSet();
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public int hashCode() {
            return getHashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return isEqual(obj);
        }

        @Override
        public String toString() {
            return "PartitionSet(empty)";
        }
    };

    /**
     * Adds the partition to the partition set.
     *
     * @param partitionId Partition ID.
     */
    void set(int partitionId);

    /**
     * Removes the partition from the partition set.
     *
     * @param partitionId Partition ID.
     */
    void clear(int partitionId);

    /**
     * Returns {@code true} if partition with {@code partitionId} is present in this set, {@code false} otherwise.
     *
     * @param partitionId Partition ID.
     */
    boolean get(int partitionId);

    /**
     * Returns a stream of partition IDs in this set.
     * The IDs are returned in order, from lowest to highest.
     */
    IntStream stream();

    /**
     * Returns count of partitions.
     */
    int size();

    /**
     * Returns a copy of this {@link PartitionSet}.
     */
    PartitionSet copy();

    /**
     * Returns {@code true} if this partition set is equal to the parameter, {@code false} otherwise.
     *
     * @param another Object to be compared for equality with this partition set.
     */
    default boolean isEqual(Object another) {
        if (!(another instanceof PartitionSet)) {
            return false;
        }

        PartitionSet anotherSet = (PartitionSet) another;

        if (size() != anotherSet.size()) {
            return false;
        }

        OfInt iterator1 = stream().iterator();
        OfInt iterator2 = anotherSet.stream().iterator();

        while (iterator1.hasNext() && iterator2.hasNext()) {
            if (iterator1.nextInt() != iterator2.nextInt()) {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns the hash code value for this partition set.
     */
    default int getHashCode() {
        int h = 0;

        OfInt iter = stream().iterator();

        while (iter.hasNext()) {
            int idx = iter.nextInt();

            h += idx;
        }

        return h;
    }

    /**
     * Returns an immutable set of partition IDs containing a single value.
     *
     * @param partId Partition ID.
     */
    static PartitionSet of(int partId) {
        return new PartitionSet() {
            @Override
            public void set(int partitionId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void clear(int partitionId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean get(int partitionId) {
                return partitionId == partId;
            }

            @Override
            public IntStream stream() {
                return IntStream.of(partId);
            }

            @Override
            public int size() {
                return 1;
            }

            @Override
            public PartitionSet copy() {
                return of(partId);
            }

            @Override
            public int hashCode() {
                return getHashCode();
            }

            @Override
            public boolean equals(Object o) {
                return isEqual(o);
            }

            @Override
            public String toString() {
                return S.toString(PartitionSet.class, this, "partitionId=" + partId);
            }
        };
    }
}
