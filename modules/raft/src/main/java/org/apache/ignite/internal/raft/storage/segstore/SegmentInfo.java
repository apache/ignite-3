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

package org.apache.ignite.internal.raft.storage.segstore;

import static java.lang.Math.toIntExact;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Information about a segment file for single Raft Group stored in a {@link IndexMemTable}.
 */
class SegmentInfo {
    private static class ArrayWithSize {
        private static final int INITIAL_CAPACITY = 10;

        private final int[] array;

        private final int size;

        ArrayWithSize() {
            this(new int[INITIAL_CAPACITY], 0);
        }

        private ArrayWithSize(int[] array, int size) {
            this.array = array;
            this.size = size;
        }

        ArrayWithSize add(int element) {
            // The array can be shared between multiple instances, but since it always grows and we read at most "size" elements,
            // we don't need to copy it every time.
            int[] array = this.array;

            if (size == array.length) {
                array = Arrays.copyOf(array, array.length * 2);
            }

            array[size] = element;

            return new ArrayWithSize(array, size + 1);
        }

        ArrayWithSize truncateSuffix(int newSize) {
            return truncate(0, newSize);
        }

        ArrayWithSize truncatePrefix(int newSize) {
            int srcPos = size - newSize;

            return truncate(srcPos, newSize);
        }

        private ArrayWithSize truncate(int srcPos, int newSize) {
            assert newSize <= size
                    : String.format("Array must shrink on truncation, current size: %d, size after truncation: %d", size, newSize);

            int[] newArray = new int[array.length];

            System.arraycopy(array, srcPos, newArray, 0, newSize);

            return new ArrayWithSize(newArray, newSize);
        }

        int get(int index) {
            return array[index];
        }

        int size() {
            return size;
        }
    }

    private static final VarHandle SEGMENT_FILE_OFFSETS_VH;

    static {
        try {
            SEGMENT_FILE_OFFSETS_VH = MethodHandles.lookup()
                    .findVarHandle(SegmentInfo.class, "segmentFileOffsets", ArrayWithSize.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Base log index. All log indexes in this memtable lie in the {@code [logIndexBase, logIndexBase + segmentFileOffsets.size)} range.
     */
    private final long logIndexBase;

    private final long firstIndexKept;

    /**
     * Offsets in a segment file.
     */
    @SuppressWarnings("FieldMayBeFinal") // Updated through a VarHandle.
    private volatile ArrayWithSize segmentFileOffsets;

    SegmentInfo(long logIndexBase) {
        this(logIndexBase, -1, new ArrayWithSize());
    }

    SegmentInfo(long logIndexBase, long firstIndexKept) {
        this(logIndexBase, firstIndexKept, new ArrayWithSize());
    }

    static SegmentInfo prefixTombstone(long firstIndexKept) {
        return new SegmentInfo(-1, firstIndexKept);
    }

    private SegmentInfo(long logIndexBase, long firstIndexKept, ArrayWithSize segmentFileOffsets) {
        this.logIndexBase = logIndexBase;
        this.firstIndexKept = firstIndexKept;
        this.segmentFileOffsets = segmentFileOffsets;
    }

    /**
     * Puts the given segment file offset under the given log index.
     */
    void addOffset(long logIndex, int segmentFileOffset) {
        ArrayWithSize segmentFileOffsets = this.segmentFileOffsets;

        // Check that log indexes are monotonically increasing.
        assert segmentFileOffsets.size() == logIndex - logIndexBase :
                String.format("Log indexes are not monotonically increasing [logIndex=%d, expectedLogIndex=%d].",
                        logIndex, logIndexBase + segmentFileOffsets.size());

        setSegmentFileOffsets(segmentFileOffsets, segmentFileOffsets.add(segmentFileOffset));
    }

    /**
     * Returns the segment file offset for the given log index or {@code 0} if the log index was not found.
     */
    int getOffset(long logIndex) {
        long offsetIndex = logIndex - logIndexBase;

        if (offsetIndex < 0) {
            return 0;
        }

        ArrayWithSize segmentFileOffsets = this.segmentFileOffsets;

        if (offsetIndex >= segmentFileOffsets.size()) {
            return 0;
        }

        return segmentFileOffsets.get((int) offsetIndex);
    }

    /**
     * Returns the inclusive lower bound of log indices stored in this memtable.
     */
    long firstLogIndexInclusive() {
        return logIndexBase;
    }

    /**
     * Returns the inclusive upper bound of log indices stored in this memtable.
     */
    long lastLogIndexExclusive() {
        return logIndexBase + segmentFileOffsets.size();
    }

    /**
     * Returns the log index used during prefix truncation or {@code -1} if no prefix truncation was issued.
     */
    long firstIndexKept() {
        return firstIndexKept;
    }

    boolean isPrefixTombstone() {
        return logIndexBase == -1;
    }

    /**
     * Returns the number of offsets stored in this memtable.
     */
    int size() {
        return segmentFileOffsets.size();
    }

    /**
     * Serializes the offsets to the given byte buffer.
     */
    void saveOffsetsTo(ByteBuffer buffer) {
        ArrayWithSize offsets = segmentFileOffsets;

        assert offsets.size() > 0 : "Offsets array must not be empty";

        buffer.asIntBuffer().put(offsets.array, 0, offsets.size);
    }

    /**
     * Removes all data which log indices are strictly greater than {@code lastLogIndexKept}.
     */
    SegmentInfo truncateSuffix(long lastLogIndexKept) {
        assert lastLogIndexKept >= logIndexBase : String.format("logIndexBase=%d, lastLogIndexKept=%d", logIndexBase, lastLogIndexKept);

        ArrayWithSize segmentFileOffsets = this.segmentFileOffsets;

        long lastLogIndexExclusive = logIndexBase + segmentFileOffsets.size();

        // Not using an assertion here, because this value doesn't come from the storage code.
        if (lastLogIndexKept >= lastLogIndexExclusive) {
            throw new IllegalArgumentException(String.format(
                    "lastLogIndexKept is too large. Last index in memtable: %d, lastLogIndexKept: %d",
                    lastLogIndexExclusive - 1, lastLogIndexKept
            ));
        }

        int newSize = toIntExact(lastLogIndexKept - logIndexBase + 1);

        setSegmentFileOffsets(segmentFileOffsets, segmentFileOffsets.truncateSuffix(newSize));

        // This could have been a "void" method, but this way it looks consistent with "truncatePrefix".
        return this;
    }

    /**
     * Removes all data which log indices are strictly smaller than {@code firstIndexKept}.
     */
    SegmentInfo truncatePrefix(long firstIndexKept) {
        if (isPrefixTombstone()) {
            if (this.firstIndexKept >= firstIndexKept) {
                throw new IllegalStateException(String.format(
                        "Trying to truncate an already truncated prefix [curFirstIndexKept=%d, newFirstIndexKept=%d]",
                        this.firstIndexKept, firstIndexKept
                ));
            }

            return prefixTombstone(firstIndexKept);
        }

        ArrayWithSize segmentFileOffsets = this.segmentFileOffsets;

        if (firstIndexKept < logIndexBase) {
            return new SegmentInfo(logIndexBase, firstIndexKept, segmentFileOffsets);
        }

        long lastLogIndexExclusive = logIndexBase + segmentFileOffsets.size();

        // Not using an assertion here, because this value doesn't come from the storage code.
        if (firstIndexKept >= lastLogIndexExclusive) {
            throw new IllegalArgumentException(String.format(
                    "firstIndexKept is too large. Last index in memtable: %d, firstIndexKept: %d",
                    lastLogIndexExclusive - 1, firstIndexKept
            ));
        }

        int newSize = toIntExact(lastLogIndexExclusive - firstIndexKept);

        return new SegmentInfo(firstIndexKept, firstIndexKept, segmentFileOffsets.truncatePrefix(newSize));
    }

    private void setSegmentFileOffsets(ArrayWithSize segmentFileOffsets, ArrayWithSize newSegmentFileOffsets) {
        // Simple assignment would suffice, since we only have one thread writing to this field, but we use compareAndSet to verify
        // this invariant, just in case.
        boolean updated = SEGMENT_FILE_OFFSETS_VH.compareAndSet(this, segmentFileOffsets, newSegmentFileOffsets);

        assert updated : "Concurrent writes detected";
    }
}
