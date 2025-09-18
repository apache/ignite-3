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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Mutable index memtable.
 *
 * <p>This class represents an in-memory index of the current segment file used by a {@link SegmentFileManager}. Index is
 * essentially a mapping from {@code [groupId, logIndex]} to the offset in the segment file where the corresponding log entry is stored.
 *
 * <p>It is expected that entries for each {@code groupId} are written by one thread, therefore concurrent writes to the same
 * {@code groupId} are not safe. However, reads from multiple threads are safe in relation to the aforementioned writes.
 */
class IndexMemTable {
    private static class Stripe {
        /** Map from group ID to SegmentInfo. */
        private final ConcurrentMap<Long, SegmentInfo> memTable = new ConcurrentHashMap<>();
    }

    private static class SegmentInfo {
        private static final VarHandle SEGMENT_FILE_OFFSETS_VH;

        private static final int INITIAL_SEGMENT_FILE_OFFSETS_CAPACITY = 10;

        static {
            try {
                SEGMENT_FILE_OFFSETS_VH = MethodHandles.lookup()
                        .findVarHandle(SegmentInfo.class, "segmentFileOffsets", int[].class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        /**
         * Base log index. All log indexes in this memtable lie in the
         * {@code [logIndexBase, logIndexBase + segmentFileOffsets.length]} range.
         */
        private final long logIndexBase;

        /**
         * Offsets in a segment file.
         */
        @SuppressWarnings("FieldMayBeFinal") // Updated through a VarHandle.
        private volatile int[] segmentFileOffsets = new int[INITIAL_SEGMENT_FILE_OFFSETS_CAPACITY];

        /**
         * Number of entries in the {@link #segmentFileOffsets} array.
         *
         * <p>Multi-threaded visibility is guaranteed by volatile reads or writes to the {@link #segmentFileOffsets} field.
         */
        private int segmentFileOffsetSize = 0;

        SegmentInfo(long logIndexBase) {
            this.logIndexBase = logIndexBase;
        }

        void addOffset(long logIndex, int segmentFileOffset) {
            int[] originalSegmentFileOffsets = this.segmentFileOffsets;

            int[] segmentFileOffsets = originalSegmentFileOffsets;

            // Check that log indexes are monotonically increasing.
            assert segmentFileOffsetSize == logIndex - logIndexBase :
                    String.format("Log indexes are not monotonically increasing [logIndex=%d, expectedLogIndex=%d].",
                            logIndex, logIndexBase + segmentFileOffsetSize);

            if (segmentFileOffsets.length == segmentFileOffsetSize) {
                segmentFileOffsets = Arrays.copyOf(segmentFileOffsets, segmentFileOffsets.length * 2);
            }

            segmentFileOffsets[segmentFileOffsetSize++] = segmentFileOffset;

            // Simple assignment would suffice, since we only have one thread writing to this field, but we use compareAndSet to verify
            // this invariant, just in case.
            boolean updated = SEGMENT_FILE_OFFSETS_VH.compareAndSet(this, originalSegmentFileOffsets, segmentFileOffsets);

            assert updated : "Concurrent writes detected";
        }

        int getOffset(long logIndex) {
            long offsetIndex = logIndex - logIndexBase;

            if (offsetIndex < 0) {
                return 0;
            }

            // Read segmentFileOffsets first to acquire segmentFileOffsetSize.
            int[] segmentFileOffsets = this.segmentFileOffsets;

            if (offsetIndex >= segmentFileOffsetSize) {
                return 0;
            }

            return segmentFileOffsets[(int) offsetIndex];
        }
    }

    private final Stripe[] stripes;

    IndexMemTable(int stripes) {
        this.stripes = new Stripe[stripes];

        for (int i = 0; i < stripes; i++) {
            this.stripes[i] = new Stripe();
        }
    }

    void appendSegmentFileOffset(long groupId, long logIndex, int segmentFileOffset) {
        // File offset can be less than 0 (it's treated as an unsigned integer) but never 0, because of the file header.
        assert segmentFileOffset != 0 : String.format("Segment file offset must not be 0 [groupId=%d]", groupId);

        SegmentInfo segmentInfo = stripe(groupId).memTable.computeIfAbsent(groupId, id -> new SegmentInfo(logIndex));

        segmentInfo.addOffset(logIndex, segmentFileOffset);
    }

    /**
     * Returns the offset in the segment file where the log entry with the given {@code logIndex} is stored or {@code 0} if the log entry
     * was not found in the memtable.
     */
    int getSegmentFileOffset(long groupId, long logIndex) {
        SegmentInfo segmentInfo = stripe(groupId).memTable.get(groupId);

        return segmentInfo == null ? 0 : segmentInfo.getOffset(logIndex);
    }

    private Stripe stripe(long groupId) {
        int stripeIndex = Long.hashCode(groupId) % stripes.length;

        return stripes[stripeIndex];
    }
}
