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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
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

    /**
     * Returns the number of Raft Group IDs stored in this memtable.
     */
    int numGroups() {
        int result = 0;

        for (Stripe stripe : stripes) {
            result += stripe.memTable.size();
        }

        return result;
    }

    /**
     * Returns an iterator over all {@code Group ID -> SegmentInfo} entries in this memtable.
     */
    Iterator<Entry<Long, SegmentInfo>> iterator() {
        return new SegmentInfoIterator();
    }

    private Stripe stripe(long groupId) {
        int stripeIndex = Long.hashCode(groupId) % stripes.length;

        return stripes[stripeIndex];
    }

    private class SegmentInfoIterator implements Iterator<Entry<Long, SegmentInfo>> {
        private int stripeIndex = 0;

        private Iterator<Entry<Long, SegmentInfo>> mapIterator = refreshIterator();

        @Override
        public boolean hasNext() {
            if (mapIterator.hasNext()) {
                return true;
            }

            if (stripeIndex < stripes.length) {
                mapIterator = refreshIterator();

                return hasNext();
            }

            return false;
        }

        @Override
        public Entry<Long, SegmentInfo> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            return mapIterator.next();
        }

        private Iterator<Entry<Long, SegmentInfo>> refreshIterator() {
            Stripe nextStripe = stripes[stripeIndex++];

            return nextStripe.memTable.entrySet().iterator();
        }
    }
}
