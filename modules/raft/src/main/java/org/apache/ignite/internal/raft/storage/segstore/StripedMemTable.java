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

import static org.apache.ignite.internal.util.IgniteUtils.safeAbs;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Thread-safe memtable implementation using group-based stripes to reduce contention.
 */
class StripedMemTable extends AbstractMemTable {
    private static class Stripe {
        /** Map from group ID to SegmentInfo. */
        private final ConcurrentMap<Long, SegmentInfo> memTable = new ConcurrentHashMap<>();
    }

    private final Stripe[] stripes;

    StripedMemTable(int stripes) {
        this.stripes = new Stripe[stripes];

        for (int i = 0; i < stripes; i++) {
            this.stripes[i] = new Stripe();
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>This method is not thread-safe wrt concurrent writes, because it is expected to be used when no writes are happening anymore.
     */
    @Override
    public int numGroups() {
        int result = 0;

        for (Stripe stripe : stripes) {
            result += stripe.memTable.size();
        }

        return result;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This method is not thread-safe wrt concurrent writes, because it is expected to be used when no writes are happening anymore.
     */
    @Override
    public Iterator<Entry<Long, SegmentInfo>> iterator() {
        return new SegmentInfoIterator();
    }

    private Stripe stripe(long groupId) {
        // FIXME: We should calculate stripes the same way it is done in StripedDisruptor,
        //  see https://issues.apache.org/jira/browse/IGNITE-26907
        int stripeIndex = safeAbs(Long.hashCode(groupId) % stripes.length);

        return stripes[stripeIndex];
    }

    @Override
    protected Map<Long, SegmentInfo> memtable(long groupId) {
        return stripe(groupId).memTable;
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
