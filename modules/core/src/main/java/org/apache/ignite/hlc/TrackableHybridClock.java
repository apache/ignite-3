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

package org.apache.ignite.hlc;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.Iterator;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import org.jetbrains.annotations.NotNull;

/**
 * Hybrid clock that allows to wait for certain timestamp.
 */
public class TrackableHybridClock extends HybridClock {
    /** Set of timestamp futures. */
    private final SortedSet<TimestampFuture> timestampFutures = new ConcurrentSkipListSet<>();

    /** {@inheritDoc} */
    @Override
    protected void onUpdate(HybridTimestamp timestamp) {
        completeTimestampFutures(timestamp);
    }

    /**
     * Complete all waiting timestamp futures that have a timestamp that is lower or equal to the given one.
     *
     * @param timestamp Timestamp.
     */
    private void completeTimestampFutures(HybridTimestamp timestamp) {
        for (Iterator<TimestampFuture> iterator = timestampFutures.iterator(); iterator.hasNext();) {
            TimestampFuture tf = iterator.next();

            if (timestamp.compareTo(tf.timestamp()) >= 0) {
                tf.complete();

                iterator.remove();
            } else {
                break;
            }
        }
    }

    /**
     * Provides the future that is completed when this clock's latest timestamp becomes higher or equal to the given one.
     *
     * @param timestamp Timestamp to wait.
     * @return Future.
     */
    public CompletableFuture<Void> waitFor(HybridTimestamp timestamp) {
        if (latestTime.compareTo(timestamp) >= 0) {
            return completedFuture(null);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        TimestampFuture timestampFuture = new TimestampFuture(timestamp, future);
        timestampFutures.add(timestampFuture);

        if (latestTime.compareTo(timestamp) >= 0) {
            future.complete(null);

            timestampFutures.remove(timestampFuture);
        }

        return future;
    }

    /**
     * Future that contains a timestamp and should be completed when this clock reaches this timestamp.
     */
    private static class TimestampFuture implements Comparable<TimestampFuture> {
        /** Timestamp. */
        private final HybridTimestamp timestamp;

        /** Internal future. */
        private final CompletableFuture<Void> future;

        /** Unique uuid, for identifying timestamp futures in case of identical timestamps. */
        private final UUID uuid = UUID.randomUUID();

        public TimestampFuture(HybridTimestamp timestamp, CompletableFuture<Void> future) {
            this.timestamp = timestamp;
            this.future = future;
        }

        public HybridTimestamp timestamp() {
            return timestamp;
        }

        public void complete() {
            future.complete(null);
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull TrackableHybridClock.TimestampFuture tf) {
            int res = timestamp.compareTo(tf.timestamp);

            if (res == 0) {
                return uuid.compareTo(tf.uuid);
            }

            return res;
        }
    }
}
