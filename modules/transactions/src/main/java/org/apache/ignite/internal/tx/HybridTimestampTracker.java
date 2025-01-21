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

package org.apache.ignite.internal.tx;

import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;
import static org.apache.ignite.internal.hlc.HybridTimestamp.nullableHybridTimestamp;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;

/**
 * Interface is used to provide a track timestamp into a transaction operation.
 */
public interface HybridTimestampTracker {
    /** This tracker do nothing.*/
    HybridTimestampTracker EMPTY_TS_PROVIDER = new HybridTimestampTracker() {
        @Override
        public @Nullable HybridTimestamp get() {
            return null;
        }

        @Override
        public void update(@Nullable HybridTimestamp ts) {
        }
    };

    /**
     * Returns an empty HybridTimestampTracker instance that performs no operations.
     *
     * @return A HybridTimestampTracker instance that does nothing for both retrieval and update operations.
     */
    static HybridTimestampTracker emptyTracker() {
        return EMPTY_TS_PROVIDER;
    }

    /**
     * Creates an atomic HybridTimestampTracker instance that uses an {@link AtomicLong} to track and update the timestamp.
     *
     * @param intTs The initial HybridTimestamp, or null if no initial timestamp is provided.
     * @return A HybridTimestampTracker instance for tracking and updating a hybrid timestamp atomically.
     */
    static HybridTimestampTracker atomicTracker(@Nullable HybridTimestamp intTs) {
        return new HybridTimestampTracker() {
            /** Timestamp. */
            private final AtomicLong timestamp = new AtomicLong(hybridTimestampToLong(intTs));

            @Override
            public @Nullable HybridTimestamp get() {
                return nullableHybridTimestamp(timestamp.get());
            }

            @Override
            public void update(@Nullable HybridTimestamp ts) {
                long tsVal = hybridTimestampToLong(ts);

                timestamp.updateAndGet(x -> Math.max(x, tsVal));
            }
        };
    }

    /**
     * Creates a client-managed HybridTimestampTracker based on a given initial timestamp and an update consumer.
     *
     * @param intTs The initial HybridTimestamp, or null if no initial timestamp is provided.
     * @param updateTs A Consumer that accepts a HybridTimestamp for managing updates to the timestamp.
     * @return A HybridTimestampTracker instance that uses the provided initial timestamp and update mechanism.
     */
    static HybridTimestampTracker clientTracker(@Nullable HybridTimestamp intTs, Consumer<HybridTimestamp> updateTs) {
        return new HybridTimestampTracker() {
            @Override
            public @Nullable HybridTimestamp get() {
                return intTs;
            }

            @Override
            public void update(@Nullable HybridTimestamp ts) {
                updateTs.accept(ts);
            }
        };
    }

    /**
     * Get the observable timestamp.
     *
     * @return Hybrid timestamp.
     */
    @Nullable HybridTimestamp get();

    /**
     * Updates the observable timestamp after an operation is executed.
     *
     * @param ts Hybrid timestamp.
     */
    void update(@Nullable HybridTimestamp ts);
}
