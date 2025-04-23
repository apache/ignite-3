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

package org.apache.ignite.internal.metastorage.server;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.CompletableFutures;

/**
 * Tracker of read operations from metastorage or its storage. Used to track the completion of read operations before start local
 * compaction of metastorage.
 *
 * <p>Expected usage:</p>
 * <ul>
 *     <li>Before starting execution, the reading command invokes {@link #track} with the lowest estimation for revision upper bound.
 *     <li>After completion, the reading command will invoke {@link TrackingToken#close()} on an instance returned by {@link #track},
 *     regardless of whether the operation was successful or not.</li>
 *     <li>{@link #collect} will be invoked only after a new compaction revision has been set
 *     ({@link KeyValueStorage#setCompactionRevision}) for a new compaction revision.</li>
 * </ul>
 */
public class ReadOperationForCompactionTracker {
    private final Map<ReadOperationKey, CompletableFuture<Void>> readOperationFutureByKey = new ConcurrentHashMap<>();

    private final AtomicLong longOperationIdGenerator = new AtomicLong();

    /**
     * Token to stop tracking the read operation.
     *
     * @see #track(long, LongSupplier, LongSupplier)
     */
    @FunctionalInterface
    public interface TrackingToken extends AutoCloseable {
        @Override
        void close();
    }

    /**
     * Starts tracking the completion of a read operation on its lowest estimation for revision upper bound.
     *
     * <p>Expected usage pattern:</p>
     * <pre><code>
     *     int operationRevision = ...;
     *
     *     try (var token = tracker.track(operationRevision, storage::revision, storage::getCompactionRevision)) {
     *         doReadOperation(...);
     *     }
     * </code></pre>
     *
     * <p>Or you can use an explicit {@link TrackingToken#close()} call if execution context requires that, for example if your operation is
     * asynchronous.
     *
     * @see TrackingToken
     */
    public TrackingToken track(
            long operationRevision, LongSupplier latestRevision, LongSupplier compactedRevision
    ) throws CompactedException {
        long operationId = longOperationIdGenerator.getAndIncrement();

        while (true) {
            // "operationRevision" parameter is immutable, because we might need it on a next iteration.
            // If it is asked to track the latest revision, we receive it right here from the corresponding supplier.
            long currentOperationRevision = operationRevision == MetaStorageManager.LATEST_REVISION
                    ? latestRevision.getAsLong()
                    : operationRevision;

            // Value from compacted revision supplier can only grow. We only use it for upper bound checks, so it's safe to read it every
            // time instead of caching it. It applies to all usages of the supplier.
            if (currentOperationRevision <= compactedRevision.getAsLong()) {
                // Latest revision can never be compacted. If for some reason latest revision is concurrently updated and this revision is
                // already compacted, we should retry until we succeed. That's quite a lag, but it is possible in theory and in tests.
                if (operationRevision == MetaStorageManager.LATEST_REVISION) {
                    continue;
                }

                throw new CompactedException(currentOperationRevision, compactedRevision.getAsLong());
            }

            var key = new ReadOperationKey(operationId, currentOperationRevision);

            TrackingToken trackingToken = () -> {
                CompletableFuture<Void> removed = readOperationFutureByKey.remove(key);

                // Might be null, that's fine, "close" can be called multiple times.
                if (removed != null) {
                    removed.complete(null);
                }
            };

            CompletableFuture<Void> operationFuture = new CompletableFuture<>();
            readOperationFutureByKey.put(key, operationFuture);

            // If this condition passes, it is possible that a future returned by "collect" in a compaction thread is already completed.
            // It is not safe to proceed with tracking, we have to invalidate current token and either retry, or throw an exception.
            if (currentOperationRevision <= compactedRevision.getAsLong()) {
                trackingToken.close();

                if (operationRevision == MetaStorageManager.LATEST_REVISION) {
                    continue;
                }

                throw new CompactedException(currentOperationRevision, compactedRevision.getAsLong());
            }

            return trackingToken;
        }
    }

    /**
     * Collects all read operations that were started before {@code compactionRevision} and returns a future that will complete
     * when all collected operations complete.
     *
     * <p>Future completes without exception.</p>
     */
    public CompletableFuture<Void> collect(long compactionRevision) {
        return readOperationFutureByKey.entrySet().stream()
                .filter(entry -> entry.getKey().operationRevision <= compactionRevision)
                .map(Entry::getValue)
                .collect(collectingAndThen(toList(), CompletableFutures::allOf));
    }

    private static class ReadOperationKey {
        @IgniteToStringInclude
        private final long readOperationId;

        private final long operationRevision;

        private ReadOperationKey(long readOperationId, long operationRevision) {
            assert operationRevision >= 0 : operationRevision;

            this.readOperationId = readOperationId;
            this.operationRevision = operationRevision;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ReadOperationKey that = (ReadOperationKey) o;

            return operationRevision == that.operationRevision && readOperationId == that.readOperationId;
        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(readOperationId);
            result = 31 * result + Long.hashCode(operationRevision);
            return result;
        }

        @Override
        public String toString() {
            return S.toString(this);
        }
    }
}
