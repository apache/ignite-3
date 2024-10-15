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
import org.apache.ignite.internal.metastorage.MetaStorageCompactionManager;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.CompletableFutures;

/**
 * Tracker of read operations from metastorage or its storage. Used to track the completion of read operations before start local
 * compaction of metastorage.
 *
 * <p>Thread safe, but component that uses it must synchronize {@link #track} and {@link #collect} the with each other.</p>
 *
 * <p>Expected usage:</p>
 * <ul>
 *     <li>Before starting execution, the reading command invoke {@link #track} with its ID and the compaction revision that is currently
 *     set ({@link MetaStorageCompactionManager#setCompactionRevisionLocally}/{@link KeyValueStorage#setCompactionRevision}).</li>
 *     <li>After completion, the reading command will invoke {@link #untrack} with the same arguments as when calling {@link #track},
 *     regardless of whether the operation was successful or not.</li>
 *     <li>{@link #collect} will be invoked only after a new compaction revision has been set
 *     ({@link MetaStorageCompactionManager#setCompactionRevisionLocally}/{@link KeyValueStorage#setCompactionRevision}) for a new
 *     compaction revision.</li>
 * </ul>
 */
public class ReadOperationForCompactionTracker {
    private final Map<ReadOperationKey, CompletableFuture<Void>> readOperationFutureByKey = new ConcurrentHashMap<>();

    /**
     * Starts tracking the completion of a read operation on the current compaction revision.
     *
     * <p>Method is expected not to be called more than once for the same arguments.</p>
     *
     * <p>Expected usage pattern:</p>
     * <pre><code>
     *     Object readOperationId = ...;
     *     int compactionRevision = ...;
     *
     *     tracker.track(readOperationId, compactionRevision);
     *
     *     try {
     *         doReadOperation(...);
     *     } finally {
     *         tracker.untrack(readOperationId, compactionRevision);
     *     }
     * </code></pre>
     *
     * @see #untrack(Object, long)
     */
    public void track(Object readOperationId, long compactionRevision) {
        var key = new ReadOperationKey(readOperationId, compactionRevision);

        CompletableFuture<Void> previous = readOperationFutureByKey.putIfAbsent(key, new CompletableFuture<>());

        assert previous == null : key;
    }

    /**
     * Stops tracking the read operation on the compaction revision on which tracking start.
     *
     * <p>Method is expected not to be called more than once for the same arguments, and {@link #track} was previously called for same
     * arguments.</p>
     *
     * @see #track(Object, long)
     */
    public void untrack(Object readOperationId, long compactionRevision) {
        var key = new ReadOperationKey(readOperationId, compactionRevision);

        CompletableFuture<Void> removed = readOperationFutureByKey.remove(key);

        assert removed != null : key;

        removed.complete(null);
    }

    /**
     * Collects all read operations that were started before {@code compactionRevisionExcluded} and returns a future that will complete
     * when all collected operations complete.
     *
     * <p>Future completes without exception.</p>
     */
    public CompletableFuture<Void> collect(long compactionRevisionExcluded) {
        return readOperationFutureByKey.entrySet().stream()
                .filter(entry -> entry.getKey().compactionRevision < compactionRevisionExcluded)
                .map(Entry::getValue)
                .collect(collectingAndThen(toList(), CompletableFutures::allOf));
    }

    private static class ReadOperationKey {
        @IgniteToStringInclude
        private final Object readOperationId;

        private final long compactionRevision;

        private ReadOperationKey(Object readOperationId, long compactionRevision) {
            this.readOperationId = readOperationId;
            this.compactionRevision = compactionRevision;
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

            return compactionRevision == that.compactionRevision && readOperationId.equals(that.readOperationId);
        }

        @Override
        public int hashCode() {
            int result = readOperationId.hashCode();
            result = 31 * result + (int) (compactionRevision ^ (compactionRevision >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return S.toString(this);
        }
    }
}
