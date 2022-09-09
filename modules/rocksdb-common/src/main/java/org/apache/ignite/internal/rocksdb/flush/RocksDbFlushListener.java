/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.rocksdb.flush;

import static org.rocksdb.AbstractEventListener.EnabledEventCallback.ON_FLUSH_BEGIN;
import static org.rocksdb.AbstractEventListener.EnabledEventCallback.ON_FLUSH_COMPLETED;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.FlushJobInfo;
import org.rocksdb.RocksDB;

/**
 * Represents a listener of RocksDB flush events.
 */
class RocksDbFlushListener extends AbstractEventListener {
    /** Flusher instance. */
    private final RocksDbFlusher flusher;

    /**
     * Type of last processed event. Real amount of events doesn't matter in atomic flush mode. All "completed" events go after all "begin"
     * events, and vice versa.
     */
    private final AtomicReference<EnabledEventCallback> lastEventType = new AtomicReference<>(ON_FLUSH_COMPLETED);

    /**
     * Future that guarantees that last flush was fully processed and the new flush can safely begin.
     */
    private volatile CompletableFuture<?> lastFlushProcessed = CompletableFuture.completedFuture(null);

    /**
     * Constructor.
     *
     * @param flusher Flusher instance to delegate events processing to.
     */
    RocksDbFlushListener(RocksDbFlusher flusher) {
        super(EnabledEventCallback.ON_FLUSH_BEGIN, EnabledEventCallback.ON_FLUSH_COMPLETED);

        this.flusher = flusher;
    }

    /** {@inheritDoc} */
    @Override
    public void onFlushBegin(RocksDB db, FlushJobInfo flushJobInfo) {
        if (lastEventType.compareAndSet(ON_FLUSH_COMPLETED, ON_FLUSH_BEGIN)) {
            lastFlushProcessed.join();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onFlushCompleted(RocksDB db, FlushJobInfo flushJobInfo) {
        ExecutorService threadPool = flusher.threadPool;

        if (lastEventType.compareAndSet(ON_FLUSH_BEGIN, ON_FLUSH_COMPLETED)) {
            lastFlushProcessed = CompletableFuture.runAsync(flusher.onFlushCompleted, threadPool);
        }

        // Do it for every column family, there's no way to tell in advance which one has the latest sequence number.
        lastFlushProcessed.whenCompleteAsync((o, throwable) -> flusher.completeFutures(flushJobInfo.getLargestSeqno()), threadPool);
    }
}
