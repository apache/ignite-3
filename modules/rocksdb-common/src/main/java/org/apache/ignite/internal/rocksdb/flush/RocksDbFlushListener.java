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

package org.apache.ignite.internal.rocksdb.flush;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.rocksdb.LoggingRocksDbFlushListener;
import org.rocksdb.FlushJobInfo;
import org.rocksdb.RocksDB;

/**
 * Represents a listener of RocksDB flush events.
 */
class RocksDbFlushListener extends LoggingRocksDbFlushListener {
    /** Flusher instance. */
    private final RocksDbFlusher flusher;

    /** Write-ahead log synchronizer. */
    private final LogSyncer logSyncer;

    private final FailureProcessor failureProcessor;

    /**
     * Future that guarantees that last flush was fully processed and the new flush can safely begin.
     */
    private volatile CompletableFuture<?> lastFlushProcessed = nullCompletedFuture();

    /**
     * Constructor.
     *
     * @param name Listener name, for logs.
     * @param nodeName Node name, for logs.
     * @param flusher Flusher instance to delegate events processing to.
     * @param logSyncer Write-ahead log synchronizer.
     */
    RocksDbFlushListener(String name, String nodeName, RocksDbFlusher flusher, LogSyncer logSyncer, FailureProcessor failureProcessor) {
        super(name, nodeName);

        this.flusher = flusher;
        this.logSyncer = logSyncer;
        this.failureProcessor = failureProcessor;
    }

    @Override
    protected void onFlushBeginCallback(RocksDB db, FlushJobInfo flushJobInfo) {
        lastFlushProcessed.join();

        try {
            logSyncer.sync();
        } catch (Exception e) {
            failureProcessor.process(new FailureContext(e, "Couldn't sync RocksDB WAL on flush begin"));
        }
    }

    @Override
    protected void onFlushCompletedCallback(RocksDB db, FlushJobInfo flushJobInfo) {
        lastFlushProcessed = flusher.onFlushCompleted();
    }

    @Override
    public void onFlushCompleted(RocksDB db, FlushJobInfo flushJobInfo) {
        super.onFlushCompleted(db, flushJobInfo);

        // Do it for every column family, there's no way to tell in advance which one has the latest sequence number.
        lastFlushProcessed.whenCompleteAsync((o, throwable) -> flusher.completeFutures(flushJobInfo.getLargestSeqno()), flusher.threadPool);
    }
}
