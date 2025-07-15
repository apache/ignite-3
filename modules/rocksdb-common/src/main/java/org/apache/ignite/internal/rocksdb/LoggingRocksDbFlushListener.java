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

package org.apache.ignite.internal.rocksdb;

import static java.util.stream.Collectors.toList;
import static org.rocksdb.AbstractEventListener.EnabledEventCallback.ON_COMPACTION_BEGIN;
import static org.rocksdb.AbstractEventListener.EnabledEventCallback.ON_COMPACTION_COMPLETED;
import static org.rocksdb.AbstractEventListener.EnabledEventCallback.ON_FLUSH_BEGIN;
import static org.rocksdb.AbstractEventListener.EnabledEventCallback.ON_FLUSH_COMPLETED;

import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.FlushJobInfo;
import org.rocksdb.RocksDB;

/**
 * Represents a listener of RocksDB flush events.
 */
public class LoggingRocksDbFlushListener extends AbstractEventListener {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(LoggingRocksDbFlushListener.class);

    /** Listener name, for logs. */
    private final String name;

    /** Node name, for logs. */
    private final String nodeName;

    /**
     * Type of last processed flush event. Real amount of events doesn't matter in atomic flush mode. All "completed" events go after all
     * "begin" events, and vice versa.
     */
    private final AtomicReference<EnabledEventCallback> lastFlushEventType = new AtomicReference<>(ON_FLUSH_COMPLETED);

    /** Type of last processed compaction event. */
    private final AtomicReference<EnabledEventCallback> lastCompactionEventType = new AtomicReference<>(ON_COMPACTION_COMPLETED);

    /** This field is used for determining flush duration. */
    private volatile long lastFlushStartTimeNanos;

    /** This field is used for determining compaction duration. */
    private volatile long lastCompactionStartTimeNanos;

    /**
     * Constructor.
     *
     * @param name Listener name, for logs.
     * @param nodeName Node name, for logs.
     */
    public LoggingRocksDbFlushListener(String name, String nodeName) {
        super(ON_FLUSH_BEGIN, ON_FLUSH_COMPLETED, ON_COMPACTION_BEGIN, ON_COMPACTION_COMPLETED);

        this.name = name;
        this.nodeName = nodeName;
    }

    @Override
    public void onFlushBegin(RocksDB db, FlushJobInfo flushJobInfo) {
        if (lastFlushEventType.compareAndSet(ON_FLUSH_COMPLETED, ON_FLUSH_BEGIN)) {
            if (LOG.isInfoEnabled()) {
                LOG.info(
                        "Starting rocksdb flush process [name='{}', nodeName='{}', reason={}]",
                        name, nodeName, flushJobInfo.getFlushReason()
                );

                lastFlushStartTimeNanos = System.nanoTime();
            }

            onFlushBeginCallback(db, flushJobInfo);
        }
    }

    @Override
    public void onFlushCompleted(RocksDB db, FlushJobInfo flushJobInfo) {
        if (lastFlushEventType.compareAndSet(ON_FLUSH_BEGIN, ON_FLUSH_COMPLETED)) {
            if (LOG.isInfoEnabled()) {
                long duration = System.nanoTime() - lastFlushStartTimeNanos;

                LOG.info(
                        "Finishing rocksdb flush process [name='{}', nodeName='{}', duration={}ms]",
                        name, nodeName, TimeUnit.NANOSECONDS.toMillis(duration)
                );
            }

            onFlushCompletedCallback(db, flushJobInfo);
        }
    }

    protected void onFlushBeginCallback(RocksDB db, FlushJobInfo flushJobInfo) {
        // No-op.
    }

    protected void onFlushCompletedCallback(RocksDB db, FlushJobInfo flushJobInfo) {
        // No-op.
    }

    @Override
    public void onCompactionBegin(RocksDB db, CompactionJobInfo compactionJobInfo) {
        if (lastCompactionEventType.compareAndSet(ON_COMPACTION_COMPLETED, ON_COMPACTION_BEGIN)) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Starting rocksdb compaction process [name='{}', nodeName='{}', reason={}, input={}, output={}]",
                        name,
                        nodeName,
                        compactionJobInfo.compactionReason(),
                        // Extract file names from full paths.
                        compactionJobInfo.inputFiles().stream().map(path -> Paths.get(path).getFileName()).collect(toList()),
                        compactionJobInfo.outputFiles().stream().map(path -> Paths.get(path).getFileName()).collect(toList())
                );

                lastCompactionStartTimeNanos = System.nanoTime();
            }
        }
    }

    @Override
    public void onCompactionCompleted(RocksDB db, CompactionJobInfo compactionJobInfo) {
        if (lastCompactionEventType.compareAndSet(ON_COMPACTION_BEGIN, ON_COMPACTION_COMPLETED)) {
            if (LOG.isInfoEnabled()) {
                long duration = System.nanoTime() - lastCompactionStartTimeNanos;

                LOG.info(
                        "Finishing rocksdb compaction process [name='{}', nodeName='{}', duration={}ms]",
                        name, nodeName, TimeUnit.NANOSECONDS.toMillis(duration)
                );
            }
        }
    }
}
