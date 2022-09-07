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

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Helper class do deal with RocksDB flushes.
 */
public class RocksDbFlusher {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RocksDbFlusher.class);

    /** Rocks DB instance. */
    private volatile RocksDB db;

    /** List of all column families. */
    private volatile List<ColumnFamilyHandle> columnFamilyHandles;

    /** Scheduled pool the schedule flushes. */
    private final ScheduledExecutorService scheduledPool;

    /** Thread pool to complete flush completion futures. */
    final ExecutorService threadPool;

    /** Supplier of delay values to batch independant flush requests. */
    private final IntSupplier delaySupplier;

    /** Flush completion callback. */
    final Runnable onFlushCompleted;

    /** Instance of {@link AbstractEventListener} to process actual RocksDB events. */
    private final RocksDbFlushListener flushListener;

    /**
     * Flush options to be used to asynchronously flush the Rocks DB memtable. It needs to be cached, because
     * {@link RocksDB#flush(FlushOptions)} contract requires this object to not be GC-ed.
     */
    private final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(false);

    /** Map with flush futures by sequence number at the time of the {@link #awaitFlush(boolean)} call. */
    final ConcurrentMap<Long, CompletableFuture<Void>> flushFuturesBySequenceNumber = new ConcurrentHashMap<>();

    /** Latest known sequence number for persisted data. Not volatile, protected by explicit synchronization. */
    private long latestPersistedSequenceNumber;

    /** Mutex for {@link #latestPersistedSequenceNumber} modifications. */
    private final Object latestPersistedSequenceNumberMux = new Object();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock;

    /**
     * Instance of the latest scheduled flush closure.
     *
     * @see #scheduleFlush()
     */
    private volatile Runnable latestFlushClosure;

    /**
     * Constructor.
     *
     * @param busyLock Busy lock.
     * @param scheduledPool Scheduled pool the schedule flushes.
     * @param threadPool Thread pool to complete flush completion futures.
     * @param delaySupplier Supplier of delay values to batch independant flush requests.
     * @param onFlushCompleted Flush completion callback.
     */
    public RocksDbFlusher(
            IgniteSpinBusyLock busyLock,
            ScheduledExecutorService scheduledPool,
            ExecutorService threadPool,
            IntSupplier delaySupplier,
            Runnable onFlushCompleted
    ) {
        this.busyLock = busyLock;
        this.scheduledPool = scheduledPool;
        this.threadPool = threadPool;
        this.delaySupplier = delaySupplier;
        this.onFlushCompleted = onFlushCompleted;

        flushListener = new RocksDbFlushListener(this);
    }

    /**
     * Returns an instance of {@link AbstractEventListener} to process actual RocksDB events.
     *
     * @see DBOptions#setListeners(List)
     */
    public AbstractEventListener listener() {
        return flushListener;
    }

    /**
     * Initializes the flusher with DB instance and a list of column families.
     *
     * @param db Rocks DB instance.
     * @param columnFamilyHandles List of all column families.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public void init(RocksDB db, List<ColumnFamilyHandle> columnFamilyHandles) {
        this.db = db;
        this.columnFamilyHandles = columnFamilyHandles;

        synchronized (latestPersistedSequenceNumberMux) {
            latestPersistedSequenceNumber = db.getLatestSequenceNumber();
        }
    }

    /**
     * Returns a future to wait next flush operation from the current point in time. Uses {@link RocksDB#getLatestSequenceNumber()} to
     * achieve this.
     *
     * @param schedule {@code true} if {@link RocksDB#flush(FlushOptions)} should be explicitly triggerred in the near future.
     *
     * @see #scheduleFlush()
     */
    public CompletableFuture<Void> awaitFlush(boolean schedule) {
        CompletableFuture<Void> future;

        long dbSequenceNumber = db.getLatestSequenceNumber();

        synchronized (latestPersistedSequenceNumberMux) {
            if (dbSequenceNumber <= latestPersistedSequenceNumber) {
                return CompletableFuture.completedFuture(null);
            }

            future = flushFuturesBySequenceNumber.computeIfAbsent(dbSequenceNumber, l -> new CompletableFuture<>());
        }

        if (schedule) {
            scheduleFlush();
        }

        return future;
    }

    /**
     * Schedules a flush of the table. If run several times within a small amount of time, only the last scheduled flush will be executed.
     */
    private void scheduleFlush() {
        Runnable newClosure = new Runnable() {
            @Override
            public void run() {
                if (latestFlushClosure != this) {
                    return;
                }

                if (!busyLock.enterBusy()) {
                    return;
                }

                try {
                    // Explicit list of CF handles is mandatory!
                    // Default flush is buggy and only invokes listener methods for a single random CF.
                    db.flush(flushOptions, columnFamilyHandles);
                } catch (RocksDBException e) {
                    LOG.error("Error occurred during the explicit flush");
                } finally {
                    busyLock.leaveBusy();
                }
            }
        };

        latestFlushClosure = newClosure;

        scheduledPool.schedule(newClosure, delaySupplier.getAsInt(), TimeUnit.MILLISECONDS);
    }

    /**
     * Completes all futures in {@link #flushFuturesBySequenceNumber} up to a given sequence number.
     */
    void completeFutures(long sequenceNumber) {
        synchronized (latestPersistedSequenceNumberMux) {
            if (sequenceNumber <= latestPersistedSequenceNumber) {
                return;
            }

            latestPersistedSequenceNumber = sequenceNumber;
        }

        Set<Entry<Long, CompletableFuture<Void>>> entries = flushFuturesBySequenceNumber.entrySet();

        for (Iterator<Entry<Long, CompletableFuture<Void>>> iterator = entries.iterator(); iterator.hasNext(); ) {
            Entry<Long, CompletableFuture<Void>> entry = iterator.next();

            if (sequenceNumber >= entry.getKey()) {
                entry.getValue().complete(null);

                iterator.remove();
            }
        }
    }

    /**
     * Stops the flusher by cancelling all of its futures.
     */
    public void stop() {
        for (CompletableFuture<Void> future : flushFuturesBySequenceNumber.values()) {
            future.cancel(false);
        }

        flushOptions.close();
    }
}
