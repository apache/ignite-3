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

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Helper class to deal with RocksDB flushes. Provides an ability to wait until current state of data is flushed to the storage.
 * Requires enabled {@link Options#setAtomicFlush(boolean)} option to work properly.
 */
public class RocksDbFlusher {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RocksDbFlusher.class);

    /** Rocks DB instance. */
    private volatile RocksDB db;

    /** List of all column families. */
    private final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    /** Mutex for {@link #columnFamilyHandles} access. */
    private final Object columnFamilyHandlesMux = new Object();

    /** Scheduled pool to schedule flushes. */
    private final ScheduledExecutorService scheduledPool;

    /** Thread pool to complete flush completion futures. */
    final ExecutorService threadPool;

    /** Supplier of delay values to batch independent flush requests. */
    private final IntSupplier delaySupplier;

    /** Flush completion callback. */
    private final Runnable onFlushCompleted;

    /**
     * Flush options to be used to asynchronously flush the Rocks DB memtable. It needs to be cached, because
     * {@link RocksDB#flush(FlushOptions)} contract requires this object to not be GC-ed.
     */
    private final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(false);

    /** Map with flush futures by sequence number at the time of the {@link #awaitFlush(boolean)} call. */
    private final SortedMap<Long, CompletableFuture<Void>> flushFuturesBySequenceNumber = new ConcurrentSkipListMap<>();

    /** Latest known sequence number for persisted data. Not volatile, protected by explicit synchronization. */
    private long latestPersistedSequenceNumber;

    /** Mutex for {@link #latestPersistedSequenceNumber} modifications. */
    private final Object latestPersistedSequenceNumberMux = new Object();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock;

    private final RocksDbFlushListener flushListener;

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
     * @param threadPool Thread pool to run flush completion closure, provided by {@code onFlushCompleted} parameter.
     * @param delaySupplier Supplier of delay values to batch independent flush requests. When {@link #awaitFlush(boolean)} is called with
     *      {@code true} parameter, the flusher waits given number of milliseconds (using {@code scheduledPool}) and then executes flush
     *      only if there were no other {@code awaitFlush(true)} calls. Otherwise, it does nothing after the timeout. This guarantees that
     *      either the last one wins, or automatic flush wins if there's an endless stream of {@code awaitFlush(true)} calls with very small
     *      time-intervals between them. Such behavior allows to save on unnecessary flushes when multiple await flush calls appear at
     *      roughly the same time from different threads. For example, several partitions might be flushed at the same time, because they
     *      started at the same time and their flush frequency is also the same.
     * @param onFlushCompleted Flush completion callback. Executed on every individual column family flush.
     * @param name
     */
    public RocksDbFlusher(
            IgniteSpinBusyLock busyLock,
            ScheduledExecutorService scheduledPool,
            ExecutorService threadPool,
            IntSupplier delaySupplier,
            LogSyncer logSyncer,
            Runnable onFlushCompleted,
            String name
    ) {
        this.busyLock = busyLock;
        this.scheduledPool = scheduledPool;
        this.threadPool = threadPool;
        this.delaySupplier = delaySupplier;
        this.onFlushCompleted = onFlushCompleted;
        this.flushListener = new RocksDbFlushListener(this, logSyncer, name);
    }

    /**
     * Returns an instance of {@link AbstractEventListener} to process actual RocksDB events. Returned listener must be set into
     * {@link Options#setListeners(List)} before database is started. Otherwise, no events would occur.
     */
    public AbstractEventListener listener() {
        return flushListener;
    }

    /**
     * Initializes the flusher with DB instance and a list of column families.
     *
     * @param db Rocks DB instance.
     * @param columnFamilyHandles List of all column families. Column families missing from this list may not have flush events processed.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public void init(RocksDB db, List<ColumnFamilyHandle> columnFamilyHandles) {
        this.db = db;

        synchronized (columnFamilyHandlesMux) {
            this.columnFamilyHandles.addAll(columnFamilyHandles);
        }

        synchronized (latestPersistedSequenceNumberMux) {
            latestPersistedSequenceNumber = db.getLatestSequenceNumber();
        }
    }

    /**
     * Adds the given handle to the list of CF handles.
     */
    public void addColumnFamily(ColumnFamilyHandle handle) {
        synchronized (columnFamilyHandlesMux) {
            columnFamilyHandles.add(handle);
        }
    }

    /**
     * Removes the given handle to the list of CF handles.
     */
    public void removeColumnFamily(ColumnFamilyHandle handle) {
        synchronized (columnFamilyHandlesMux) {
            columnFamilyHandles.remove(handle);
        }
    }

    /**
     * Returns a future to wait next flush operation from the current point in time. Uses {@link RocksDB#getLatestSequenceNumber()} to
     * achieve this, by fixing its value at the time of invocation. Storage is considered flushed when at least one persisted column
     * family has its latest sequence number greater or equal to the one that we fixed. This is enough to guarantee that all column families
     * have up-to-data state as well, because flusher expects its users to also have {@link Options#setAtomicFlush(boolean)} option
     * enabled.
     *
     * @param schedule {@code true} if {@link RocksDB#flush(FlushOptions)} should be explicitly triggerred in the near future. Please refer
     *      to {@link RocksDbFlusher#RocksDbFlusher(IgniteSpinBusyLock, ScheduledExecutorService, ExecutorService, IntSupplier, LogSyncer,
     *      Runnable)} parameters description to see what's really happening in this case.
     *
     * @see #scheduleFlush()
     */
    public CompletableFuture<Void> awaitFlush(boolean schedule) {
        CompletableFuture<Void> future;

        long dbSequenceNumber = db.getLatestSequenceNumber();

        synchronized (latestPersistedSequenceNumberMux) {
            if (dbSequenceNumber <= latestPersistedSequenceNumber) {
                return nullCompletedFuture();
            }

            future = flushFuturesBySequenceNumber.computeIfAbsent(dbSequenceNumber, s -> new CompletableFuture<>());
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
                    synchronized (columnFamilyHandlesMux) {
                        db.flush(flushOptions, columnFamilyHandles);
                    }
                } catch (RocksDBException e) {
                    LOG.error("Error occurred during the explicit flush", e);
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

        SortedMap<Long, CompletableFuture<Void>> futuresToComplete = flushFuturesBySequenceNumber.headMap(sequenceNumber + 1);

        for (CompletableFuture<Void> future : futuresToComplete.values()) {
            future.complete(null);
        }

        futuresToComplete.clear();
    }

    /**
     * Stops the flusher by cancelling all of its futures.
     */
    public void stop() {
        for (CompletableFuture<Void> future : flushFuturesBySequenceNumber.values()) {
            future.cancel(false);
        }

        RocksUtils.closeAll(flushListener, flushOptions);
    }

    /**
     * Executes the {@code onFlushCompleted} callback.
     *
     * @return Future that completes when the {@code onFlushCompleted} callback finishes.
     */
    CompletableFuture<Void> onFlushCompleted() {
        return CompletableFuture.runAsync(onFlushCompleted, threadPool);
    }
}
