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

package org.apache.ignite.internal.table.distributed.gc;

import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent.LOW_WATERMARK_CHANGED;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.TrackerClosedException;
import org.apache.ignite.lang.ErrorGroups.GarbageCollector;
import org.jetbrains.annotations.TestOnly;

/**
 * Garbage collector for multi-versioned storages and their indexes in the background.
 *
 * @see GcUpdateHandler#vacuumBatch(HybridTimestamp, int, boolean)
 */
public class MvGc implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(MvGc.class);

    /** Node name. */
    private final String nodeName;

    /** Garbage collector configuration. */
    private final GcConfiguration gcConfig;

    /** Garbage collection thread pool. */
    private volatile ExecutorService executor;

    /** Prevents double closing. */
    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /** Busy lock to close synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Low watermark. */
    private final LowWatermark lowWatermark;

    /** Storage handler by table partition ID for which garbage will be collected. */
    private final ConcurrentMap<TablePartitionId, GcStorageHandler> storageHandlerByPartitionId = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param gcConfig Garbage collector configuration.
     * @param lowWatermark Low watermark.
     */
    public MvGc(String nodeName, GcConfiguration gcConfig, LowWatermark lowWatermark) {
        this.nodeName = nodeName;
        this.gcConfig = gcConfig;
        this.lowWatermark = lowWatermark;
    }

    /** Starts the garbage collector. */
    public void start() {
        inBusyLock(() -> {
            int threadCount = gcConfig.threads().value();

            executor = new ThreadPoolExecutor(
                    threadCount,
                    threadCount,
                    30,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(),
                    IgniteThreadFactory.create(nodeName, "mv-gc", LOG, STORAGE_READ, STORAGE_WRITE)
            );

            lowWatermark.listen(LOW_WATERMARK_CHANGED, fromConsumer(this::onLwmChanged));
        });
    }

    /**
     * Adds storage for background garbage collection when updating a low watermark.
     *
     * @param tablePartitionId Table partition ID.
     * @param gcUpdateHandler Gc update handler.
     * @throws IgniteInternalException with {@link GarbageCollector#CLOSED_ERR} If the garbage collector is closed.
     */
    public void addStorage(TablePartitionId tablePartitionId, GcUpdateHandler gcUpdateHandler) {
        inBusyLock(() -> {
            GcStorageHandler previous = storageHandlerByPartitionId.putIfAbsent(
                    tablePartitionId,
                    new GcStorageHandler(gcUpdateHandler)
            );

            // TODO: IGNITE-18939 Should be called once, you need to check that previous == null
            if (previous == null && lowWatermark.getLowWatermark() != null) {
                scheduleGcForStorage(tablePartitionId);
            }
        });
    }

    /**
     * Removes storage for background garbage collection and completes the garbage collection for it.
     *
     * <p>Should be called before rebalancing/closing/destroying the storage.
     *
     * @param tablePartitionId Table partition ID.
     * @return Storage garbage collection completion future.
     * @throws IgniteInternalException with {@link GarbageCollector#CLOSED_ERR} If the garbage collector is closed.
     */
    public CompletableFuture<Void> removeStorage(TablePartitionId tablePartitionId) {
        return inBusyLock(() -> {
            GcStorageHandler removed = storageHandlerByPartitionId.remove(tablePartitionId);

            if (removed == null) {
                return nullCompletedFuture();
            }

            CompletableFuture<Void> gcInProgressFuture = removed.gcInProgressFuture.get();

            return gcInProgressFuture == null ? nullCompletedFuture() : gcInProgressFuture;
        });
    }

    private void onLwmChanged(ChangeLowWatermarkEventParameters parameters) {
        inBusyLock(() -> executor.submit(() -> inBusyLock(this::initNewGcBusy)));
    }

    @Override
    public void close() throws Exception {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        if (executor != null) {
            shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
        }
    }

    private void initNewGcBusy() {
        storageHandlerByPartitionId.keySet().forEach(this::scheduleGcForStorage);
    }

    private void scheduleGcForStorage(TablePartitionId tablePartitionId) {
        inBusyLock(() -> {
            CompletableFuture<Void> currentGcFuture = new CompletableFuture<>();

            GcStorageHandler storageHandler = storageHandlerByPartitionId.compute(tablePartitionId, (tablePartId, gcStorageHandler) -> {
                if (gcStorageHandler == null) {
                    // Storage has been removed from garbage collection.
                    return null;
                }

                CompletableFuture<Void> inProgressFuture = gcStorageHandler.gcInProgressFuture.get();

                if (inProgressFuture == null || inProgressFuture.isDone()) {
                    boolean casResult = gcStorageHandler.gcInProgressFuture.compareAndSet(inProgressFuture, currentGcFuture);

                    assert casResult : tablePartId;
                } else {
                    inProgressFuture.whenComplete((unused, throwable) -> scheduleGcForStorage(tablePartitionId));
                }

                return gcStorageHandler;
            });

            if (storageHandler == null) {
                // Storage has been removed from garbage collection.
                return;
            }

            if (storageHandler.gcInProgressFuture.get() != currentGcFuture) {
                // Someone in parallel is already collecting garbage, we will try once again after completion of gcInProgressFuture.
                return;
            }

            try {
                HybridTimestamp lowWatermark = this.lowWatermark.getLowWatermark();

                assert lowWatermark != null : tablePartitionId;

                // If the storage has been deleted, then garbage collection is no longer necessary.
                if (!storageHandlerByPartitionId.containsKey(tablePartitionId)) {
                    currentGcFuture.complete(null);

                    return;
                }

                GcUpdateHandler gcUpdateHandler = storageHandler.gcUpdateHandler;

                // We can only start garbage collection when the partition safe time is reached.
                gcUpdateHandler.getSafeTimeTracker()
                        .waitFor(lowWatermark)
                        .thenApplyAsync(unused -> gcUpdateHandler.vacuumBatch(lowWatermark, gcConfig.value().batchSize(), true), executor)
                        .whenComplete((isGarbageLeft, throwable) -> {
                            if (throwable != null) {
                                if (unwrapCause(throwable) instanceof TrackerClosedException) {
                                    LOG.debug("TrackerClosedException caught", throwable);

                                    currentGcFuture.complete(null);
                                } else {
                                    LOG.error("Error when running GC", throwable);

                                    currentGcFuture.completeExceptionally(throwable);
                                }

                                return;
                            }

                            currentGcFuture.complete(null);

                            // If there is garbage left and the storage has not been deleted, then we will schedule the next garbage
                            // collection.
                            if (isGarbageLeft && storageHandlerByPartitionId.containsKey(tablePartitionId)) {
                                scheduleGcForStorage(tablePartitionId);
                            }
                        });
            } catch (Throwable t) {
                LOG.error("Error when running GC", t);

                currentGcFuture.completeExceptionally(t);
            }
        });
    }

    private <T> T inBusyLock(Supplier<T> supplier) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(GarbageCollector.CLOSED_ERR);
        }

        try {
            return supplier.get();
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void inBusyLock(Runnable runnable) {
        inBusyLock(() -> {
            runnable.run();

            return null;
        });
    }

    /**
     * Schedule a new garbage collection for all storages.
     */
    @TestOnly
    void scheduleGcForAllStorages() {
        inBusyLock(this::initNewGcBusy);
    }
}
