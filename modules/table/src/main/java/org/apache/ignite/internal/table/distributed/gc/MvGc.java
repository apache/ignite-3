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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.ErrorGroups.Gc;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Garbage collector for multi-versioned storages and their indexes in the background.
 *
 * @see MvPartitionStorage#pollForVacuum(HybridTimestamp)
 */
public class MvGc implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(MvGc.class);

    /** GC batch size for the storage. */
    static final int GC_BUTCH_SIZE = 5;

    /** Garbage collection thread pool. */
    private final ExecutorService executor;

    /** Prevents double closing. */
    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /** Busy lock to close synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Low watermark. */
    private final AtomicReference<HybridTimestamp> lowWatermarkReference = new AtomicReference<>();

    /** Storage handler by table partition ID for which garbage will be collected. */
    private final ConcurrentMap<TablePartitionId, GcStorageHandler> storageHandlerByPartitionId = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param threadCount Number of garbage collector threads.
     */
    public MvGc(String nodeName, int threadCount) {
        assert threadCount > 0 : threadCount;

        executor = new ThreadPoolExecutor(
                threadCount,
                threadCount,
                30,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory(nodeName, LOG)
        );
    }

    /**
     * Adds storage for background garbage collection when updating a low watermark.
     *
     * @param tablePartitionId Table partition ID.
     * @param storageUpdateHandler Storage update handler.
     * @throws IgniteInternalException with {@link Gc#CLOSED_ERR} If the garbage collector is closed.
     */
    public void addStorage(TablePartitionId tablePartitionId, StorageUpdateHandler storageUpdateHandler) {
        inBusyLock(() -> {
            GcStorageHandler previous = storageHandlerByPartitionId.putIfAbsent(
                    tablePartitionId,
                    new GcStorageHandler(storageUpdateHandler)
            );

            assert previous == null : "Storage already added for GC: " + tablePartitionId;

            if (lowWatermarkReference.get() != null) {
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
     * @throws IgniteInternalException with {@link Gc#CLOSED_ERR} If the garbage collector is closed.
     */
    public CompletableFuture<Void> removeStorage(TablePartitionId tablePartitionId) {
        return inBusyLock(() -> {
            GcStorageHandler removed = storageHandlerByPartitionId.remove(tablePartitionId);

            if (removed == null) {
                return completedFuture(null);
            }

            CompletableFuture<Void> gcInProgressFuture = removed.gcInProgressFuture.get();

            return gcInProgressFuture == null ? completedFuture(null) : gcInProgressFuture;
        });
    }

    /**
     * Updates the new watermark only if it is larger than the current low watermark.
     *
     * <p>If the update is successful, it will schedule a new garbage collection for all storages.
     *
     * @param newLwm New low watermark.
     * @throws IgniteInternalException with {@link Gc#CLOSED_ERR} If the garbage collector is closed.
     */
    public void updateLowWatermark(HybridTimestamp newLwm) {
        inBusyLock(() -> {
            HybridTimestamp updatedLwm = lowWatermarkReference.updateAndGet(currentLwm -> {
                if (currentLwm == null) {
                    return newLwm;
                }

                // Update only if the new one is greater than the current one.
                return newLwm.compareTo(currentLwm) > 0 ? newLwm : currentLwm;
            });

            // If the new watermark is smaller than the current one or has been updated in parallel, then we do nothing.
            if (updatedLwm != newLwm) {
                return;
            }

            executor.submit(() -> inBusyLock(this::initNewGcBusy));
        });
    }

    @Override
    public void close() throws Exception {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
    }

    private void initNewGcBusy() {
        storageHandlerByPartitionId.keySet().forEach(this::scheduleGcForStorage);
    }

    private void scheduleGcForStorage(TablePartitionId tablePartitionId) {
        executor.submit(() -> inBusyLock(() -> {
            GcStorageHandler storageHandler = storageHandlerByPartitionId.compute(tablePartitionId, (id, gcStorageHandler) -> {
                if (gcStorageHandler == null) {
                    // Storage has been removed from garbage collection.
                    return gcStorageHandler;
                }

                boolean casResult = gcStorageHandler.gcInProgressFuture.compareAndSet(null, new CompletableFuture<>());

                assert casResult : tablePartitionId;

                return gcStorageHandler;
            });

            if (storageHandler == null) {
                // Storage has been removed from garbage collection.
                return;
            }

            CompletableFuture<Void> future = storageHandler.gcInProgressFuture.get();

            assert future != null : tablePartitionId;

            try {
                boolean scheduleGcForStorageAgain = true;

                for (int i = 0; i < GC_BUTCH_SIZE && scheduleGcForStorageAgain; i++) {
                    HybridTimestamp lowWatermark = lowWatermarkReference.get();

                    assert lowWatermark != null : tablePartitionId;

                    // If storage has been deleted or there is no garbage, then for now we will stop collecting garbage for this storage.
                    if (!storageHandlerByPartitionId.containsKey(tablePartitionId)
                            || !storageHandler.storageUpdateHandler.vacuum(lowWatermark)) {
                        scheduleGcForStorageAgain = false;
                    }
                }

                if (scheduleGcForStorageAgain) {
                    scheduleGcForStorage(tablePartitionId);
                }

                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            } finally {
                boolean casResult = storageHandler.gcInProgressFuture.compareAndSet(future, null);

                assert casResult : tablePartitionId;
            }
        }));
    }

    private <T> T inBusyLock(Supplier<T> supplier) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(Gc.CLOSED_ERR);
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
}
