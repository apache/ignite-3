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

package org.apache.ignite.internal.table.distributed.replicator;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.anyOf;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.Collection;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/**
 * Class to track the completion of RW transactions' operations before the backfill process begins.
 *
 * <p>Allows us to cover some corner cases when a new index transitions to backfill states just before it starts.</p>
 *
 * <p>Usage notes:</p>
 * <ul>
 *     <li>To wait for all RW transaction operations to complete before creating a new index, use
 *     {@link #awaitCompleteTxRwOperations(int)}.</li>
 *     <li>When the index transition to the backfill state, {@link #updateMinAllowedCatalogVersionForStartOperation(int)} must be used.</li>
 *     <li>At the beginning of each new RW transaction operation, {@link #incrementOperationCount(int)} must be used.</li>
 *     <li>When completing a RW transaction operation, {@link #decrementOperationCount(int)} must be used.</li>
 * </ul>
 */
class IndexBuilderTxRwOperationTracker implements ManuallyCloseable {
    private final AtomicInteger minAllowedCatalogVersionForStartOperation = new AtomicInteger(-1);

    private final NavigableMap<Integer, CompletableFuture<Void>> minAllowedVersionRaiseFutures = new ConcurrentSkipListMap<>();

    private final NavigableMap<Integer, TxRwOperationCounter> operationCounterByCatalogVersion = new ConcurrentSkipListMap<>();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    @Override
    public void close() {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        closeFuture.completeExceptionally(new NodeStoppingException());
    }

    /**
     * Updating the minimum available catalog version on which new RW transaction operations can be started.
     *
     * <p>NOTE: Value will only increase.</p>
     *
     * @param catalogVersion Catalog version in which the new index appeared. New operations for RW transactions started on versions
     *      strictly before this one will not be allowed to start.
     */
    void updateMinAllowedCatalogVersionForStartOperation(int catalogVersion) {
        inBusyLock(busyLock, () -> {
            minAllowedCatalogVersionForStartOperation.updateAndGet(
                    previousCatalogVersion -> Math.max(catalogVersion, previousCatalogVersion)
            );

            Collection<CompletableFuture<Void>> futures = minAllowedVersionRaiseFutures.headMap(catalogVersion, true)
                    .values();

            futures.forEach(future -> future.complete(null));

            futures.clear();
        });
    }

    /**
     * Waits for RW transactions operations to complete strictly lower than the requested version.
     *
     * <p>It first waits for the requested catalog version to be reached in {@link #updateMinAllowedCatalogVersionForStartOperation(int)}
     * and then waits for all RW transaction operations to complete.</p>
     *
     * @param catalogVersion Catalog version in which the new index appeared.
     */
    CompletableFuture<Void> awaitCompleteTxRwOperations(int catalogVersion) {
        return inBusyLock(busyLock, () -> {
            // This code is needed to avoid races with updateAllowedCatalogVersionForStartOperation.
            CompletableFuture<Void> waitForMinAllowedReachingVersion = minAllowedVersionRaiseFutures.computeIfAbsent(
                    catalogVersion,
                    i -> new CompletableFuture<>()
            );

            if (catalogVersion <= minAllowedCatalogVersionForStartOperation.get()) {
                minAllowedVersionRaiseFutures.remove(catalogVersion);

                waitForMinAllowedReachingVersion.complete(null);
            }

            // To avoid freezing after closing the tracker.
            waitForMinAllowedReachingVersion = orCloseFuture(waitForMinAllowedReachingVersion);

            return waitForMinAllowedReachingVersion.thenCompose(unused -> {
                // Let's select counters for versions up to (not including) the requested catalog version.
                CompletableFuture<?>[] futures = operationCounterByCatalogVersion.headMap(catalogVersion, false).values().stream()
                        .map(TxRwOperationCounter::operationsFuture)
                        .toArray(CompletableFuture[]::new);

                // To avoid freezing after closing the tracker.
                return orCloseFuture(allOf(futures));
            });
        });
    }

    /**
     * Increases the count of transaction operations by {@code 1}, at the start of a new RW transaction operation.
     *
     * <p>It is expected that after the operation is completed, {@link #decrementOperationCount(int)} will be called.</p>
     *
     * @param catalogVersion Catalog version at the beginning of the RW transaction of operation.
     * @return {@code true} if successful, otherwise {@code false} because the passed catalog version is less than the minimum available to
     *      start a RW transaction operation.
     */
    boolean incrementOperationCount(int catalogVersion) {
        return inBusyLock(busyLock, () -> {
            operationCounterByCatalogVersion.compute(catalogVersion, (i, txRwOperationCounter) -> {
                if (txRwOperationCounter == null) {
                    return TxRwOperationCounter.withCountOne();
                }

                return txRwOperationCounter.incrementOperationCount();
            });

            if (catalogVersion < minAllowedCatalogVersionForStartOperation.get()) {
                decrementOperationCount(catalogVersion);

                return false;
            }

            return true;
        });
    }

    /**
     * Decreases the count of transaction operations by {@code 1}, at the finish of a new RW transaction operation.
     *
     * <p>It is expected that {@link #incrementOperationCount(int)} was previously called.</p>
     *
     * @param catalogVersion Catalog version at the beginning of the RW transaction of operation.
     */
    void decrementOperationCount(int catalogVersion) {
        inBusyLock(busyLock, () -> {
            // I intentionally create a local holder so as not to complete the TxRwOperationCounter#operationsFuture inside Map#compute.
            TxRwOperationCounter[] localHolder = new TxRwOperationCounter[1];

            operationCounterByCatalogVersion.compute(catalogVersion, (i, txRwOperationCounter) -> {
                assert txRwOperationCounter != null : catalogVersion;

                // Reset due to a retry on an unsuccessful CAS on the map key.
                localHolder[0] = null;

                txRwOperationCounter = txRwOperationCounter.decrementOperationCount();

                if (txRwOperationCounter.isOperationsOver()) {
                    localHolder[0] = txRwOperationCounter;

                    return null;
                }

                return txRwOperationCounter;
            });

            if (localHolder[0] != null) {
                localHolder[0].operationsFuture().complete(null);
            }
        });
    }

    private <T> CompletableFuture<T> orCloseFuture(CompletableFuture<T> future) {
        if (future.isDone()) {
            return future;
        }

        return anyOf(future, closeFuture).thenApply(o -> (T) o);
    }
}
