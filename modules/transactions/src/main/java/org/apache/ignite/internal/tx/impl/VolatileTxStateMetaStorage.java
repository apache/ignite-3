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

package org.apache.ignite.internal.tx.impl;

import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.apache.ignite.internal.tx.TxState.checkTransitionCorrectness;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.impl.PersistentTxStateVacuumizer.PersistentTxStateVacuumResult;
import org.apache.ignite.internal.tx.impl.PersistentTxStateVacuumizer.VacuumizableTx;
import org.apache.ignite.internal.tx.metrics.ResourceVacuumMetrics;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * The class represents volatile transaction state storage that stores a transaction state meta until the node stops.
 */
public class VolatileTxStateMetaStorage {
    /** The local map for tx states. */
    private ConcurrentHashMap<UUID, TxStateMeta> txStateMap;

    /**
     * Creates and starts the storage.
     *
     * <p>Intended for tests/benchmarks where the storage is used directly without a full component lifecycle.
     */
    @TestOnly
    public static VolatileTxStateMetaStorage createStarted() {
        VolatileTxStateMetaStorage storage = new VolatileTxStateMetaStorage();
        storage.start();
        return storage;
    }

    /**
     * Starts the storage.
     */
    public void start() {
        txStateMap = new ConcurrentHashMap<>();
    }

    /**
     * Stops the detector.
     */
    public void stop() {
        txStateMap.clear();
    }

    /**
     * Initializes the meta state for a created transaction.
     *
     * @param tx Transaction object.
     * @param txLabel Transaction label.
     */
    public void initialize(InternalTransaction tx, @Nullable String txLabel) {
        TxStateMeta previous = txStateMap.put(
                tx.id(),
                TxStateMeta.builder(PENDING)
                        .txCoordinatorId(tx.coordinatorId())
                        .tx(tx)
                        .txLabel(txLabel)
                        .build()
        );

        assert previous == null : "Transaction state has already defined [txId=" + tx.id() + ", state=" + previous.txState() + ']';
    }

    /**
     * Atomically changes the state meta of a transaction.
     *
     * @param txId Transaction id.
     * @param updater Transaction meta updater.
     * @return Updated transaction state.
     */
    public @Nullable <T extends TxStateMeta> T updateMeta(UUID txId, Function<@Nullable TxStateMeta, TxStateMeta> updater) {
        return (T) txStateMap.compute(txId, (k, oldMeta) -> {
            TxStateMeta newMeta = updater.apply(oldMeta);

            if (newMeta == null) {
                return null;
            }

            TxState oldState = oldMeta == null ? null : oldMeta.txState();

            return checkTransitionCorrectness(oldState, newMeta.txState()) ? newMeta : oldMeta;
        });
    }

    /**
     * Returns a transaction state meta.
     *
     * @param txId Transaction id.
     * @return The state meta or null if the state is unknown.
     */
    public TxStateMeta state(UUID txId) {
        return txStateMap.get(txId);
    }

    /**
     * Gets all defined transactions meta states.
     *
     * @return Collection of transaction meta states.
     */
    public Collection<TxStateMeta> states() {
        return txStateMap.values();
    }

    /**
     * Locally vacuums no longer needed transactional resource. Also calls {@code persistentVacuumOp} to vacuum some persistent states.
     * For each finished (COMMITTED or ABORTED) transactions:
     * <ol>
     *     <li> Takes every suitable txn state from the volatile storage if txnResourcesTTL == 0 or if
     *     max(txnState.initialVacuumObservationTimestamp, txnState.cleanupCompletionTimestamp) + txnResourcesTTL <
     *     vacuumObservationTimestamp. If txnState.cleanupCompletionTimestamp is {@code null}, then only
     *     txnState.initialVacuumObservationTimestamp is used.</li>
     *     <li>If txnState.commitPartitionId is {@code null}, then only volatile state is vacuumized, if not {@code null} this
     *     means we are probably on commit partition and this txnState should be passed to {@code persistentVacuumOp} to vacuumize the
     *     persistent state as well. Only after such txn states are processed by {@code persistentVacuumOp} we can remove the volatile
     *     txn state. Only states which are marked by {@code persistentVacuumOp} as sucessfully vacuumized and
     *     {@code cleanupCompletionTimestamp} is the same that was passed to {@code persistentVacuumOp} can be removed, to prevent
     *     races.</li>
     *     <li>Updates txnState.initialVacuumObservationTimestamp by setting it to vacuumObservationTimestamp
     *     if it's not already initialized.</li>
     * </ol>
     *
     * @param vacuumObservationTimestamp Timestamp of the vacuum attempt.
     * @param txnResourceTtl Transactional resource time to live in milliseconds.
     * @param persistentVacuumOp Persistent vacuum operation. Accepts the map of commit partition ids to set of
     *     tx ids, returns a future with set of successfully vacuumized tx ids.
     * @return Vacuum complete future.
     */
    public CompletableFuture<Void> vacuum(
            long vacuumObservationTimestamp,
            long txnResourceTtl,
            Function<Map<ZonePartitionId, Set<VacuumizableTx>>, CompletableFuture<PersistentTxStateVacuumResult>> persistentVacuumOp,
            ResourceVacuumMetrics resourceVacuumMetrics
    ) {
        Map<ZonePartitionId, Set<VacuumizableTx>> txIds = new HashMap<>();
        Map<UUID, Long> cleanupCompletionTimestamps = new HashMap<>();

        var skippedForFurtherProcessingUnfinishedTxnsCount = new AtomicInteger();

        txStateMap.forEach((txId, meta) -> {
            txStateMap.computeIfPresent(txId, (txId0, meta0) -> {
                if (meta0.tx() != null && meta0.tx().isReadOnly()) {
                    if (meta0.tx().isFinishingOrFinished()) {
                        resourceVacuumMetrics.onVolatileStateVacuum();

                        return null;
                    }
                } else if (TxState.isFinalState(meta0.txState())) {
                    Long initialVacuumObservationTimestamp = meta0.initialVacuumObservationTimestamp();

                    Long cleanupCompletionTimestamp = meta0.cleanupCompletionTimestamp();

                    boolean shouldBeVacuumized = shouldBeVacuumized(initialVacuumObservationTimestamp,
                            cleanupCompletionTimestamp, txnResourceTtl, vacuumObservationTimestamp);

                    if (shouldBeVacuumized) {
                        if (meta0.commitPartitionId() == null) {
                            resourceVacuumMetrics.onVolatileStateVacuum();

                            return null;
                        } else {
                            Set<VacuumizableTx> ids = txIds.computeIfAbsent(meta0.commitPartitionId(), k -> new HashSet<>());
                            ids.add(new VacuumizableTx(txId, cleanupCompletionTimestamp));

                            if (cleanupCompletionTimestamp != null) {
                                cleanupCompletionTimestamps.put(txId, cleanupCompletionTimestamp);
                            }

                            return meta0;
                        }
                    } else {
                        resourceVacuumMetrics.onMarkedForVacuum();

                        return meta0;
                    }

                }

                skippedForFurtherProcessingUnfinishedTxnsCount.incrementAndGet();
                return meta0;
            });
        });

        return persistentVacuumOp.apply(txIds)
                .thenAccept(vacuumResult -> {
                    for (UUID txId : vacuumResult.txnsToVacuum) {
                        txStateMap.compute(txId, (k, v) -> {
                            if (v == null) {
                                return null;
                            } else {
                                Long cleanupCompletionTs = cleanupCompletionTimestamps.get(txId);

                                TxStateMeta newMeta = (Objects.equals(cleanupCompletionTs, v.cleanupCompletionTimestamp())) ? null : v;

                                if (newMeta == null) {
                                    resourceVacuumMetrics.onVolatileStateVacuum();
                                }

                                return newMeta;
                            }
                        });
                    }

                    resourceVacuumMetrics.onVacuumFinish(
                            vacuumResult.vacuumizedPersistentTxnStatesCount,
                            skippedForFurtherProcessingUnfinishedTxnsCount.get()
                    );
                });
    }

    /** Returns a mapping of all stored transaction IDs to their state. */
    Map<UUID, TxStateMeta> statesMap() {
        return Collections.unmodifiableMap(txStateMap);
    }

    private static boolean shouldBeVacuumized(
            Long initialVacuumObservationTimestamp,
            @Nullable Long cleanupCompletionTimestamp,
            long txnResourceTtl,
            long vacuumObservationTimestamp) {
        if (txnResourceTtl == 0) {
            return true;
        }

        assert initialVacuumObservationTimestamp != null : "initialVacuumObservationTimestamp should have been set if txnResourceTtl > 0 "
                + "[txnResourceTtl=" + txnResourceTtl + "].";

        if (cleanupCompletionTimestamp == null) {
            return initialVacuumObservationTimestamp + txnResourceTtl < vacuumObservationTimestamp;
        } else {
            return cleanupCompletionTimestamp + txnResourceTtl < vacuumObservationTimestamp;
        }
    }
}
