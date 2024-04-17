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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.LifecycleAwareComponent;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.jetbrains.annotations.Nullable;

/**
 * The class represents volatile transaction state storage that stores a transaction state meta until the node stops.
 */
public class VolatileTxStateMetaStorage extends LifecycleAwareComponent {
    private static final IgniteLogger LOG = Loggers.forClass(VolatileTxStateMetaStorage.class);
    /** The local map for tx states. */
    private ConcurrentHashMap<UUID, TxStateMeta> txStateMap;

    /**
     * Starts the storage.
     */
    @Override
    public CompletableFuture<Void> startAsync() {
        return startAsync(() -> txStateMap = new ConcurrentHashMap<>());
    }

    /**
     * Stops the detector.
     */
    @Override
    public CompletableFuture<Void> stopAsync() {
        return stopAsync(() -> txStateMap.clear());
    }

    /**
     * Initializes the meta state for a created transaction.
     *
     * @param txId Transaction id.
     * @param txCrdId Transaction coordinator id.
     */
    public void initialize(UUID txId, String txCrdId) {
        TxStateMeta previous = txStateMap.put(txId, new TxStateMeta(PENDING, txCrdId, null, null));

        assert previous == null : "Transaction state has already defined [txId=" + txCrdId + ", state=" + previous.txState() + ']';
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
     * Locally vacuums no longer needed transactional resource.
     * For each finished (COMMITTED or ABORTED) transactions:
     * <ol>
     *     <li> Removes it from the volatile storage if txnResourcesTTL == 0 or if
     *     txnState.initialVacuumObservationTimestamp + txnResourcesTTL < vacuumObservationTimestamp.</li>
     *     <li>Updates txnState.initialVacuumObservationTimestamp by setting it to vacuumObservationTimestamp
     *     if it's not already initialized.</li>
     * </ol>
     *
     * @param vacuumObservationTimestamp Timestamp of the vacuum attempt.
     * @param txnResourceTtl Transactional resource time to live in milliseconds.
     */
    public void vacuum(long vacuumObservationTimestamp, long txnResourceTtl) {
        LOG.info("Vacuum started [vacuumObservationTimestamp={}, txnResourceTtl={}].", vacuumObservationTimestamp, txnResourceTtl);

        AtomicInteger vacuumizedTxnsCount = new AtomicInteger(0);
        AtomicInteger markedAsInitiallyDetectedTxnsCount = new AtomicInteger(0);
        AtomicInteger alreadyMarkedTxnsCount = new AtomicInteger(0);
        AtomicInteger skippedForFurtherProcessingUnfinishedTxnsCount = new AtomicInteger(0);

        txStateMap.forEach((txId, meta) -> {
            txStateMap.computeIfPresent(txId, (txId0, meta0) -> {
                if (TxState.isFinalState(meta0.txState())) {
                    if (txnResourceTtl == 0) {
                        vacuumizedTxnsCount.incrementAndGet();
                        return null;
                    } else if (meta0.initialVacuumObservationTimestamp() == null) {
                        markedAsInitiallyDetectedTxnsCount.incrementAndGet();
                        return new TxStateMeta(
                                meta0.txState(),
                                meta0.txCoordinatorId(),
                                meta0.commitPartitionId(),
                                meta0.commitTimestamp(),
                                vacuumObservationTimestamp
                        );
                    } else if (meta0.initialVacuumObservationTimestamp() + txnResourceTtl < vacuumObservationTimestamp) {
                        vacuumizedTxnsCount.incrementAndGet();
                        return null;
                    } else {
                        alreadyMarkedTxnsCount.incrementAndGet();
                        return meta0;
                    }
                } else {
                    skippedForFurtherProcessingUnfinishedTxnsCount.incrementAndGet();
                    return meta0;
                }
            });
        });

        LOG.info("Vacuum finished [vacuumObservationTimestamp={}, txnResourceTtl={}, vacuumizedTxnsCount={},"
                + " markedAsInitiallyDetectedTxnsCount={}, alreadyMarkedTxnsCount={}, skippedForFurtherProcessingUnfinishedTxnsCount={}].",
                vacuumObservationTimestamp,
                txnResourceTtl,
                vacuumizedTxnsCount,
                markedAsInitiallyDetectedTxnsCount,
                alreadyMarkedTxnsCount,
                skippedForFurtherProcessingUnfinishedTxnsCount
        );
    }
}
