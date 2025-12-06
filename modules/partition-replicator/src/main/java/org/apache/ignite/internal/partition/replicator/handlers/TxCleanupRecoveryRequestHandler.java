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

package org.apache.ignite.internal.partition.replicator.handlers;

import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.internal.tx.TxState.isFinalState;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.message.TxCleanupRecoveryRequest;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageClosedException;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageDestroyedException;
import org.apache.ignite.internal.util.Cursor;

/**
 * Handler for {@link TxCleanupRecoveryRequest}s.
 */
public class TxCleanupRecoveryRequestHandler {
    private static final IgniteLogger LOG = Loggers.forClass(TxCleanupRecoveryRequestHandler.class);
    private static final int THROTTLE_BATCH_SIZE = 1000;

    private final TxStatePartitionStorage txStatePartitionStorage;
    private final TxManager txManager;
    private final FailureProcessor failureProcessor;
    private final ZonePartitionId replicationGroupId;

    /** Constructor. */
    public TxCleanupRecoveryRequestHandler(
            TxStatePartitionStorage txStatePartitionStorage,
            TxManager txManager,
            FailureProcessor failureProcessor,
            ZonePartitionId replicationGroupId
    ) {
        this.txStatePartitionStorage = txStatePartitionStorage;
        this.txManager = txManager;
        this.failureProcessor = failureProcessor;
        this.replicationGroupId = replicationGroupId;
    }

    /**
     * Handles a {@link TxCleanupRecoveryRequest}.
     *
     * @param request Request to handle.
     * @return Future completed when the request has been handled.
     */
    public CompletableFuture<Void> handle(TxCleanupRecoveryRequest request) {
        runPersistentStorageScan();

        return nullCompletedFuture();
    }

    private void runPersistentStorageScan() {
        int committedCount = 0;
        int abortedCount = 0;
        List<IgniteBiTuple<UUID, TxMeta>> tasks = new ArrayList<>();

        try (Cursor<IgniteBiTuple<UUID, TxMeta>> txs = txStatePartitionStorage.scan()) {
            for (IgniteBiTuple<UUID, TxMeta> tx : txs) {
                UUID txId = tx.getKey();
                TxMeta txMeta = tx.getValue();

                assert !txMeta.enlistedPartitions().isEmpty();

                assert isFinalState(txMeta.txState()) : "Unexpected state [txId=" + txId + ", state=" + txMeta.txState() + "].";

                if (txMeta.txState() == COMMITTED) {
                    committedCount++;
                } else {
                    abortedCount++;
                }

                tasks.add(tx);
            }
        } catch (IgniteInternalException e) {
            // TODO: https://issues.apache.org/jira/browse/IGNITE-25302 - remove this IF after proper stop is implemented.
            if (!hasCause(e, TxStateStorageClosedException.class, TxStateStorageDestroyedException.class)) {
                String errorMessage = String.format("Failed to scan transaction state storage [commitPartition=%s].", replicationGroupId);
                failureProcessor.process(new FailureContext(e, errorMessage));
            }
        }

        LOG.debug("Persistent storage scan finished [committed={}, aborted={}].", committedCount, abortedCount);

        if (!tasks.isEmpty()) {
            throttledCleanup(new CopyOnWriteArrayList<>(tasks));
        }
    }

    private void throttledCleanup(List<IgniteBiTuple<UUID, TxMeta>> tasks) {
        if (tasks.isEmpty()) {
            return;
        }

        List<IgniteBiTuple<UUID, TxMeta>> batch = tasks.subList(0, Math.min(tasks.size(), THROTTLE_BATCH_SIZE));

        List<IgniteBiTuple<UUID, TxMeta>> toCleanup = new ArrayList<>(batch);
        batch.clear();

        try {
            callCleanup(toCleanup).whenComplete((r, e) -> throttledCleanup(tasks));
        } catch (IgniteInternalException e) {
            // TODO: https://issues.apache.org/jira/browse/IGNITE-25302 - remove this IF after proper stop is implemented.
            if (!hasCause(e, TxStateStorageClosedException.class, TxStateStorageDestroyedException.class)) {
                String errorMessage = String.format("Failed to cleanup transaction states [commitPartition=%s].", replicationGroupId);
                failureProcessor.process(new FailureContext(e, errorMessage));
            }
        }
    }

    private CompletableFuture<?> callCleanup(List<IgniteBiTuple<UUID, TxMeta>> tasks) {
        CompletableFuture<?>[] array = tasks.stream().map(task -> callCleanup(task.getValue(), task.getKey()))
                .toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(array);
    }

    private CompletableFuture<?> callCleanup(TxMeta txMeta, UUID txId) {
        return txManager.cleanup(
                replicationGroupId,
                txMeta.enlistedPartitions(),
                txMeta.txState() == COMMITTED,
                txMeta.commitTimestamp(),
                txId
        ).exceptionally(throwable -> {
            LOG.warn("Failed to cleanup transaction [txId={}].", throwable, txId);

            return null;
        });
    }
}
