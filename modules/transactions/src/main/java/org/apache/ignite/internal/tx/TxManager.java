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

package org.apache.ignite.internal.tx;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * A transaction manager.
 */
public interface TxManager extends IgniteComponent {
    /**
     * Starts a read-write transaction coordinated by a local node.
     *
     * @param timestampTracker Observable timestamp tracker is used to track a timestamp for either read-write or read-only
     *         transaction execution. The tracker is also used to determine the read timestamp for read-only transactions.
     * @return The transaction.
     */
    InternalTransaction begin(HybridTimestampTracker timestampTracker);

    /**
     * Starts either read-write or read-only transaction, depending on {@code readOnly} parameter value. The transaction has
     * {@link TxPriority#NORMAL} priority.
     *
     * @param timestampTracker Observable timestamp tracker is used to track a timestamp for either read-write or read-only
     *         transaction execution. The tracker is also used to determine the read timestamp for read-only transactions. Each client
     *         should pass its own tracker to provide linearizability between read-write and read-only transactions started by this client.
     * @param readOnly {@code true} in order to start a read-only transaction, {@code false} in order to start read-write one.
     *         Calling begin with readOnly {@code false} is an equivalent of TxManager#begin().
     * @return The started transaction.
     * @throws IgniteInternalException with {@link Transactions#TX_READ_ONLY_TOO_OLD_ERR} if transaction much older than the data
     *         available in the tables.
     */
    InternalTransaction begin(HybridTimestampTracker timestampTracker, boolean readOnly);

    /**
     * Starts either read-write or read-only transaction, depending on {@code readOnly} parameter value.
     *
     * @param timestampTracker Observable timestamp tracker is used to track a timestamp for either read-write or read-only
     *         transaction execution. The tracker is also used to determine the read timestamp for read-only transactions. Each client
     *         should pass its own tracker to provide linearizability between read-write and read-only transactions started by this client.
     * @param readOnly {@code true} in order to start a read-only transaction, {@code false} in order to start read-write one.
     *         Calling begin with readOnly {@code false} is an equivalent of TxManager#begin().
     * @param priority Transaction priority. The priority is used to resolve conflicts between transactions. The higher priority is
     *         the more likely the transaction will win the conflict.
     * @return The started transaction.
     * @throws IgniteInternalException with {@link Transactions#TX_READ_ONLY_TOO_OLD_ERR} if transaction much older than the data
     *         available in the tables.
     */
    InternalTransaction begin(HybridTimestampTracker timestampTracker, boolean readOnly, TxPriority priority);

    /**
     * Returns a transaction state meta.
     *
     * @param txId Transaction id.
     * @return The state meta or null if the state is unknown.
     */
    @Nullable TxStateMeta stateMeta(UUID txId);

    /**
     * Atomically changes the state meta of a transaction.
     *
     * @param txId Transaction id.
     * @param updater Transaction meta updater.
     * @return Updated transaction state.
     */
    @Nullable
    <T extends TxStateMeta> T updateTxMeta(UUID txId, Function<TxStateMeta, TxStateMeta> updater);

    /**
     * Returns lock manager.
     *
     * @return Lock manager for the given transactions manager.
     * @deprecated Use lockManager directly.
     */
    @Deprecated
    LockManager lockManager();

    /**
     * Execute transaction cleanup asynchronously.
     *
     * @param runnable Cleanup action.
     * @return Future that completes once the cleanup action finishes.
     */
    CompletableFuture<Void> executeCleanupAsync(Runnable runnable);

    /**
     * Execute transaction cleanup asynchronously.
     *
     * @param action Cleanup action.
     * @return Future that completes once the cleanup action finishes.
     */
    CompletableFuture<?> executeCleanupAsync(Supplier<CompletableFuture<?>> action);

    /**
     * Finishes a one-phase committed transaction. This method doesn't contain any distributed communication.
     *
     * @param timestampTracker Observable timestamp tracker. This tracker is used to track an observable timestamp and should be
     *         updated with commit timestamp of every committed transaction.
     * @param txId Transaction id.
     * @param commit {@code True} if a commit requested.
     */
    void finishFull(HybridTimestampTracker timestampTracker, UUID txId, boolean commit);

    /**
     * Finishes a dependant transactions.
     *
     * @param timestampTracker Observable timestamp tracker is used to track a timestamp for either read-write or read-only
     *         transaction execution. The tracker is also used to determine the read timestamp for read-only transactions. Each client
     *         should pass its own tracker to provide linearizability between read-write and read-only transactions started by this client.
     * @param commitPartition Partition to store a transaction state.
     * @param commit {@code true} if a commit requested.
     * @param enlistedGroups Enlisted partition groups with consistency token.
     * @param txId Transaction id.
     */
    CompletableFuture<Void> finish(
            HybridTimestampTracker timestampTracker,
            TablePartitionId commitPartition,
            boolean commit,
            Map<TablePartitionId, Long> enlistedGroups,
            UUID txId
    );

    /**
     * Sends cleanup request to the cluster nodes that hosts primary replicas for the enlisted partitions.
     *
     * @param partitions Enlisted partition groups.
     * @param commit {@code true} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @param txId Transaction id.
     * @return Completable future of Void.
     */
    CompletableFuture<Void> cleanup(
            Collection<TablePartitionId> partitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    );

    /**
     * Sends cleanup request to the nodes than initiated recovery.
     *
     * @param node Target node.
     * @param txId Transaction id.
     * @return Completable future of Void.
     */
    CompletableFuture<Void> cleanup(String node, UUID txId);

    /**
     * Returns a number of finished transactions.
     *
     * @return A number of finished transactions.
     */
    @TestOnly
    int finished();

    /**
     * Returns a number of pending transactions, that is, transactions that have not yet been committed or rolled back.
     *
     * @return A number of pending transactions.
     */
    @TestOnly
    int pending();

    /**
     * Updates the low watermark, the value is expected to only increase.
     *
     * <p>All new read-only transactions will need to be created with a read time greater than this value.
     *
     * @param newLowWatermark New low watermark.
     * @return Future of all read-only transactions with read timestamp less or equals the given new low watermark.
     */
    CompletableFuture<Void> updateLowWatermark(HybridTimestamp newLowWatermark);

    /**
     * Registers the infligh update for a transaction.
     *
     * @param txId The transaction id.
     * @return {@code True} if the inflight was registered. The update must be failed on false.
     */
    boolean addInflight(UUID txId);

    /**
     * Unregisters the inflight for a transaction.
     *
     * @param txId The transction id
     */
    void removeInflight(UUID txId);
}
