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
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.network.ClusterNode;
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
    <T extends TxStateMeta> T updateTxMeta(UUID txId, Function<@Nullable TxStateMeta, TxStateMeta> updater);

    /**
     * Returns lock manager.
     *
     * @return Lock manager for the given transactions manager.
     * @deprecated Use lockManager directly.
     */
    @Deprecated
    LockManager lockManager();

    /**
     * Execute write intent switch asynchronously.
     *
     * @param runnable Write intent switch action.
     * @return Future that completes once the write intent switch action finishes.
     */
    CompletableFuture<Void> executeWriteIntentSwitchAsync(Runnable runnable);

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
     * @param enlistedGroups Enlisted partition groups with consistency tokens.
     * @param txId Transaction id.
     */
    CompletableFuture<Void> finish(
            HybridTimestampTracker timestampTracker,
            ZonePartitionId commitPartition,
            boolean commit,
            Map<ZonePartitionId, IgniteBiTuple<ClusterNode, Long>> enlistedGroups,
            UUID txId
    );

    /**
     * Sends cleanup request to the cluster nodes that hosts primary replicas for the enlisted partitions.
     *
     * <p>The nodes to send the request to are taken from the mapping `partition id -> partition primary`.
     *
     * @param commitPartitionId Commit partition id.
     * @param enlistedPartitions Map of partition groups to their primary nodes.
     * @param commit {@code true} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @param txId Transaction id.
     * @return Completable future of Void.
     */
    CompletableFuture<Void> cleanup(
            ZonePartitionId commitPartitionId,
            Map<ZonePartitionId, String> enlistedPartitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    );

    /**
     * Sends cleanup request to the cluster nodes that hosts primary replicas for the enlisted partitions.
     *
     * <p>The nodes to sends the request to are calculated by the placement driver.
     *
     * @param commitPartitionId Commit partition id.
     * @param enlistedPartitions Enlisted partition groups.
     * @param commit {@code true} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @param txId Transaction id.
     * @return Completable future of Void.
     */
    CompletableFuture<Void> cleanup(
            ZonePartitionId commitPartitionId,
            Collection<ZonePartitionId> enlistedPartitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    );

    /**
     * Sends cleanup request to the nodes than initiated recovery.
     *
     * @param commitPartitionId Commit partition id.
     * @param node Target node.
     * @param txId Transaction id.
     * @return Completable future of Void.
     */
    CompletableFuture<Void> cleanup(ZonePartitionId commitPartitionId, String node, UUID txId);

    /**
     * Locally vacuums no longer needed transactional resources, like txnState both persistent and volatile.
     *
     * @return Vacuum complete future.
     */
    CompletableFuture<Void> vacuum();

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
}
