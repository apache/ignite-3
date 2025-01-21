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
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * A transaction manager.
 */
public interface TxManager extends IgniteComponent {
    /**
     * Starts an implicit read-write transaction coordinated by a local node.
     *
     * @param timestampTracker Observable timestamp tracker is used to track a timestamp for either read-write or read-only
     *         transaction execution. The tracker is also used to determine the read timestamp for read-only transactions.
     * @return The transaction.
     */
    default InternalTransaction beginImplicitRw(HybridTimestampTracker timestampTracker) {
        return beginImplicit(timestampTracker, false);
    }

    /**
     * Starts an implicit read-only transaction coordinated by a local node.
     *
     * @param timestampTracker Observable timestamp tracker is used to track a timestamp for either read-write or read-only
     *         transaction execution. The tracker is also used to determine the read timestamp for read-only transactions.
     * @return The transaction.
     */
    default InternalTransaction beginImplicitRo(HybridTimestampTracker timestampTracker) {
        return beginImplicit(timestampTracker, true);
    }

    /**
     * Starts an implicit transaction coordinated by a local node.
     *
     * @param timestampTracker Observable timestamp tracker is used to track a timestamp for either read-write or read-only
     *         transaction execution. The tracker is also used to determine the read timestamp for read-only transactions.
     * @param readOnly {@code true} in order to start a read-only transaction, {@code false} in order to start read-write one.
     * @return The transaction.
     */
    InternalTransaction beginImplicit(HybridTimestampTracker timestampTracker, boolean readOnly);

    /**
     * Starts an explicit read-write transaction coordinated by a local node.
     *
     * @param timestampTracker Observable timestamp tracker is used to track a timestamp for either read-write or read-only
     *         transaction execution. The tracker is also used to determine the read timestamp for read-only transactions.
     * @param options Transaction options.
     * @return The transaction.
     */
    default InternalTransaction beginExplicitRw(HybridTimestampTracker timestampTracker, InternalTxOptions options) {
        return beginExplicit(timestampTracker, false, options);
    }

    /**
     * Starts an explicit read-only transaction coordinated by a local node.
     *
     * @param timestampTracker Observable timestamp tracker is used to track a timestamp for either read-write or read-only
     *         transaction execution. The tracker is also used to determine the read timestamp for read-only transactions.
     * @param options Transaction options.
     * @return The transaction.
     */
    default InternalTransaction beginExplicitRo(HybridTimestampTracker timestampTracker, InternalTxOptions options) {
        return beginExplicit(timestampTracker, true, options);
    }

    /**
     * Starts either read-write or read-only explicit transaction, depending on {@code readOnly} parameter value.
     *
     * @param timestampTracker Observable timestamp tracker is used to track a timestamp for either read-write or read-only
     *         transaction execution. The tracker is also used to determine the read timestamp for read-only transactions. Each client
     *         should pass its own tracker to provide linearizability between read-write and read-only transactions started by this client.
     * @param readOnly {@code true} in order to start a read-only transaction, {@code false} in order to start read-write one.
     * @param txOptions Options.
     * @return The started transaction.
     */
    InternalTransaction beginExplicit(HybridTimestampTracker timestampTracker, boolean readOnly, InternalTxOptions txOptions);

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
     *         updated with commit timestamp of every committed transaction. Not null on commit.
     * @param txId Transaction id.
     * @param ts The timestamp which is associated to txn completion.
     * @param commit {@code True} if a commit requested.
     */
    void finishFull(HybridTimestampTracker timestampTracker, UUID txId, @Nullable HybridTimestamp ts, boolean commit);

    /**
     * Finishes a dependant transactions.
     *
     * @param timestampTracker Observable timestamp tracker is used to determine the read timestamp for read-only transactions. Each client
     *         should pass its own tracker to provide linearizability between read-write and read-only transactions started by this client.
     * @param commitPartition Partition to store a transaction state.
     * @param commit {@code true} if a commit requested.
     * @param enlistedGroups Enlisted partition groups with consistency tokens.
     * @param txId Transaction id.
     */
    CompletableFuture<Void> finish(
            HybridTimestampTracker timestampTracker,
            TablePartitionId commitPartition,
            boolean commit,
            Map<TablePartitionId, IgniteBiTuple<ClusterNode, Long>> enlistedGroups,
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
            TablePartitionId commitPartitionId,
            Map<TablePartitionId, String> enlistedPartitions,
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
            TablePartitionId commitPartitionId,
            Collection<TablePartitionId> enlistedPartitions,
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
    CompletableFuture<Void> cleanup(TablePartitionId commitPartitionId, String node, UUID txId);

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
