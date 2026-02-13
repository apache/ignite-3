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
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.impl.EnlistedPartitionGroup;
import org.apache.ignite.internal.tx.metrics.ResourceVacuumMetrics;
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
        return beginImplicit(timestampTracker, false, null);
    }

    /**
     * Starts an implicit read-only transaction coordinated by a local node.
     *
     * @param timestampTracker Observable timestamp tracker is used to track a timestamp for either read-write or read-only
     *         transaction execution. The tracker is also used to determine the read timestamp for read-only transactions.
     * @return The transaction.
     */
    default InternalTransaction beginImplicitRo(HybridTimestampTracker timestampTracker) {
        return beginImplicit(timestampTracker, true, null);
    }

    /**
     * Starts an implicit transaction coordinated by a local node.
     *
     * @param timestampTracker Observable timestamp tracker is used to track a timestamp for either read-write or read-only
     *         transaction execution. The tracker is also used to determine the read timestamp for read-only transactions.
     * @param readOnly {@code true} in order to start a read snapshot transaction, {@code false} in order to start read-write one.
     * @param txLabel Transaction label.
     * @return The transaction.
     */
    InternalTransaction beginImplicit(HybridTimestampTracker timestampTracker, boolean readOnly, @Nullable String txLabel);

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
     * Starts an explicit read snapshot transaction coordinated by a local node.
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
     * Begins a remote transaction.
     *
     * @param txId Tx id.
     * @param commitPartId Table partition id.
     * @param coord Tx coordinator.
     * @param token Enlistment token.
     * @param timeout The timeout.
     * @param cb Delayed ack callback.
     *
     * @return Remote transaction.
     */
    InternalTransaction beginRemote(UUID txId, ZonePartitionId commitPartId, UUID coord, long token, long timeout, Consumer<Throwable> cb);

    /**
     * Returns a transaction state meta.
     *
     * @param txId Transaction id.
     * @return The state meta or null if the state is unknown.
     */
    @Nullable TxStateMeta stateMeta(UUID txId);

    CompletableFuture<@Nullable TransactionMeta> checkEnlistedPartitionsAndAbortIfNeeded(
            TxStateMeta txMeta,
            InternalTransaction tx,
            long currentEnlistmentConsistencyToken,
            ZonePartitionId senderGroupId);

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
     * @param commit {@code true} if a commit requested.
     */
    CompletableFuture<Void> finishFull(
            HybridTimestampTracker timestampTracker,
            UUID txId,
            @Nullable HybridTimestamp ts,
            boolean commit
    );

    /**
     * Finishes a dependant transactions.
     *
     * @param timestampTracker Observable timestamp tracker is used to determine the read timestamp for read-only transactions. Each client
     *         should pass its own tracker to provide linearizability between read-write and read-only transactions started by this client.
     * @param commitPartition Partition to store a transaction state. {@code null} if nothing was enlisted into the transaction.
     * @param commitIntent {@code true} if a commit requested.
     * @param finishReason Optional finish reason (for example, timeout). Must be {@code null} for commit.
     * @param recovery {@code true} if finished by recovery.
     * @param noRemoteWrites {@code true} if remote(directly mapped) part of this transaction has no writes.
     * @param enlistedGroups Map of enlisted partitions.
     * @param txId Transaction id.
     */
    CompletableFuture<Void> finish(
            HybridTimestampTracker timestampTracker,
            @Nullable ZonePartitionId commitPartition,
            boolean commitIntent,
            @Nullable Throwable finishReason,
            boolean recovery,
            boolean noRemoteWrites,
            Map<ZonePartitionId, PendingTxPartitionEnlistment> enlistedGroups,
            UUID txId
    );

    /**
     * Sends cleanup request to the cluster nodes that hosts primary replicas for the enlisted partitions.
     *
     * <p>The nodes to send the request to are taken from the mapping `partition id -> partition primary`.
     *
     * @param commitPartitionId Commit partition id. {@code Null} for unlock only path.
     * @param enlistedPartitions Map of enlisted partitions.
     * @param commit {@code true} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @param txId Transaction id.
     * @return Completable future of Void.
     */
    CompletableFuture<Void> cleanup(
            @Nullable ZonePartitionId commitPartitionId,
            Map<ZonePartitionId, PartitionEnlistment> enlistedPartitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    );

    /**
     * Sends cleanup request to the cluster nodes that hosts primary replicas for the enlisted partitions.
     *
     * <p>The nodes to sends the request to are calculated by the placement driver.
     *
     * @param commitPartitionId Commit partition id. {@code Null} for unlock only path.
     * @param enlistedPartitions Enlisted partitions.
     * @param commit {@code true} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @param txId Transaction id.
     * @return Completable future of Void.
     */
    CompletableFuture<Void> cleanup(
            @Nullable ZonePartitionId commitPartitionId,
            Collection<EnlistedPartitionGroup> enlistedPartitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    );

    /**
     * Sends cleanup request to a node that had initiated the recovery.
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
     * @param resourceVacuumMetrics Metrics of resource vacuumizing.
     * @return Vacuum complete future.
     */
    CompletableFuture<Void> vacuum(ResourceVacuumMetrics resourceVacuumMetrics);

    /**
     * Kills a local transaction by its id. The behavior is similar to the transaction rollback.
     *
     * @param txId Transaction id.
     * @return Future will be completed with value true if the transaction was started locally and completed by this call.
     */
    CompletableFuture<Boolean> kill(UUID txId);

    /**
     * Returns lock retry count.
     *
     * @return The count.
     */
    int lockRetryCount();

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
