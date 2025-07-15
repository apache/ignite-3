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

package org.apache.ignite.internal.tx.storage.state;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.storage.engine.MvPartitionMeta;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.jetbrains.annotations.Nullable;

/**
 * Storage for transaction meta, {@link TxMeta}.
 */
public interface TxStatePartitionStorage extends ManuallyCloseable {
    /**
     * Value of the {@link #lastAppliedIndex()} and {@link #lastAppliedTerm()} during rebalance of transaction state storage.
     *
     * <p>Allows to determine on a node restart that rebalance has not been completed and storage should be cleared before using it.
     */
    long REBALANCE_IN_PROGRESS = -1;

    /**
     * Returns tx meta by tx id.
     *
     * @param txId Tx id.
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_ERR} error code in case when the operation has failed.
     */
    @Nullable TxMeta get(UUID txId);

    /**
     * Puts the tx meta into the storage. WARNING: this method should be used only within the rebalance, because it doesn't update
     * the index and the term in the storage. Index and term are updated after the rebalance is finished,
     * see {@link #finishRebalance}.
     *
     * @param txId Tx id.
     * @param txMeta Tx meta.
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_ERR} error code in case when the operation has failed.
     */
    void putForRebalance(UUID txId, TxMeta txMeta);

    /**
     * Atomically changes the tx meta in the storage. If transaction meta that is already in the storage, is equal to {@code txMeta}, the
     * operation also succeeds.
     *
     * @param txId Tx id.
     * @param txStateExpected Tx state that is expected to be in the storage.
     * @param txMeta Tx meta.
     * @param commandIndex New value for {@link #lastAppliedIndex()}.
     * @param commandTerm New value for {@link #lastAppliedTerm()}.
     * @return Whether the CAS operation is successful.
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_ERR} error code in case when the operation has failed.
     */
    boolean compareAndSet(UUID txId, @Nullable TxState txStateExpected, TxMeta txMeta, long commandIndex, long commandTerm);

    /**
     * Removes the tx meta from the storage.
     *
     * @param txId Tx id.
     * @param commandIndex New value for {@link #lastAppliedIndex()}.
     * @param commandTerm New value for {@link #lastAppliedTerm()}.
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_ERR} error code in case when the operation has failed.
     */
    void remove(UUID txId, long commandIndex, long commandTerm);

    /**
     * Removes all the given transaction metas from the storage.
     *
     * @param txIds Tx ids.
     * @param commandIndex New value for {@link #lastAppliedIndex()}.
     * @param commandTerm New value for {@link #lastAppliedTerm()}.
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_ERR} error code in case when the operation has failed.
     */
    void removeAll(Collection<UUID> txIds, long commandIndex, long commandTerm);

    /**
     * Creates a cursor to scan all data in the storage.
     *
     * <p>The cursor yields exactly the data that was existing in the storage at the moment when the method was called.
     *
     * <p>The cursor yields data ordered by transaction ID interpreted as an unsigned 128 bit integer.
     */
    Cursor<IgniteBiTuple<UUID, TxMeta>> scan();

    /**
     * Flushes current state of the data or <i>the state from the nearest future</i> to the storage. It means that the future can be
     * completed when persisted index is higher than last applied index at the moment of the method's call.
     *
     * @return Future that's completed when flushing of the data is completed.
     */
    CompletableFuture<Void> flush();

    /**
     * Returns index of the highest write command applied to the storage. {@code 0} if index is unknown.
     */
    long lastAppliedIndex();

    /**
     * Returns term of the highest write command applied to the storage. {@code 0} if term is unknown.
     */
    long lastAppliedTerm();

    /**
     * Sets the last applied index and term.
     *
     * @param lastAppliedIndex Last applied index.
     * @param lastAppliedTerm Last applied term.
     */
    void lastApplied(long lastAppliedIndex, long lastAppliedTerm);

    /**
     * Closes the storage.
     *
     * <p>REQUIRED: For background tasks for storage, such as rebalancing, to be completed by the time the method is called.
     */
    @Override
    void close();

    /**
     * Closes and removes all data from the storage.
     *
     * <p>The destruction is not guaranteed to be durable (that is, if a node stops/crashes before persisting this change to disk,
     * the storage might still be there after node restart).
     *
     * <p>REQUIRED: For background tasks for storage, such as rebalancing, to be completed by the time the method is called.
     */
    void destroy();

    /**
     * Prepares the transaction state storage for rebalance.
     * <ul>
     *     <li>Clears the storage;</li>
     *     <li>Sets the {@link #lastAppliedIndex()} and {@link #lastAppliedTerm()} to {@link #REBALANCE_IN_PROGRESS};</li>
     *     <li>Stops the cursors of a transaction state storage, subsequent calls to {@link Cursor#hasNext()} and
     *     {@link Cursor#next()} will throw {@link IgniteInternalException} with {@link Transactions#TX_STATE_STORAGE_REBALANCE_ERR};</li>
     *     <li>For a transaction state storage, methods for reading and writing data will throw {@link IgniteInternalException} with
     *     {@link Transactions#TX_STATE_STORAGE_REBALANCE_ERR} except:<ul>
     *         <li>{@link TxStatePartitionStorage#putForRebalance(UUID, TxMeta)};</li>
     *         <li>{@link TxStatePartitionStorage#lastAppliedIndex()};</li>
     *         <li>{@link TxStatePartitionStorage#lastAppliedTerm()}} ()};</li>
     *     </ul></li>
     * </ul>
     *
     * <p>This method must be called before every rebalance of transaction state storage and ends with a call to one of the methods:
     * <ul>
     *     <li>{@link #abortRebalance} - in case of errors or cancellation of rebalance;</li>
     *     <li>{@link #finishRebalance} - in case of successful completion of rebalance.</li>
     * </ul>
     *
     * <p>If the {@link #lastAppliedIndex()} is {@link #REBALANCE_IN_PROGRESS} after a node restart, then the storage needs to be
     * cleared before it start.
     *
     * <p>If the partition started to be destroyed or closed, then there will be an error when trying to start rebalancing.
     *
     * @return Future of the start rebalance for transaction state storage.
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_REBALANCE_ERR} error code in case when the operation
     *      has failed.
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_STOPPED_ERR} if the storage is closed or destroyed.
     */
    CompletableFuture<Void> startRebalance();

    /**
     * Aborts rebalance for transaction state storage: clears the storage, sets the {@link #lastAppliedIndex()} and
     * {@link #lastAppliedTerm()} to {@code 0}.
     *
     * <p>After calling this method, methods for writing and reading will be available.
     *
     * <p>If rebalance has not started, then nothing will happen.
     *
     * @return Future of the abort rebalance for transaction state storage.
     */
    CompletableFuture<Void> abortRebalance();

    /**
     * Completes rebalance for transaction state storage: updates {@link #lastAppliedIndex}, {@link #lastAppliedTerm},
     * {@link #committedGroupConfiguration} and {@link #leaseInfo}.
     *
     * <p>After calling this method, methods for writing and reading will be available.
     *
     * <p>If rebalance has not started, then an IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_REBALANCE_ERR}
     * will be thrown
     *
     * @param partitionMeta Metadata of the partition.
     * @return Future of the finish rebalance for transaction state storage.
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_REBALANCE_ERR} error code in case when the operation
     *      has failed.
     */
    CompletableFuture<Void> finishRebalance(MvPartitionMeta partitionMeta);

    /**
     * Clears transaction state storage. After the cleaning is completed, the storage will be fully available.
     * <ul>
     *     <li>Cancels all current operations (including cursors) with storage and waits for their completion;</li>
     *     <li>Does not allow operations to be performed (exceptions will be thrown) with the storage until the cleaning is completed;</li>
     *     <li>Clears storage;</li>
     *     <li>Sets the {@link #lastAppliedIndex()} and {@link #lastAppliedTerm()} to {@code 0};</li>
     *     <li>Once the storage cleanup is complete (success or error), allows to perform all storage operations.</li>
     * </ul>
     *
     * @return Future of transaction state storage cleanup.
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_STOPPED_ERR} if the storage is closed or destroyed.
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_REBALANCE_ERR} if the storage is in process of rebalance.
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_ERR} if the storage is in progress of cleanup or failed for
     *      another reason.
     */
    CompletableFuture<Void> clear();

    /**
     * Updates the replication protocol group configuration.
     */
    void committedGroupConfiguration(byte[] config, long index, long term);

    /**
     * Byte representation of the committed replication protocol group configuration corresponding to the write command with the highest
     * index applied to the storage or {@code null} if it was never saved.
     */
    byte @Nullable [] committedGroupConfiguration();

    /**
     * Updates the current lease information.
     */
    void leaseInfo(LeaseInfo leaseInfo, long index, long term);

    /**
     * Returns the current lease information of {@code null} if it was never saved.
     */
    @Nullable LeaseInfo leaseInfo();

    /**
     * Updates the current snapshot information.
     */
    void snapshotInfo(byte[] snapshotInfo);

    /**
     * Returns the current snapshot information of {@code null} if it was never saved.
     */
    byte @Nullable [] snapshotInfo();
}
