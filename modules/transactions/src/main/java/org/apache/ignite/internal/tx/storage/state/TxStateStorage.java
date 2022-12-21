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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * Storage for transaction meta, {@link TxMeta}.
 */
public interface TxStateStorage extends ManuallyCloseable {
    /**
     * Value of the {@link #lastAppliedIndex()} and {@link #lastAppliedTerm()} during a full rebalance of transaction state storage.
     *
     * <p>Allows to determine on a node restart that a full rebalance has not been completed and storage should be cleared before using it.
     */
    long FULL_REBALANCE_IN_PROGRESS = -1;

    /**
     * Returns tx meta by tx id.
     *
     * @param txId Tx id.
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_ERR} error code in case when the operation has failed.
     */
    @Nullable TxMeta get(UUID txId);

    /**
     * Puts the tx meta into the storage.
     *
     * @param txId Tx id.
     * @param txMeta Tx meta.
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_ERR} error code in case when the operation has failed.
     */
    void put(UUID txId, TxMeta txMeta);

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
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_ERR} error code in case when the operation has failed.
     */
    void remove(UUID txId);

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
     * Returns {@link #lastAppliedIndex()} value consistent with the data, already persisted on the storage.
     */
    long persistedIndex();

    /**
     * Closes the storage.
     */
    @Override
    void close();

    /**
     * Closes and removes all data from the storage.
     *
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_ERR} error code in case when the operation has failed.
     */
    void destroy();

    /**
     * Prepares the transaction state storage for a full rebalance: clears the storage, sets the {@link #lastAppliedIndex()} and
     * {@link #lastAppliedTerm()} to {@link #FULL_REBALANCE_IN_PROGRESS}, and closes all cursors.
     *
     * <p>After calling this method, only write methods will be available, and read methods with {@link #lastApplied(long, long)} will
     * throw {@link IgniteInternalException}.
     *
     * <p>This method must be called before every full rebalance of transaction state storage and ends with a call to one of the methods:
     * <ul>
     *     <li>{@link #abortFullRebalance()} - in case of errors or cancellation of a full rebalance;</li>
     *     <li>{@link #finishFullRebalance(long, long)} - in case of successful completion of a full rebalance.</li>
     * </ul>
     *
     * <p>If the {@link #lastAppliedIndex()} is {@link #FULL_REBALANCE_IN_PROGRESS} after a node restart, then the storage needs to be
     * cleared before it can be used.
     *
     * @return Future of the start a full rebalance for transaction state storage.
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_ERR} error code in case when the operation has failed.
     */
    CompletableFuture<Void> startFullRebalance();

    /**
     * Aborts a full rebalance for transaction state storage: clears the storage, sets the {@link #lastAppliedIndex()} and
     * {@link #lastAppliedTerm()} to {@code 0}.
     *
     * <p>After calling this method, methods for writing and reading will be available.
     *
     * <p>If a full rebalance has not started, then nothing will happen.
     *
     * @return Future of the abort a full rebalance for transaction state storage.
     */
    CompletableFuture<Void> abortFullRebalance();

    /**
     * Completes a full rebalance for transaction state storage: updates {@link #lastAppliedIndex()} and {@link #lastAppliedTerm()}.
     *
     * <p>After calling this method, methods for writing and reading will be available.
     *
     * <p>If a full rebalance has not started, then nothing will happen.
     *
     * @param lastAppliedIndex Last applied index.
     * @param lastAppliedTerm Last applied term.
     * @return Future of the finish a full rebalance for transaction state storage.
     */
    CompletableFuture<Void> finishFullRebalance(long lastAppliedIndex, long lastAppliedTerm);
}
