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

package org.apache.ignite.internal.client.tx;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.ViewUtils.sync;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_ERR;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.PartitionMapping;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Client transaction.
 */
public class ClientTransaction implements Transaction {
    private static final int NO_COMMIT_PARTITION = -1;

    /** Open state. */
    private static final int STATE_OPEN = 0;

    /** Committed state. */
    private static final int STATE_COMMITTED = 1;

    /** Rolled back state. */
    private static final int STATE_ROLLED_BACK = 2;

    /** Channel that the transaction belongs to. */
    private final ClientChannel ch;

    /** Transaction id. */
    private final long id;

    /** The future used on repeated commit/rollback. */
    private final AtomicReference<CompletableFuture<Void>> finishFut = new AtomicReference<>();

    /** State. */
    private final AtomicInteger state = new AtomicInteger(STATE_OPEN);

    /** Read-only flag. */
    private final boolean isReadOnly;

    private final UUID txId;

    private final int commitTableId;

    private final int commitPartition;

    private final UUID coordId;

    private final String nodeName;

    /** Direct enlistment map. */
    private final Map<ReplicationGroupId, CompletableFuture<Long>> enlisted = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param ch Channel that the transaction belongs to.
     * @param id Transaction id.
     * @param isReadOnly Read-only flag.
     * @param txId Transaction id.
     * @param pm The partition mapping or {@code null} if not known.
     * @param coordId Tx coordinator id.
     */
    public ClientTransaction(ClientChannel ch, long id, boolean isReadOnly, UUID txId, @Nullable PartitionMapping pm, UUID coordId) {
        this.ch = ch;
        this.id = id;
        this.isReadOnly = isReadOnly;
        this.txId = txId;
        this.nodeName = ch.protocolContext().clusterNode().name();

        // If mapping is known, assign commit partition.
        if (pm != null) {
            assert pm.node().equals(this.nodeName) : "Invalid node name: " + pm.node() + " " + this.nodeName;

            this.commitTableId = pm.tableId();
            this.commitPartition = pm.partition();
        } else {
            this.commitTableId = NO_COMMIT_PARTITION;
            this.commitPartition = NO_COMMIT_PARTITION;
        }

        this.coordId = coordId;

        assert this.coordId != null;
    }

    /**
     * Gets the id.
     *
     * @return Id.
     */
    public long id() {
        return id;
    }

    public UUID txId() {
        return txId;
    }

    public int commitTableId() {
        return commitTableId;
    }

    public int commitPartition() {
        return commitPartition;
    }

    private boolean hasCommitPartition() {
        return commitPartition != NO_COMMIT_PARTITION;
    }

    public UUID coordinatorId() {
        return coordId;
    }

    /**
     * Get coordinator node name.
     *
     * @return The name.
     */
    public String nodeName() {
        return nodeName;
    }

    /**
     * Gets the associated channel.
     *
     * @return Channel.
     */
    public ClientChannel channel() {
        return ch;
    }

    /** {@inheritDoc} */
    @Override
    public void commit() throws TransactionException {
        sync(commitAsync());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> commitAsync() {
        if (!finishFut.compareAndSet(null, new CompletableFuture<>())) {
            return finishFut.get();
        }

        setState(STATE_COMMITTED);

        CompletableFuture<Void> mainFinishFut = ch.serviceAsync(ClientOp.TX_COMMIT, w -> w.out().packLong(id), r -> null);

        mainFinishFut.handle((res, e) -> finishFut.get().complete(null));

        return mainFinishFut;
    }

    /** {@inheritDoc} */
    @Override
    public void rollback() throws TransactionException {
        sync(rollbackAsync());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> rollbackAsync() {
        if (!finishFut.compareAndSet(null, new CompletableFuture<>())) {
            return finishFut.get();
        }

        setState(STATE_ROLLED_BACK);

        CompletableFuture<Void> mainFinishFut = ch.serviceAsync(ClientOp.TX_ROLLBACK, w -> w.out().packLong(id), r -> null);

        mainFinishFut.handle((res, e) -> finishFut.get().complete(null));

        return mainFinishFut;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }

    /**
     * Gets the internal transaction from the given public transaction. Throws an exception if the given transaction is
     * not an instance of {@link ClientTransaction}.
     *
     * @param tx Public transaction.
     * @return Internal transaction.
     */
    public static ClientTransaction get(Transaction tx) {
        if (!(tx instanceof ClientLazyTransaction)) {
            throw unsupportedTxTypeException(tx);
        }

        ClientTransaction clientTx = ((ClientLazyTransaction) tx).startedTx();

        int state = clientTx.state.get();

        if (state == STATE_OPEN) {
            return clientTx;
        }

        // Match org.apache.ignite.internal.tx.TxState enum:
        String stateStr = state == STATE_COMMITTED ? "COMMITTED" : "ABORTED";

        throw new TransactionException(
                TX_ALREADY_FINISHED_ERR,
                format("Transaction is already finished [id={}, state={}].", clientTx.id, stateStr));
    }

    static IgniteException unsupportedTxTypeException(Transaction tx) {
        return new IgniteException(INTERNAL_ERR, "Unsupported transaction implementation: '"
                + tx.getClass()
                + "'. Use IgniteClient.transactions() to start transactions.");
    }

    private void setState(int state) {
        this.state.compareAndExchange(STATE_OPEN, state);
    }

    public @Nullable Long token(@Nullable PartitionMapping pm) {
        if (pm == null) {
            return null;
        }

        // TODO can avoid new object ?
        CompletableFuture<Long> fut = enlisted.get(new TablePartitionId(pm.tableId(), pm.partition()));

        // Check if direct mapping is active.
        if (fut == null) {
            return null;
        }

        return fut.join(); // Should be completed. TODO errors
    }

    public CompletableFuture<Void> enlistFuture(ClientChannel opChannel, @Nullable PartitionMapping pm) {
        // Check if direct mapping is active.
        if (pm != null && pm.node().equals(opChannel.protocolContext().clusterNode().name()) && hasCommitPartition()) {
            boolean[] first = {false};

            // TODO avoid new object.
            TablePartitionId tablePartitionId = new TablePartitionId(pm.tableId(), pm.partition());

            CompletableFuture<Long> fut = enlisted.compute(tablePartitionId, (k, v) -> {
                if (v == null) {
                    first[0] = true;
                    return CompletableFuture.completedFuture(0L);
                } else {
                    return v;
                }
            });

            if (first[0]) {
                // For the first request returns completed future.
                return CompletableFutures.nullCompletedFuture();
            } else {
                return fut.thenApply(ignored -> null);
            }
        }

        return CompletableFutures.nullCompletedFuture();
    }
}
