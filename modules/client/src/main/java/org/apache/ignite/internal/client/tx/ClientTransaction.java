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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;

/**
 * Client transaction.
 */
public class ClientTransaction implements Transaction {
    public static final int NO_COMMIT_PARTITION = -1;

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

    private UUID txId;

    private int commitPartition = NO_COMMIT_PARTITION;

    private UUID coordId;

    private String nodeName;

    /**
     * Constructor.
     *
     * @param ch Channel that the transaction belongs to.
     * @param id Transaction id.
     * @param isReadOnly Read-only flag.
     * @param txId Transaction id.
     * @param ctup The tuple representing commit partition information (node, partition).
     * @param coordId Tx coordinator id.
     */
    public ClientTransaction(ClientChannel ch, long id, boolean isReadOnly, UUID txId, IgniteBiTuple<String, Integer> ctup, UUID coordId) {
        this.ch = ch;
        this.id = id;
        this.isReadOnly = isReadOnly;
        this.txId = txId;
        this.nodeName = ch.protocolContext().clusterNode().name();

        // If mapping is known, assign commit partition.
        if (ctup.get1() != null) {
            assert ctup.get1().equals(this.nodeName) : "Invalid node name: " + ctup.get1() + " " + this.nodeName;

            this.commitPartition = ctup.get2();
        }

        this.coordId = coordId;
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

    public int commitPartition() {
        return commitPartition;
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
}
