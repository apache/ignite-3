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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.client.ClientUtils.sync;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;

/**
 * Client transaction.
 */
public class ClientTransaction implements Transaction {
    /** Channel that the transaction belongs to. */
    private final ClientChannel ch;

    /** Transaction id. */
    private final long id;

    /** The future used on repeated commit/rollback. */
    private AtomicReference<CompletableFuture<Void>> finishFut = new AtomicReference<>();

    /**
     * Constructor.
     *
     * @param ch Channel that the transaction belongs to.
     * @param id Transaction id.
     */
    public ClientTransaction(ClientChannel ch, long id) {
        this.ch = ch;
        this.id = id;
    }

    /**
     * Gets the id.
     *
     * @return Id.
     */
    public long id() {
        return id;
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

        CompletableFuture<Void> mainFinishFut = ch.serviceAsync(ClientOp.TX_COMMIT, w -> w.out().packLong(id), r -> null);

        mainFinishFut.thenRun(() -> finishFut.get().complete(null));

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

        CompletableFuture<Void> mainFinishFut = ch.serviceAsync(ClientOp.TX_ROLLBACK, w -> w.out().packLong(id), r -> null);

        mainFinishFut.thenRun(() -> finishFut.get().complete(null));

        return mainFinishFut;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isReadOnly() {
        // TODO: IGNITE-17929 Add read-only support to ClientTransactions
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public HybridTimestamp readTimestamp() {
        // TODO: IGNITE-17929 Add read-only support to ClientTransactions
        return null;
    }
}
