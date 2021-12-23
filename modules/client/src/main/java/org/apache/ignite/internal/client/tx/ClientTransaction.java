/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;

/**
 * Client transaction.
 */
public class ClientTransaction implements Transaction {
    /** Open state. */
    private static final int STATE_OPEN = 0;

    /** Committed state. */
    private static final int STATE_COMMITTED = 1;

    /** Rolled back state. */
    private static final int STATE_ROLLED_BACK = 2;

    /** Channel. */
    private final ReliableChannel ch;

    /** Transaction id. */
    private final long id;

    /** State. */
    private final AtomicInteger state = new AtomicInteger(STATE_OPEN);

    /**
     * Constructor.
     *
     * @param ch Channel.
     * @param id Transaction id.
     */
    public ClientTransaction(ReliableChannel ch, long id) {
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

    /** {@inheritDoc} */
    @Override
    public void commit() throws TransactionException {
        commitAsync().join();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> commitAsync() {
        setState(STATE_COMMITTED);

        return ch.serviceAsync(ClientOp.TX_COMMIT, w -> w.out().packLong(id), r -> null);
    }

    /** {@inheritDoc} */
    @Override
    public void rollback() throws TransactionException {
        rollbackAsync().join();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> rollbackAsync() {
        setState(STATE_ROLLED_BACK);

        return ch.serviceAsync(ClientOp.TX_ROLLBACK, w -> w.out().packLong(id), r -> null);
    }

    private void setState(int state) {
        if (this.state.compareAndSet(STATE_OPEN, state)) {
            return;
        }

        String message = this.state.get() == STATE_COMMITTED
                ? "Transaction is already committed."
                : "Transaction is already rolled back.";

        throw new TransactionException(message);
    }
}
