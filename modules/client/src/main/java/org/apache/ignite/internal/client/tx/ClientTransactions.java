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
import static org.apache.ignite.internal.client.tx.ClientTransaction.EMPTY;
import static org.apache.ignite.internal.util.ViewUtils.sync;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.PayloadInputChannel;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;

/**
 * Client transactions implementation.
 */
public class ClientTransactions implements IgniteTransactions {
    /** 0 timeout is used as a flag to use the configured timeout. */
    public static final int USE_CONFIGURED_TIMEOUT_DEFAULT = 0;

    /** Channel. */
    private final ReliableChannel ch;

    /**
     * Constructor.
     *
     * @param ch Channel.
     */
    public ClientTransactions(ReliableChannel ch) {
        this.ch = ch;
    }

    /** {@inheritDoc} */
    @Override
    public Transaction begin(@Nullable TransactionOptions options) {
        return sync(beginAsync(options));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Transaction> beginAsync(@Nullable TransactionOptions options) {
        return completedFuture(new ClientLazyTransaction(ch.observableTimestamp(), options));
    }

    /**
     * Begins the transaction on any node.
     *
     * @param ch Reliable channel.
     * @param options The options.
     * @param observableTimestamp The timestamp.
     * @param channelResolver Client channel resolver.
     *
     * @return The future.
     */
    static CompletableFuture<ClientTransaction> beginAsync(
            ReliableChannel ch,
            @Nullable TransactionOptions options,
            long observableTimestamp,
            Supplier<CompletableFuture<ClientChannel>> channelResolver
    ) {
        boolean readOnly = options != null && options.readOnly();
        long timeout = options == null ? USE_CONFIGURED_TIMEOUT_DEFAULT : options.timeoutMillis();

        return ch.serviceAsync(
                ClientOp.TX_BEGIN,
                w -> {
                    w.out().packBoolean(readOnly);
                    w.out().packLong(timeout);
                    w.out().packLong(observableTimestamp);
                },
                r -> readTx(r, ch, readOnly, timeout),
                channelResolver,
                null,
                false);
    }

    private static ClientTransaction readTx(
            PayloadInputChannel r,
            ReliableChannel ch,
            boolean isReadOnly,
            long timeout
    ) {
        ClientMessageUnpacker in = r.in();

        long id = in.unpackLong();

        return new ClientTransaction(r.clientChannel(), ch, id, isReadOnly, EMPTY, null, EMPTY, null, timeout);
    }
}
