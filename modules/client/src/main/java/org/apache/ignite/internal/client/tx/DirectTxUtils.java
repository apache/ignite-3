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
import static org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature.TX_CLIENT_GETALL_SUPPORTS_TX_OPTIONS;
import static org.apache.ignite.internal.client.proto.tx.ClientInternalTxOptions.READ_ONLY;
import static org.apache.ignite.internal.client.proto.tx.ClientTxUtils.TX_ID_DIRECT;
import static org.apache.ignite.internal.client.proto.tx.ClientTxUtils.TX_ID_FIRST_DIRECT;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Client.CONNECTION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.EnumSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.PartitionMapping;
import org.apache.ignite.internal.client.PayloadInputChannel;
import org.apache.ignite.internal.client.PayloadOutputChannel;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.WriteContext;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.tx.ClientInternalTxOptions;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Collection of helper methods to unify handling of direct transactions and piggybacking of tx start request.
 */
public class DirectTxUtils {
    /**
     * Ensures that a client-side transaction is started and ready to serve requests.
     *
     * <p>If the provided transaction is {@code null}, returns a completed future with a {@code null} result.
     * If no specific partition mapping is provided, it initiates the transaction lazily using the default channel. Otherwise, it uses the
     * provided partition mapping to determine the correct node and channel, and ensures the transaction is started accordingly.
     *
     * <p>The {@code piggybackSupported} predicate is used to determine whether transaction start piggybacking is
     * supported for the resolved {@link ClientChannel}. If piggybacking is not supported, the transaction will be started explicitly.
     *
     * @param ch The {@link ReliableChannel} used for communication with the cluster.
     * @param tx The transaction to ensure is started; may be {@code null}.
     * @param pm The partition mapping that identifies the target node; may be {@code null}.
     * @param ctx The {@link WriteContext} that may be updated if this is the first request in a transaction.
     * @param piggybackSupported A predicate that determines whether piggybacking the transaction start is supported on a given
     *         {@link ClientChannel}.
     * @return A {@link CompletableFuture} that completes when the transaction is ensured to be started, or a completed future with
     *         {@code null} if {@code tx} is {@code null}.
     */
    public static CompletableFuture<@Nullable ClientTransaction> ensureStarted(
            ReliableChannel ch,
            @Nullable Transaction tx,
            @Nullable PartitionMapping pm,
            WriteContext ctx,
            Predicate<ClientChannel> piggybackSupported
    ) {
        if (tx == null) {
            return nullCompletedFuture();
        }

        if (pm == null) {
            CompletableFuture<ClientTransaction> transactionFuture =
                    ClientLazyTransaction.ensureStarted(tx, ch, () -> ch.getChannelAsync(null)).get1();

            assert transactionFuture != null;

            return transactionFuture;
        }

        return ch.getChannelAsync(pm.nodeConsistentId()).thenCompose(ch0 -> {
            IgniteBiTuple<CompletableFuture<ClientTransaction>, Boolean> tuple = ClientLazyTransaction.ensureStarted(tx, ch,
                    piggybackSupported.test(ch0) ? null : () -> completedFuture(ch0));

            if (tuple.get2()) {
                // If this is the first direct request in transaction, it will also piggyback a transaction start.
                ctx.pm = pm;
                ctx.readOnly = tx.isReadOnly();
                ctx.channel = ch0;
                ctx.firstReqFut = tuple.get1();
                return nullCompletedFuture();
            } else {
                return tuple.get1();
            }
        });
    }

    /**
     * Writes transaction metadata to the given {@link PayloadOutputChannel}, encoding the necessary information for the server to associate
     * the request with the appropriate transaction context.
     *
     * <p>Depending on the current transaction state and {@link WriteContext}, this method handles several scenarios:
     * <ul>
     *   <li>If {@code tx} is {@code null}, a {@code nil} value is written to indicate the absence of a transaction.</li>
     *   <li>If {@code ctx.firstReqFut} is non-null, this is the first request in a lazily-started transaction.
     *       The method writes a special marker, timestamp, read-only flag, and timeout to piggyback the transaction start.</li>
     *   <li>If {@code ctx.enlistmentToken} is present, it writes metadata required to perform a direct enlistment
     *       for a partition-aware transaction (e.g., token, transaction ID, commit partition, coordinator, etc.).</li>
     *   <li>If neither case applies, it writes only the transaction ID, verifying that the transaction channel matches
     *       the output channel. If the channels do not match, an exception is thrown to indicate lost transaction context.</li>
     * </ul>
     *
     * @param tx The transaction to write; may be {@code null} if no transaction is associated with the request.
     * @param out The {@link PayloadOutputChannel} to which transaction data will be written.
     * @param ctx The {@link WriteContext} that provides additional transaction state, or {@code null} if not available.
     * @throws IgniteException If the transaction's channel does not match the current output channel.
     */
    public static void writeTx(@Nullable Transaction tx, PayloadOutputChannel out, @Nullable WriteContext ctx) {
        if (tx == null) {
            out.out().packNil();
        } else {
            if (ctx != null && (ctx.enlistmentToken != null || ctx.firstReqFut != null)) {
                if (ctx.firstReqFut != null) {
                    ClientLazyTransaction tx0 = (ClientLazyTransaction) tx;
                    out.out().packLong(TX_ID_FIRST_DIRECT);
                    out.out().packLong(tx0.observableTimestamp());

                    if (ctx.channel.protocolContext().isFeatureSupported(TX_CLIENT_GETALL_SUPPORTS_TX_OPTIONS)
                            && (ctx.opCode == ClientOp.TUPLE_GET_ALL || ctx.opCode == ClientOp.TUPLE_CONTAINS_ALL_KEYS)) {
                        // Use changed pack order for timeout and options.
                        out.out().packLong(tx0.timeout());
                        EnumSet<ClientInternalTxOptions> opts = tx0.options();
                        // Try avoid to allocate new objects for unrelated transactions.
                        int mask = 0;
                        if (tx0.isReadOnly()) {
                            mask |= READ_ONLY.mask();
                        }
                        if (opts != null) {
                            for (ClientInternalTxOptions opt : opts) {
                                mask |= opt.mask();
                            }
                        }
                        out.out().packInt(mask);
                    } else {
                        out.out().packBoolean(tx.isReadOnly());
                        out.out().packLong(tx0.timeout());
                    }
                } else {
                    ClientTransaction tx0 = ClientTransaction.get(tx);
                    out.out().packLong(TX_ID_DIRECT);
                    out.out().packLong(ctx.enlistmentToken);
                    out.out().packUuid(tx0.txId());
                    out.out().packInt(tx0.commitTableId());
                    out.out().packInt(tx0.commitPartition());
                    out.out().packUuid(tx0.coordinatorId());
                    out.out().packLong(tx0.timeout());
                }
            } else {
                ClientTransaction tx0 = ClientTransaction.get(tx);

                //noinspection resource
                if (tx0.channel() != out.clientChannel()) {
                    // Do not throw IgniteClientConnectionException to avoid retry kicking in.
                    throw new IgniteException(CONNECTION_ERR, "Transaction context has been lost due to connection errors.");
                }

                out.out().packLong(tx0.id());
            }
        }
    }

    /**
     * Processes the transaction-related part of a server response and updates the client transaction or context accordingly.
     *
     * <p>This logic handles piggybacked transaction starts and direct enlistment in partition-aware transactions,
     * ensuring the transaction state is correctly initialized or synchronized with the server.
     *
     * @param payloadChannel The {@link PayloadInputChannel} containing the server's response data.
     * @param ch Channels repository.
     * @param ctx The {@link WriteContext} holding the transaction request state and response future.
     * @param tx The current {@link ClientTransaction}, or {@code null} if piggybacking a new transaction.
     * @param observableTimestamp A tracker for observable timestamps used for transaction visibility and causality.
     */
    public static void readTx(
            PayloadInputChannel payloadChannel,
            ReliableChannel ch,
            WriteContext ctx,
            @Nullable ClientTransaction tx,
            HybridTimestampTracker observableTimestamp
    ) {
        ClientMessageUnpacker in = payloadChannel.in();
        if (ctx.firstReqFut != null) {
            assert tx == null;

            long id = in.unpackLong();
            UUID txId = in.unpackUuid();
            UUID coordId = in.unpackUuid();
            long timeout = in.unpackLong();

            ClientTransaction startedTx =
                    new ClientTransaction(payloadChannel.clientChannel(), ch, id, ctx.readOnly, txId, ctx.pm, coordId, observableTimestamp,
                            timeout);

            ctx.firstReqFut.complete(startedTx);
        } else if (ctx.enlistmentToken != null) { // Use enlistment meta only for remote transactions.
            assert tx != null;
            assert ctx.pm != null;

            if (in.tryUnpackNil()) { // This may happen on no-op enlistment when a newer client is connected to older server.
                payloadChannel.clientChannel().inflights().removeInflight(tx.txId(), null);

                // If this is first enlistment to a partition, we hit a bug and can't do anything but fail.
                if (ctx.enlistmentToken == 0) {
                    tx.tryFailEnlist(ctx.pm, new IgniteException(INTERNAL_ERR,
                            "Encountered no-op on first direct enlistment, server version upgrade is required"));
                }
            } else {
                String consistentId = payloadChannel.in().unpackString();
                long token = payloadChannel.in().unpackLong();

                // Test if no-op enlistment.
                if (payloadChannel.in().unpackBoolean()) {
                    payloadChannel.clientChannel().inflights().removeInflight(tx.txId(), null);
                }

                // Finish enlist on first request only.
                if (ctx.enlistmentToken == 0) {
                    tx.tryFinishEnlist(ctx.pm, consistentId, token);
                }
            }
        }
    }

    /**
     * Resolves the {@link ClientChannel} to be used for the current operation based on the transaction and partition mapping.
     *
     * <p>If this is the first request in the transaction (i.e., {@code ctx.firstReqFut} is {@code null}), it selects a channel
     * based on the preferred node resolved from the transaction or partition mapping. Otherwise, the channel used for the first request is
     * reused.
     *
     * @param ctx The {@link WriteContext} that holds transactional context information and may be updated.
     * @param ch The {@link ReliableChannel} used to resolve the actual communication channel.
     * @param trackOperation Whether the current operation should be additionally tracked by transaction.
     * @param tx The client transaction associated with the request, or {@code null} if none.
     * @param mapping The partition mapping, possibly used to resolve the preferred target node; may be {@code null}.
     * @return A {@link CompletableFuture} that completes with the resolved {@link ClientChannel} for the operation.
     */
    public static CompletableFuture<ClientChannel> resolveChannel(
            WriteContext ctx,
            ReliableChannel ch,
            boolean trackOperation,
            @Nullable ClientTransaction tx,
            @Nullable PartitionMapping mapping
    ) {
        CompletableFuture<ClientChannel> chFuture = ctx.firstReqFut != null
                ? completedFuture(ctx.channel)
                : ch.getChannelAsync(resolvePreferredNode(tx, mapping));

        return chFuture.thenCompose(opCh -> {
            if (tx != null && tx.hasCommitPartition()
                    // If a request is colocated with a coordinator, it's executed in proxy mode.
                    && !tx.nodeName().equals(opCh.protocolContext().clusterNode().name())) {
                ctx.pm = mapping;
                return enlistDirect(tx, ch, opCh, ctx, trackOperation).thenApply(ignored -> opCh);
            } else {
                return completedFuture(opCh);
            }
        });
    }

    private static @Nullable String resolvePreferredNode(@Nullable ClientTransaction tx, @Nullable PartitionMapping pm) {
        String opNode = pm == null ? null : pm.nodeConsistentId();

        if (tx != null) {
            return !tx.isReadOnly() && tx.hasCommitPartition() && opNode != null ? opNode : tx.nodeName();
        } else {
            return opNode;
        }
    }

    private static CompletableFuture<Void> enlistDirect(
            ClientTransaction tx,
            ReliableChannel ch,
            ClientChannel opChannel,
            WriteContext ctx,
            boolean trackOperation
    ) {
        return tx.enlistFuture(ch, opChannel, ctx.pm, trackOperation).thenCompose(tup -> {
            if (tup.get2() == null) { // First request.
                ctx.enlistmentToken = 0L;
                return nullCompletedFuture();
            } else if (tup.get2() == 0L) { // No-op enlistment result.
                return enlistDirect(tx, ch, opChannel, ctx, trackOperation);
            } else { // Successfull enlistment.
                ctx.enlistmentToken = tup.get2();
                return nullCompletedFuture();
            }
        });
    }
}
