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

import static java.util.function.Function.identity;
import static org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature.TX_PIGGYBACK;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.internal.util.ViewUtils.sync;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_ERR;

import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.PartitionMapping;
import org.apache.ignite.internal.client.PayloadOutputChannel;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.ViewUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Client transaction.
 */
public class ClientTransaction implements Transaction {
    private static final int NO_COMMIT_PARTITION = -1;

    public static final UUID EMPTY = new UUID(0, 0);

    /** Open state. */
    private static final int STATE_OPEN = 0;

    /** Committed state. */
    private static final int STATE_COMMITTED = 1;

    /** Rolled back state. */
    private static final int STATE_ROLLED_BACK = 2;

    /** Channel that the transaction belongs to. */
    @IgniteToStringExclude
    private final ClientChannel ch;

    /** Transaction id. */
    private final long id;

    /** The future used on repeated commit/rollback. */
    @IgniteToStringExclude
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

    private final long timeout;

    /** Direct enlistment map. */
    @IgniteToStringExclude
    private final Map<TablePartitionId, CompletableFuture<IgniteBiTuple<String, Long>>> enlisted = new ConcurrentHashMap<>();

    @IgniteToStringExclude
    private final HybridTimestampTracker tracker;

    @IgniteToStringExclude
    private final ReentrantReadWriteLock enlistPartitionLock = new ReentrantReadWriteLock();

    /**
     * Constructor.
     *
     * @param ch Channel that the transaction belongs to.
     * @param id Transaction id.
     * @param isReadOnly Read-only flag.
     * @param txId Transaction id.
     * @param cpm The commit partition mapping or {@code null} if not known.
     * @param coordId Tx coordinator id.
     * @param tracker The tracker.
     * @param timeout The timeout.
     */
    public ClientTransaction(
            ClientChannel ch,
            long id,
            boolean isReadOnly,
            UUID txId,
            @Nullable PartitionMapping cpm,
            UUID coordId,
            HybridTimestampTracker tracker,
            long timeout
    ) {
        this.ch = ch;
        this.id = id;
        this.isReadOnly = isReadOnly;
        this.txId = txId;
        this.nodeName = ch.protocolContext().clusterNode().name();
        this.tracker = tracker;
        this.timeout = timeout;

        if (cpm != null) {
            // if commit partition is known, we can attempt to do direct mappings in this transaction.
            this.commitTableId = cpm.tableId();
            this.commitPartition = cpm.partition();
        } else {
            this.commitTableId = NO_COMMIT_PARTITION;
            this.commitPartition = NO_COMMIT_PARTITION;
        }

        this.coordId = coordId;

        assert txId == null || coordId != null;
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

    public boolean hasCommitPartition() {
        return commitPartition != NO_COMMIT_PARTITION;
    }

    public UUID coordinatorId() {
        return coordId;
    }

    public long timeout() {
        return timeout;
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
        enlistPartitionLock.writeLock().lock();

        try {
            if (!finishFut.compareAndSet(null, new CompletableFuture<>())) {
                return finishFut.get();
            }
        } finally {
            enlistPartitionLock.writeLock().unlock();
        }

        boolean enabled = ch.protocolContext().isFeatureSupported(TX_PIGGYBACK);
        CompletableFuture<Void> finishFut = enabled ? ch.inflights().finishFuture(txId()) : nullCompletedFuture();

        CompletableFuture<Void> mainFinishFut = finishFut.handle((ignored, e) -> {
            if (e != null) {
                ch.serviceAsync(ClientOp.TX_ROLLBACK, w -> {
                    w.out().packLong(id);

                    if (!isReadOnly && enabled) {
                        packEnlisted(w);
                    }
                }, r -> null);

                return CompletableFuture.<Void>failedFuture(e);
            }

            return ch.serviceAsync(ClientOp.TX_COMMIT, w -> {
                w.out().packLong(id);

                if (!isReadOnly && enabled) {
                    packEnlisted(w);
                }
            }, r -> (Void) null);
        }).thenCompose(identity());

        mainFinishFut.handle((res, e) -> {
            setState(STATE_COMMITTED);
            ch.inflights().erase(txId());
            this.finishFut.get().complete(null);
            return null;
        });

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
        try {
            return rollbackAsyncInternal().handle((res, e) -> {
                if (e != null) {
                    if (hasCause(e, IgniteClientConnectionException.class)) {
                        // Connection exception: tx rolled back on server.
                        return null;
                    }

                    throw sneakyThrow(ViewUtils.ensurePublicException(e));
                }

                return null;
            });
        } catch (Throwable t) {
            if (hasCause(t, IgniteClientConnectionException.class)) {
                // Connection exception: tx rolled back on server.
                return nullCompletedFuture();
            }

            throw sneakyThrow(ViewUtils.ensurePublicException(t));
        }
    }

    private CompletableFuture<Void> rollbackAsyncInternal() {
        enlistPartitionLock.writeLock().lock();

        try {
            if (!finishFut.compareAndSet(null, new CompletableFuture<>())) {
                return finishFut.get();
            }
        } finally {
            enlistPartitionLock.writeLock().unlock();
        }

        // Don't wait inflights on rollback.
        CompletableFuture<Void> mainFinishFut = ch.serviceAsync(ClientOp.TX_ROLLBACK, w -> {
            w.out().packLong(id);

            if (!isReadOnly && w.clientChannel().protocolContext().isFeatureSupported(TX_PIGGYBACK)) {
                packEnlisted(w);
            }
        }, r -> null);

        mainFinishFut.handle((res, e) -> {
            setState(STATE_ROLLED_BACK);
            ch.inflights().erase(txId());
            this.finishFut.get().complete(null);
            return null;
        });

        return mainFinishFut;
    }

    private void packEnlisted(PayloadOutputChannel w) {
        int pos = w.out().reserveInt();
        int cnt = 0;
        for (Entry<TablePartitionId, CompletableFuture<IgniteBiTuple<String, Long>>> entry : enlisted.entrySet()) {
            IgniteBiTuple<String, Long> info = entry.getValue().getNow(null);

            if (info == null) {
                continue; // Ignore incomplete enlistments.
            }

            w.out().packInt(entry.getKey().tableId());
            w.out().packInt(entry.getKey().partitionId());
            w.out().packString(info.get1());
            w.out().packLong(info.get2());

            cnt++;
        }

        w.out().setInt(pos, cnt);

        if (cnt > 0) {
            w.out().packLong(tracker.get().longValue());

            // Send information about directly mapped writes to ensure a proper cleanup algorithm is chosen.
            w.out().packBoolean(!ch.inflights().contains(txId));
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }

    /**
     * Gets the internal transaction from the given public transaction. Throws an exception if the given transaction is not an instance of
     * {@link ClientTransaction}.
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

        throw new TransactionException(
                TX_ALREADY_FINISHED_ERR,
                format("Transaction is already finished [tx={}].", clientTx));
    }

    static IgniteException unsupportedTxTypeException(Transaction tx) {
        return new IgniteException(INTERNAL_ERR, "Unsupported transaction implementation: '"
                + tx.getClass()
                + "'. Use IgniteClient.transactions() to start transactions.");
    }

    private void setState(int state) {
        this.state.compareAndExchange(STATE_OPEN, state);
    }

    private void checkEnlistPossible() {
        if (finishFut.get() != null) {
            throw new TransactionException(TX_ALREADY_FINISHED_ERR, format("Transaction is already finished [tx={}].", this));
        }
    }

    /**
     * Enlists a write operation in direct mapping.
     *
     * @param ch Channel facade.
     * @param opChannel Operation channel.
     * @param pm Partition mapping.
     * @param trackOperation Denotes if upcoming operation should be tracked. This affects finalization behavior as acknowledge must
     *         be received for every tracked operation prior to transaction finalization.
     * @return The future.
     */
    public CompletableFuture<IgniteBiTuple<String, Long>> enlistFuture(ReliableChannel ch, ClientChannel opChannel, PartitionMapping pm,
            boolean trackOperation) {
        if (!enlistPartitionLock.readLock().tryLock()) {
            throw new TransactionException(TX_ALREADY_FINISHED_ERR, format("Transaction is already finished [tx={}].", this));
        }

        checkEnlistPossible();

        boolean[] first = {false};

        TablePartitionId tablePartitionId = new TablePartitionId(pm.tableId(), pm.partition());

        CompletableFuture<IgniteBiTuple<String, Long>> fut = enlisted.compute(tablePartitionId, (k, v) -> {
            if (v == null) {
                first[0] = true;
                return new CompletableFuture<>();
            } else {
                return v;
            }
        });

        enlistPartitionLock.readLock().unlock();

        // Re-check after unlock.
        checkEnlistPossible();

        if (trackOperation) {
            ch.inflights().addInflight(txId);
        }

        if (first[0]) {
            // For the first request return completed future.
            return CompletableFuture.completedFuture(new IgniteBiTuple<>(null, null));
        } else {
            return fut;
        }
    }

    /**
     * Tries to finish existing enlistment.
     *
     * @param pm Partition mapping.
     * @param consistentId Consistent id.
     * @param token Enlistment token.
     */
    public void tryFinishEnlist(PartitionMapping pm, String consistentId, long token) {
        if (!hasCommitPartition()) {
            return;
        }

        TablePartitionId tablePartitionId = new TablePartitionId(pm.tableId(), pm.partition());

        CompletableFuture<IgniteBiTuple<String, Long>> fut = enlisted.get(tablePartitionId);

        if (fut != null && !fut.isDone()) {
            fut.complete(new IgniteBiTuple<>(consistentId, token));
        }
    }

    /**
     * Tries to fail the enlistment.
     *
     * @param pm Partition mapping.
     * @param exception The exception.
     */
    public void tryFailEnlist(PartitionMapping pm, Exception exception) {
        if (!hasCommitPartition()) {
            return;
        }

        TablePartitionId tablePartitionId = new TablePartitionId(pm.tableId(), pm.partition());

        CompletableFuture<IgniteBiTuple<String, Long>> fut = enlisted.get(tablePartitionId);

        if (fut != null && !fut.isDone()) {
            fut.completeExceptionally(exception);
        }
    }

    @TestOnly
    public int enlistedCount() {
        return enlisted.size();
    }

    /** Fail the transaction. */
    public void fail() {
        state.set(STATE_ROLLED_BACK);
        finishFut.set(nullCompletedFuture());
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
