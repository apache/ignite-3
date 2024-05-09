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

package org.apache.ignite.internal.tx.impl;

import static org.apache.ignite.internal.thread.PublicApiThreading.execUserAsyncOperation;
import static org.apache.ignite.internal.thread.PublicApiThreading.execUserSyncOperation;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper around {@link Transaction} that maintains public API invariants relating to threading.
 * That is, it adds protection against thread hijacking by users and also marks threads as 'executing a sync user operation' or
 * 'executing an async user operation'.
 */
public class PublicApiThreadingTransaction implements InternalTransaction, Wrapper {
    private final InternalTransaction transaction;
    private final Executor asyncContinuationExecutor;

    PublicApiThreadingTransaction(Transaction transaction, Executor asyncContinuationExecutor) {
        this.transaction = (InternalTransaction) transaction;
        this.asyncContinuationExecutor = asyncContinuationExecutor;
    }

    @Override
    public void commit() throws TransactionException {
        execUserSyncOperation(transaction::commit);
    }

    @Override
    public CompletableFuture<Void> commitAsync() {
        return preventThreadHijack(transaction::commitAsync);
    }

    @Override
    public void rollback() throws TransactionException {
        execUserSyncOperation(transaction::rollback);
    }

    @Override
    public CompletableFuture<Void> rollbackAsync() {
        return preventThreadHijack(transaction::rollbackAsync);
    }

    @Override
    public boolean isReadOnly() {
        return transaction.isReadOnly();
    }

    private <T> CompletableFuture<T> preventThreadHijack(Supplier<CompletableFuture<T>> operation) {
        CompletableFuture<T> future = execUserAsyncOperation(operation);
        return PublicApiThreading.preventThreadHijack(future, asyncContinuationExecutor);
    }

    @Override
    public UUID id() {
        return transaction.id();
    }

    @Override
    public IgniteBiTuple<ClusterNode, Long> enlistedNodeAndConsistencyToken(TablePartitionId tablePartitionId) {
        return transaction.enlistedNodeAndConsistencyToken(tablePartitionId);
    }

    @Override
    public TxState state() {
        return transaction.state();
    }

    @Override
    public boolean assignCommitPartition(TablePartitionId tablePartitionId) {
        return transaction.assignCommitPartition(tablePartitionId);
    }

    @Override
    public TablePartitionId commitPartition() {
        return transaction.commitPartition();
    }

    @Override
    public IgniteBiTuple<ClusterNode, Long> enlist(TablePartitionId tablePartitionId,
            IgniteBiTuple<ClusterNode, Long> nodeAndConsistencyToken) {
        return transaction.enlist(tablePartitionId, nodeAndConsistencyToken);
    }

    @Override
    public @Nullable HybridTimestamp readTimestamp() {
        return transaction.readTimestamp();
    }

    @Override
    public HybridTimestamp startTimestamp() {
        return transaction.startTimestamp();
    }

    @Override
    public String coordinatorId() {
        return transaction.coordinatorId();
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return classToUnwrap.cast(transaction);
    }
}
