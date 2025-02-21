package org.apache.ignite.internal.tx.impl;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

public class RemoteTransaction implements InternalTransaction {
    private UUID txId;

    public RemoteTransaction(UUID txId, int commitPart, UUID coord) {
        this.txId = txId;
    }

    @Override
    public void commit() throws TransactionException {

    }

    @Override
    public CompletableFuture<Void> commitAsync() {
        return null;
    }

    @Override
    public void rollback() throws TransactionException {
        assert false;
    }

    @Override
    public CompletableFuture<Void> rollbackAsync() {
        assert false;

        return null;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public UUID id() {
        return txId;
    }

    @Override
    public IgniteBiTuple<ClusterNode, Long> enlistedNodeAndConsistencyToken(ReplicationGroupId replicationGroupId) {
        return null;
    }

    @Override
    public TxState state() {
        return null;
    }

    @Override
    public boolean assignCommitPartition(TablePartitionId tablePartitionId) {
        return false;
    }

    @Override
    public TablePartitionId commitPartition() {
        return null;
    }

    @Override
    public IgniteBiTuple<ClusterNode, Long> enlist(ReplicationGroupId replicationGroupId, int tableId,
            IgniteBiTuple<ClusterNode, Long> nodeAndConsistencyToken) {
        return null;
    }

    @Override
    public @Nullable HybridTimestamp readTimestamp() {
        return null;
    }

    @Override
    public HybridTimestamp startTimestamp() {
        return null;
    }

    @Override
    public UUID coordinatorId() {
        return null;
    }

    @Override
    public boolean implicit() {
        return false;
    }

    @Override
    public CompletableFuture<Void> finish(boolean commit, @Nullable HybridTimestamp executionTimestamp, boolean full) {
        return null;
    }

    @Override
    public boolean isFinishingOrFinished() {
        return false;
    }

    @Override
    public long timeout() {
        return 0;
    }

    @Override
    public CompletableFuture<Void> kill() {
        return null;
    }
}
