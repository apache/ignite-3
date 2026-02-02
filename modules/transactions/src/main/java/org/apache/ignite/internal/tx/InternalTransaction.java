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

package org.apache.ignite.internal.tx;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * An extension of a transaction for internal usage.
 */
public interface InternalTransaction extends Transaction {
    /** 0 timeout means we have to use the default value from configuration. */
    int USE_CONFIGURED_TIMEOUT_DEFAULT = 0;

    /**
     * Returns an id.
     *
     * @return The id.
     */
    UUID id();

    /**
     * Returns enlisted partition information.
     *
     * @param replicationGroupId Replication group ID.
     * @return Enlisted partition information.
     */
    PendingTxPartitionEnlistment enlistedPartition(ZonePartitionId replicationGroupId);

    /**
     * Returns a transaction state.
     *
     * @return The state.
     */
    TxState state();

    /**
     * Assigns a partition id to store the transaction state.
     *
     * @param commitPartitionId Commit partition group id.
     * @return True if the partition was assigned as committed, false otherwise.
     */
    boolean assignCommitPartition(ZonePartitionId commitPartitionId);

    /**
     * Gets a partition id that stores the transaction state.
     *
     * @return Partition id.
     */
    ZonePartitionId commitPartition();

    /**
     * Enlists a partition group into a transaction.
     *
     * @param replicationGroupId Replication group id to enlist.
     * @param tableId Table ID for enlistment.
     * @param primaryNodeConsistentId Consistent node id of primary replica.
     * @param consistencyToken Consistency token to enlist for given replication group.
     */
    void enlist(
            ZonePartitionId replicationGroupId,
            int tableId,
            String primaryNodeConsistentId,
            long consistencyToken
    );

    /**
     * Returns read timestamp for the given transaction if it is a read-only one or {code null} otherwise.
     *
     * @return Read timestamp for the given transaction if it is a read-only one or {code null} otherwise.
     */
    @Nullable HybridTimestamp readTimestamp();

    /**
     * Returns a timestamp of the schema corresponding to the transaction.
     * For RW transactions, this is the beginTimestamp; for RO transactions, it's {@link #readTimestamp()}.
     *
     * @return Timestamp that is used to obtain the effective schema version used inside the transaction.
     */
    HybridTimestamp schemaTimestamp();

    /**
     * Get the transaction coordinator inconsistent ID.
     *
     * @return Transaction coordinator inconsistent ID.
     */
    UUID coordinatorId();

    /**
     * Gets the transaction implicit flag.
     *
     * @return True if the transaction is implicit, false if it is started explicitly.
     */
    boolean implicit();

    /**
     * Gets the transaction remote flag.
     *
     * @return True if the transaction is remotely coordinated, false otherwise.
     */
    default boolean remote() {
        return false;
    }

    /**
     * Finishes a read-only transaction with a specific execution timestamp.
     *
     * @param commit Commit flag. The flag is ignored for read-only transactions.
     * @param executionTimestamp The timestamp is the time when a read-only transaction is applied to the remote node. The parameter
     *         is not used for read-write transactions.
     * @param full Full state transaction marker.
     * @param timeoutExceeded Timeout exceeded flag (commit flag must be {@code false}).
     * @return The future.
     */
    CompletableFuture<Void> finish(boolean commit, @Nullable HybridTimestamp executionTimestamp, boolean full, boolean timeoutExceeded);

    /**
     * Checks if the transaction is finishing or finished. If {@code true}, no more operations can be performed on the transaction.
     * Becomes {@code true} after {@link #commitAsync()} or {@link #rollbackAsync()} is called.
     *
     * @return Whether the transaction is finishing or finished
     */
    boolean isFinishingOrFinished();

    /**
     * Returns the transaction timeout in millis.
     *
     * @return The transaction timeout.
     */
    long getTimeout();

    /**
     * Kills this transaction.
     *
     * @return The future.
     */
    CompletableFuture<Void> kill();

    /**
     * Rolls back the transaction due to timeout exceeded. After this method is called, {@link #isRolledBackWithTimeoutExceeded()} will
     * return {@code true}. A rollback of a completed or ending transaction has no effect and always succeeds when the transaction is
     * completed.
     *
     * @return The future.
     */
    CompletableFuture<Void> rollbackTimeoutExceededAsync();

    /**
     * Rolls back the transaction due to a non-user exception and records the abort reason.
     * A rollback of a completed or ending transaction has no effect and always succeeds when the transaction is completed.
     *
     * @param throwable Abort reason.
     * @return The future.
     */
    CompletableFuture<Void> rollbackWithExceptionAsync(Throwable throwable);

    /**
     * Checks if the transaction was rolled back due to timeout exceeded. The only way to roll back a transaction due to timeout exceeded is
     * to call {@link #rollbackTimeoutExceededAsync()}.
     *
     * @return {@code true} if the transaction was rolled back due to timeout exceeded, {@code false} otherwise.
     */
    boolean isRolledBackWithTimeoutExceeded();

    /**
     * Process delayed ack.
     *
     * @param val The value.
     * @param err The error.
     */
    default void processDelayedAck(Object val, @Nullable Throwable err) {
        // No-op.
    }
}
