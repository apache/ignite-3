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

package org.apache.ignite.internal.table;

import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tostring.IgniteToStringBuilder;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TransactionIds;
import org.jetbrains.annotations.TestOnly;

/**
 * Transactional operation context used in scan\lookup table operations.
 *
 * @see InternalTable
 */
public abstract class TxContext {
    /** Creates operation context from RO transaction. */
    public static TxContext readOnly(UUID txId, UUID txCoordinatorId, HybridTimestamp readTimestamp) {
        return new ReadOnly(txId, txCoordinatorId, readTimestamp);
    }

    /** Creates operation context from RO transaction. For test purposes only. */
    @TestOnly
    public static TxContext readOnly(InternalTransaction tx) {
        assert tx.isReadOnly();

        HybridTimestamp readTimestamp = tx.readTimestamp();

        assert readTimestamp != null;

        return new ReadOnly(tx.id(), tx.coordinatorId(), readTimestamp);
    }

    /** Creates operation context from RW transaction. */
    public static TxContext readWrite(
            UUID txId,
            UUID txCoordinatorId,
            ReplicationGroupId commitPartition,
            long enlistmentConsistencyToken
    ) {
        return new ReadWrite(txId, txCoordinatorId, commitPartition, enlistmentConsistencyToken);
    }

    /** Creates operation context from RW transaction. For test purposes only. */
    @TestOnly
    public static TxContext readWrite(InternalTransaction tx, long enlistmentConsistencyToken) {
        assert !tx.isReadOnly();

        return new ReadWrite(tx.id(), tx.coordinatorId(), tx.commitPartition(), enlistmentConsistencyToken);
    }

    protected final UUID txId;
    protected final UUID coordinatorId;

    protected TxContext(UUID txId, UUID coordinatorId) {
        Objects.requireNonNull(txId, "Transaction id is mandatory");
        Objects.requireNonNull(coordinatorId, "Transaction coordinator id is mandatory");

        this.txId = txId;
        this.coordinatorId = coordinatorId;
    }

    /** Returns {@code true} for read only transaction, {@code false} otherwise. */
    public abstract boolean isReadOnly();

    /** Returns transaction id. */
    public UUID txId() {
        return txId;
    }

    /** Returns transaction coordinator id. */
    public UUID coordinatorId() {
        return coordinatorId;
    }

    /** Read-only transaction context. */
    public static class ReadOnly extends TxContext {
        private final HybridTimestamp readTimestamp;

        private ReadOnly(UUID txId, UUID txCoordinatorId, HybridTimestamp readTimestamp) {
            super(txId, txCoordinatorId);

            this.readTimestamp = readTimestamp;
        }

        @Override
        public boolean isReadOnly() {
            return true;
        }

        /** Returns transaction read timestamp. */
        public HybridTimestamp readTimestamp() {
            return readTimestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ReadOnly txCtx = (ReadOnly) o;
            return Objects.equals(txId, txCtx.txId)
                    && Objects.equals(coordinatorId, txCtx.coordinatorId)
                    && Objects.equals(readTimestamp, txCtx.readTimestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(txId);
        }

        @Override
        public String toString() {
            return IgniteToStringBuilder.toString(this);
        }
    }

    /** Read-write transaction context. */
    public static class ReadWrite extends TxContext {
        // TODO ZonePartitionId
        private final ReplicationGroupId commitPartition;
        private final long enlistmentConsistencyToken;

        private ReadWrite(UUID txId, UUID txCoordinatorId, ReplicationGroupId commitPartition, long enlistmentConsistencyToken) {
            super(txId, txCoordinatorId);

            Objects.requireNonNull(commitPartition, "Commit partition is mandatory for RW transaction");

            if (enlistmentConsistencyToken < 0) {
                throw new IllegalArgumentException("Consistency token partition is mandatory for RW transaction");
            }

            this.commitPartition = commitPartition;
            this.enlistmentConsistencyToken = enlistmentConsistencyToken;
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ReadWrite opCtx = (ReadWrite) o;
            return enlistmentConsistencyToken == opCtx.enlistmentConsistencyToken
                    && Objects.equals(txId, opCtx.txId)
                    && Objects.equals(coordinatorId, opCtx.coordinatorId)
                    && Objects.equals(commitPartition, opCtx.commitPartition);
        }

        /** Returns transaction commit partition. */
        public ZonePartitionId commitPartition() {
            // TODO
            return (ZonePartitionId) commitPartition;
        }

        /** Returns transaction begin timestamp. */
        public HybridTimestamp beginTimestamp() {
            return TransactionIds.beginTimestamp(txId);
        }

        /** Returns enlistment consistency token. */
        public long enlistmentConsistencyToken() {
            return enlistmentConsistencyToken;
        }

        @Override
        public int hashCode() {
            return Objects.hash(txId);
        }

        @Override
        public String toString() {
            return IgniteToStringBuilder.toString(this);
        }
    }
}
